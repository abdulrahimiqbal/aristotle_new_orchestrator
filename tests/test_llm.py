from __future__ import annotations

import asyncio

import httpx

from orchestrator import llm


def test_invoke_llm_retries_without_response_format(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions_for_provider(
        payload: dict[str, object],
        *,
        base_url: str,
        api_key: str,
        timeout: float = 120.0,
    ) -> dict[str, object]:
        calls.append(
            {
                "payload": dict(payload),
                "base_url": base_url,
                "api_key": api_key,
                "timeout": timeout,
            }
        )
        if len(calls) == 1:
            request = httpx.Request("POST", "https://example.test/v1/chat/completions")
            response = httpx.Response(
                400,
                request=request,
                text='{"error":"unsupported response_format json_object"}',
            )
            raise httpx.HTTPStatusError("unsupported response_format", request=request, response=response)
        return {"choices": [{"message": {"content": "fallback-ok"}}]}

    monkeypatch.setattr(llm, "_post_chat_completions_for_provider", fake_post_chat_completions_for_provider)
    monkeypatch.setattr(llm.app_config, "LLM_JSON_MODE", True)
    monkeypatch.setattr(llm.app_config, "LLM_BACKUP_BASE_URL", "")
    monkeypatch.setattr(llm.app_config, "LLM_BACKUP_MODEL", "")

    result = asyncio.run(llm.invoke_llm("system", "user"))

    assert result == "fallback-ok"
    assert calls[0]["payload"]["response_format"] == {"type": "json_object"}
    assert "response_format" not in calls[1]["payload"]


def test_invoke_llm_sets_max_tokens_and_reads_text_parts(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions_for_provider(
        payload: dict[str, object],
        *,
        base_url: str,
        api_key: str,
        timeout: float = 120.0,
    ) -> dict[str, object]:
        calls.append(
            {
                "payload": dict(payload),
                "base_url": base_url,
                "api_key": api_key,
                "timeout": timeout,
            }
        )
        return {
            "choices": [
                {
                    "message": {
                        "content": [{"type": "text", "text": "GLM test ok"}],
                        "reasoning_content": "hidden chain of thought",
                    }
                }
            ]
        }

    monkeypatch.setattr(llm, "_post_chat_completions_for_provider", fake_post_chat_completions_for_provider)
    monkeypatch.setattr(llm.app_config, "LLM_MAX_TOKENS", 120)

    result = asyncio.run(llm.invoke_llm("system", "user", json_object=False))

    assert result == "GLM test ok"
    assert calls[0]["payload"]["max_tokens"] == 120


def test_summarize_result_sets_provider_token_budget(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions_for_provider(
        payload: dict[str, object],
        *,
        base_url: str,
        api_key: str,
        timeout: float = 120.0,
    ) -> dict[str, object]:
        calls.append(
            {
                "payload": dict(payload),
                "base_url": base_url,
                "api_key": api_key,
                "timeout": timeout,
            }
        )
        return {"choices": [{"message": {"content": "short summary"}}]}

    monkeypatch.setattr(llm, "_post_chat_completions_for_provider", fake_post_chat_completions_for_provider)
    monkeypatch.setattr(llm.app_config, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(llm.app_config, "LLM_SUMMARIZE_MAX_TOKENS", 180)

    result = asyncio.run(llm.summarize_result("example proof output", use_llm=True))

    assert result == "short summary"
    assert calls[0]["payload"]["max_tokens"] == 180


def test_summarize_result_uses_fallback_when_llm_disabled(monkeypatch) -> None:
    monkeypatch.setattr(llm.app_config, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(llm.app_config, "LLM_DISABLED", True)

    result = asyncio.run(llm.summarize_result("x" * 600, use_llm=True))

    assert len(result) == 501
    assert result.endswith("…")


def test_reason_returns_skip_when_llm_disabled(monkeypatch) -> None:
    monkeypatch.setattr(llm.app_config, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(llm.app_config, "LLM_DISABLED", True)

    decision = asyncio.run(llm.reason(None))  # type: ignore[arg-type]

    assert "LLM disabled" in decision.reasoning
    assert decision.target_updates == []
    assert decision.new_experiments == []


def test_invoke_llm_falls_back_to_backup_provider(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions_for_provider(
        payload: dict[str, object],
        *,
        base_url: str,
        api_key: str,
        timeout: float = 120.0,
    ) -> dict[str, object]:
        calls.append(
            {
                "payload": dict(payload),
                "base_url": base_url,
                "api_key": api_key,
                "timeout": timeout,
            }
        )
        if base_url == "https://primary.example/v1":
            request = httpx.Request("POST", f"{base_url}/chat/completions")
            response = httpx.Response(502, request=request, text="bad gateway")
            raise httpx.HTTPStatusError("bad gateway", request=request, response=response)
        return {"choices": [{"message": {"content": "backup-ok"}}]}

    monkeypatch.setattr(llm, "_post_chat_completions_for_provider", fake_post_chat_completions_for_provider)
    monkeypatch.setattr(llm.app_config, "LLM_BASE_URL", "https://primary.example/v1")
    monkeypatch.setattr(llm.app_config, "LLM_API_KEY", "primary-key")
    monkeypatch.setattr(llm.app_config, "LLM_MODEL", "primary-model")
    monkeypatch.setattr(llm.app_config, "LLM_BACKUP_BASE_URL", "https://backup.example/v1")
    monkeypatch.setattr(llm.app_config, "LLM_BACKUP_API_KEY", "")
    monkeypatch.setattr(llm.app_config, "LLM_BACKUP_MODEL", "backup-model")

    result = asyncio.run(llm.invoke_llm("system", "user", json_object=False))

    assert result == "backup-ok"
    assert len(calls) == 2
    assert calls[0]["base_url"] == "https://primary.example/v1"
    assert calls[0]["payload"]["model"] == "primary-model"
    assert calls[1]["base_url"] == "https://backup.example/v1"
    assert calls[1]["api_key"] == "primary-key"
    assert calls[1]["payload"]["model"] == "backup-model"
