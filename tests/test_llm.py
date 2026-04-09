from __future__ import annotations

import asyncio

import httpx

from orchestrator import llm


def test_invoke_llm_retries_without_response_format(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions(payload: dict[str, object], *, timeout: float = 120.0) -> dict[str, object]:
        calls.append(dict(payload))
        if len(calls) == 1:
            request = httpx.Request("POST", "https://example.test/v1/chat/completions")
            response = httpx.Response(
                400,
                request=request,
                text='{"error":"unsupported response_format json_object"}',
            )
            raise httpx.HTTPStatusError("unsupported response_format", request=request, response=response)
        return {"choices": [{"message": {"content": "fallback-ok"}}]}

    monkeypatch.setattr(llm, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(llm.app_config, "LLM_JSON_MODE", True)

    result = asyncio.run(llm.invoke_llm("system", "user"))

    assert result == "fallback-ok"
    assert calls[0]["response_format"] == {"type": "json_object"}
    assert "response_format" not in calls[1]


def test_invoke_llm_sets_max_tokens_and_reads_text_parts(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions(
        payload: dict[str, object], *, timeout: float = 120.0
    ) -> dict[str, object]:
        calls.append(dict(payload))
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

    monkeypatch.setattr(llm, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(llm.app_config, "LLM_MAX_TOKENS", 120)

    result = asyncio.run(llm.invoke_llm("system", "user", json_object=False))

    assert result == "GLM test ok"
    assert calls[0]["max_tokens"] == 120


def test_summarize_result_sets_provider_token_budget(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_post_chat_completions(
        payload: dict[str, object], *, timeout: float = 120.0
    ) -> dict[str, object]:
        calls.append(dict(payload))
        return {"choices": [{"message": {"content": "short summary"}}]}

    monkeypatch.setattr(llm, "_post_chat_completions", fake_post_chat_completions)
    monkeypatch.setattr(llm.app_config, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(llm.app_config, "LLM_SUMMARIZE_MAX_TOKENS", 180)

    result = asyncio.run(llm.summarize_result("example proof output", use_llm=True))

    assert result == "short summary"
    assert calls[0]["max_tokens"] == 180
