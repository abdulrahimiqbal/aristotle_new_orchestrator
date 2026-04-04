from __future__ import annotations

import sqlite3
from pathlib import Path

from orchestrator.db import Database


def test_initialize_creates_ledger_and_parsed_columns(tmp_path: Path) -> None:
    path = tmp_path / "o.db"
    db = Database(str(path))
    db.initialize()
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='lemma_ledger'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute("PRAGMA table_info(experiments)")
        cols = {r[1] for r in cur.fetchall()}
        assert "parsed_proved_lemmas_json" in cols
        assert "parsed_blockers_json" in cols
        assert "result_structured_json" in cols
        assert "parse_source" in cols
        assert "parse_warnings_json" in cols
        cur = conn.execute("PRAGMA table_info(campaigns)")
        ccols = {r[1] for r in cur.fetchall()}
        assert "problem_map_json" in ccols
        assert "problem_refs_json" in ccols
        assert "research_packet_json" in ccols
        assert "mathlib_knowledge" in ccols
        cur = conn.execute("PRAGMA table_info(experiments)")
        ecols = {r[1] for r in cur.fetchall()}
        assert "move_kind" in ecols
        assert "move_note" in ecols
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='campaign_map_node_acks'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='shadow_epistemic_state'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='shadow_global_state'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute("PRAGMA table_info(shadow_global_hypothesis)")
        gh_cols = {r[1] for r in cur.fetchall()}
        assert "score_0_100" in gh_cols
        assert "groundability_tier" in gh_cols
        assert "kill_test" in gh_cols
        assert "source_incubation_ids_json" in gh_cols
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supershadow_state'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supershadow_concept'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supershadow_incubation'"
        )
        assert cur.fetchone() is not None
        cur = conn.execute("PRAGMA table_info(supershadow_concept)")
        sh_cols = {r[1] for r in cur.fetchall()}
        assert "compression_power" in sh_cols
        assert "fit_to_known_facts" in sh_cols
        assert "ontological_delta" in sh_cols
        assert "falsifiability" in sh_cols
        assert "bridgeability" in sh_cols
        assert "grounding_cost" in sh_cols
        assert "speculative_risk" in sh_cols
        assert "concept_family" in sh_cols
        assert "family_kind" in sh_cols
        assert "smallest_transfer_probe" in sh_cols
        assert "family_novelty" in sh_cols
        assert "transfer_value" in sh_cols
        assert "family_saturation_penalty" in sh_cols
        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        assert int(uv) >= 13
    finally:
        conn.close()


def test_shadow_tables_commit_and_promote(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    db = Database(str(tmp_path / "s.db"))
    db.initialize()
    cid = db.create_campaign(
        "test collatz angle",
        workspace_root=str(root),
        workspace_template="minimal",
    )
    tid = db.add_targets(cid, ["prove a toy lemma"])[0]
    db.ensure_shadow_state_row(cid)
    run_id = db.shadow_commit_run(
        cid,
        trigger_kind="manual",
        summary="probe",
        response_obj={"ok": True},
        new_stance_json='{"summary": "x"}',
        new_policy_json='{"exploration_bias": 0.9}',
        hypotheses=[
            {
                "kind": "exploration",
                "title": "toy",
                "body_md": "body",
                "lean_snippet": "",
                "evidence": [{"experiment_id": None, "target_id": tid, "note": "seed"}],
            }
        ],
        promotions=[
            {
                "kind": "new_experiment",
                "target_id": tid,
                "objective": "Try a bounded search",
                "move_kind": "explore",
                "move_note": "shadow:test",
            }
        ],
    )
    assert run_id
    promos = db.list_shadow_promotion_requests(cid, status="pending")
    assert len(promos) == 1
    pid = promos[0]["id"]
    ok, msg, extra = db.apply_shadow_promotion(pid)
    assert ok, msg
    assert "experiment_id" in extra


def test_get_experiment_for_submit_joins_workspace(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    db = Database(str(tmp_path / "e.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
    )
    tid = db.add_targets(cid, ["lemma"])[0]
    eid = db.create_experiment(
        cid, tid, "try a proof", move_kind="explore", move_note="t"
    )
    row = db.get_experiment_for_submit(eid)
    assert row is not None
    assert row["status"] == "pending"
    assert row["workspace_dir"]
    assert cid in str(row["workspace_dir"])


def test_supershadow_tables_commit_and_approve_without_live_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "supershadow.db"
    db = Database(str(path))
    db.initialize()
    run_id = db.supershadow_commit_run(
        "global_collatz_supershadow",
        trigger_kind="manual",
        worldview_summary="Search for ambient spaces that compress modular and odd-input facts.",
        run_summary="One concept looks bridgeable.",
        fact_basis_json='[{"fact_key":"builtin:modular_descent_mod_8","label":"Mod 8 grounded"}]',
        pressure_map_json='[{"cluster":"modular","fact_keys":["builtin:modular_descent_mod_8"]}]',
        response_obj={"ok": True},
        new_worldview_json='{"summary":"test worldview"}',
        new_policy_json='{"prefer_compression": true}',
        concepts=[
            {
                "title": "Odd-state compactification",
                "worldview_summary": "Odd subdynamics may live more naturally in a compactified odd-state space.",
                "concepts": ["Collapse even transport into a derived operator on odd states."],
                "ontological_moves": ["Compactify odd-state transport."],
                "explains_facts": [
                    {
                        "fact_key": "builtin:modular_descent_mod_8",
                        "fact_label": "Mod 8 grounded",
                        "role": "explains",
                        "note": "The compactified odd-state picture explains the modular summary.",
                    }
                ],
                "tensions": [{"text": "Needs to explain global height failure."}],
                "kill_tests": [
                    {
                        "description": "Show the compactified odd operator breaks on a small residue obstruction.",
                        "expected_failure_signal": "The operator fails to preserve the claimed odd-state class.",
                        "suggested_grounding_path": "Shadow should formalize the odd-state operator first.",
                    }
                ],
                "bridge_lemmas": [
                    "Define the induced odd-state operator and prove compatibility with one Collatz step."
                ],
                "reduce_frontier_or_rename": "This reduces the frontier only if the compactification predicts why the odd-only invariant survives.",
                "scores": {
                    "compression_power": 5,
                    "fit_to_known_facts": 5,
                    "ontological_delta": 4,
                    "falsifiability": 4,
                    "bridgeability": 4,
                    "grounding_cost": 2,
                    "speculative_risk": 2,
                },
                "shadow_handoffs": [
                    {
                        "title": "Formalize the odd-state compactification",
                        "summary": "Turn the compactified odd-state picture into a disciplined proof program.",
                        "why_compressive": "It tries to explain modular and odd-input facts with one derived operator.",
                        "bridge_lemmas": [
                            "Define the induced odd-state operator and prove compatibility with one Collatz step."
                        ],
                        "shadow_task": "Design the proof program around the induced odd-state operator.",
                        "recommended_next_step": "Start by formalizing the odd-state operator interface.",
                        "grounding_notes": "Keep the first Lean bridge local and bounded.",
                    }
                ],
            }
        ],
        goal_text="goal",
    )
    assert run_id
    concepts = db.list_supershadow_concepts("global_collatz_supershadow", limit=10)
    assert len(concepts) == 1
    fact_links = db.list_supershadow_fact_links([concepts[0]["id"]])
    tensions = db.list_supershadow_tensions([concepts[0]["id"]])
    kill_tests = db.list_supershadow_kill_tests([concepts[0]["id"]])
    handoffs = db.list_supershadow_handoff_requests("global_collatz_supershadow", limit=10)
    assert len(fact_links) == 1
    assert len(tensions) == 1
    assert len(kill_tests) == 1
    assert len(handoffs) == 1

    ok, msg, _extra = db.approve_supershadow_handoff(handoffs[0]["id"])
    assert ok, msg
    approved = db.get_supershadow_handoff_request(handoffs[0]["id"])
    assert approved is not None
    assert approved["status"] == "approved"
    incubations = db.list_supershadow_incubations("global_collatz_supershadow", limit=10)
    assert len(incubations) == 1
    incubation_id = incubations[0]["id"]
    assert incubations[0]["status"] == "incubating"
    events = db.list_supershadow_incubation_events([incubation_id])
    assert events[0]["event_kind"] == "approved_from_handoff"

    conn = sqlite3.connect(str(path))
    try:
        target_count = conn.execute("SELECT COUNT(*) FROM targets").fetchone()[0]
        experiment_count = conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
        assert int(target_count) == 0
        assert int(experiment_count) == 0
    finally:
        conn.close()

    cid = db.create_campaign("bridge campaign", workspace_root=str(tmp_path / "ws"))
    tid = db.add_targets(cid, ["first bridge target"])[0]
    run_id = db.shadow_global_commit_run(
        "global_collatz",
        trigger_kind="manual",
        summary="Shadow operationalized the incubation.",
        response_obj={},
        new_stance_json="{}",
        new_policy_json="{}",
        hypotheses=[
            {
                "kind": "proof_program",
                "title": "Derived odd-state operator",
                "body_md": "Bridge lemma from the supershadow concept.",
                "source_incubation_ids": [incubation_id],
            }
        ],
        promotions=[
            {
                "kind": "new_experiment",
                "campaign_id": cid,
                "target_id": tid,
                "objective": "Ground the first odd-state bridge lemma.",
                "grounding_reason": "Need the bridge lemma now.",
                "expected_signal": "Success grounds the operator; failure shows the obstruction.",
                "novelty_reason": "First grounded descendant of the concept.",
                "source_incubation_ids": [incubation_id],
            }
        ],
        goal_text="goal",
    )
    assert run_id
    incubation = db.get_supershadow_incubation(incubation_id)
    assert incubation is not None
    assert incubation["status"] == "operationalized"
    assert incubation["shadow_last_run_id"] == run_id

    promo = db.list_shadow_global_promotion_requests("global_collatz", limit=10)[0]
    ok, msg, extra = db.apply_shadow_global_promotion(promo["id"])
    assert ok, msg
    assert extra["experiment_id"]
    grounded = db.get_supershadow_incubation(incubation_id)
    assert grounded is not None
    assert grounded["status"] == "grounded"


def test_create_campaign_per_workspace_dir(tmp_path: Path) -> None:
    root = tmp_path / "wsroot"
    db = Database(str(tmp_path / "c.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
        mathlib_knowledge=True,
    )
    state = db.get_campaign_state(cid)
    assert cid in state.campaign.workspace_dir
    assert Path(state.campaign.workspace_dir).resolve().parent == root.resolve()
    assert state.campaign.mathlib_knowledge is True


def test_campaign_research_packet_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "wsroot"
    db = Database(str(tmp_path / "packet.db"))
    db.initialize()
    cid = db.create_campaign(
        "hello",
        workspace_root=str(root),
        workspace_template="minimal",
        research_packet_json='{"summary":"focus on route A","attack_families":[{"title":"Route A","status":"primary"}]}',
    )
    state = db.get_campaign_state(cid)
    assert "focus on route A" in state.campaign.research_packet_json
    db.update_campaign_research_packet(cid, "plain text packet")
    updated = db.get_campaign_state(cid)
    assert updated.campaign.research_packet_json == '{"summary": "plain text packet"}'
