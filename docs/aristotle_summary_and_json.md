# Aristotle results: what the orchestrator expects

Public marketing pages at [aristotle.harmonic.fun](https://aristotle.harmonic.fun/) do **not** publish a formal schema for job artifacts. What follows is grounded in **this repo’s integration spec** (`BUILD_PROMPT.md`), the **`aristotlelib`** Python package (API fields), and the orchestrator’s own parsers.

## `ARISTOTLE_SUMMARY.md` (standard archive artifact)

From `BUILD_PROMPT.md`, the summary file is markdown with sections the orchestrator recognizes (headings containing **completed**, **partial**, **failed**; bullet lines under those headings):

```markdown
# Summary of changes
## Completed
- theorem name : type := proof
## Partial
- ...
## Failed
- Error: ...
```

Heuristic classification (same doc / `parse_result` in `aristotle.py`):

- **Proved lemmas**: bullet lines with `theorem` or `lemma` under **Completed**
- **Unsolved goals**: `sorry`, `unsolved`, `goal`
- **Blockers**: `blocker`, `stuck`
- **Counterexamples**: `counterexample`, `witness`
- **Verdict**: `disproved` if counterexamples; else **Completed** + lemma/theorem → `proved`; else **Partial** → `partial`; else `inconclusive`

## `aristotlelib` API (not the same as the file)

In `aristotlelib.project.Project`, the server may expose `output_summary: str | None` on the project object. That is an API field, not a guarantee about `ARISTOTLE_SUMMARY.md` layout inside the downloaded archive. The CLI path this app uses is still: **`aristotle result <uuid> --destination <path>`** → zip/tar → find summary markdown in the tree.

## `aristotle_result.json` (machine-readable)

If the archive contains `aristotle_result.json` or `aristotle-result.json`, the orchestrator prefers it. Otherwise it **synthesizes** the same schema v1 from the markdown summary (`result_origin: orchestrator_markdown_v1`, `parse_source` stored as `markdown_derived`) so storage and parsing stay consistent.

Schema v1 fields are defined in `parse_result_json` in `src/orchestrator/aristotle.py` (`schema_version`, `verdict`, list fields, optional `error_message` / `summary`, optional `result_origin`).
