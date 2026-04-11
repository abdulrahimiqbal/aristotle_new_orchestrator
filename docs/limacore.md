# Lima-core

Lima-core is a compact, event-sourced research subsystem built around one bounded production loop:

1. select one frontier gap
2. propose one delta
3. ground it with retrieval and internal memory
4. compile it into a tiny Aristotle agenda
5. fan out Aristotle jobs in parallel
6. score the results
7. keep or revert the delta
8. update frontier, worlds, fractures, cohorts, and program state

## Architecture

The subsystem lives under `src/orchestrator/limacore/` and is separated from the older Lima lab. The major modules are:

- `db.py`: concise SQLite store with event log, materialized frontier/world/fracture tables, cohorts/jobs, and content-addressed artifacts
- `loop.py`: one bounded production loop and optional background scheduler
- `proposer.py`: emits exactly one delta per iteration
- `worldsmith.py`: disciplined world invention with structured `WorldPacket` output
- `retriever.py`: formal, literature, and internal retrieval lanes
- `compiler.py`: compiles a delta into a tiny Aristotle agenda
- `aristotle.py`: pluggable backend interface with a deterministic local backend for tests/dev
- `scorer.py`: acceptance logic
- `solved.py`: strict solved checker
- `program.py`: self-improving research program, accepted only on verified-yield improvement
- `presenter.py` and `routes.py`: dedicated UI and API routes

## Schema

The storage model is intentionally small:

- `problems`
- `events`
- `frontier_nodes`
- `world_heads`
- `fracture_heads`
- `cohorts`
- `aristotle_jobs`
- `artifacts`
- `program_state`

The event log is the source of truth for research motion. `frontier_nodes`, `world_heads`, and `fracture_heads` are materialized state tables that can be rebuilt from event artifacts.

## Event Model

Each bounded loop iteration appends a short chain of events such as:

- `frontier_gap_selected`
- `delta_proposed`
- `grounding_built`
- `agenda_compiled`
- `aristotle_jobs_submitted`
- `aristotle_jobs_finished`
- `frontier_improved` or `delta_reverted`
- `program_updated`
- `solved_confirmed`

Artifacts are content-addressed blobs stored in `artifacts`. Event rows keep only artifact references, summaries, and scoring deltas.

## Loop Behavior

Lima-core never expands recursively inside one step. A single iteration produces one explicit accept/reject decision for one small delta. A delta is only kept if it contributes replayable structure, reduces proof debt, or sharpens a fracture enough to change the next move.

## World Invention

`worldsmith.py` always emits a valid `WorldPacket` with:

- new objects or hidden representation
- bridge to the original problem
- explanation for why the world is easier
- a kill test
- a formal agenda

Built-in family heuristics include coordinate lifts, quotients, hidden-state worlds, cocycles, balancing worlds, symbolic dynamics, operator worlds, order/convexity worlds, and graph/rewrite worlds.

## Retrieval

Three retrieval lanes feed `GroundingBundle`:

- formal retrieval: internal frontier plus stub local formal corpus analogs
- literature retrieval: pluggable provider interface with a deterministic local provider by default
- internal retrieval: worlds, fractures, prior deltas, and recent cohorts/events

Each lane is capped so the compiler sees a tight context.

## Aristotle Parallelism

`compiler.py` emits a tiny agenda, and `aristotle.py` fans it into cohorts and jobs. The UI surfaces:

- queued/running/succeeded/failed jobs
- yielded lemmas
- yielded counterexamples
- yielded blockers
- stale cohorts

The local backend is deterministic and makes benchmark tests stable while preserving the same interface a real backend would use.

## Solved Semantics

A problem is solved only if:

- the target theorem node is proved
- all dependencies are proved
- replay references exist for all proved nodes
- the proof DAG is closed from a clean state

`solved.py` returns a strict `SolvedReport`; the UI shows that report directly.

## Self-Improvement

Program state is versioned in `program_state`. A `program_delta` is accepted only if a rolling window shows improved verified yield, measured by replayable lemmas, proof debt reduction, and useful fractures. Aesthetic changes alone never pass.

## UI

Dedicated routes:

- `/limacore`
- `/limacore/{problem_slug}`
- `/api/limacore/workspace?problem=...`
- `/api/limacore/run`
- `/api/limacore/problem`
- `/api/limacore/cohort/{id}`
- `/api/limacore/job/{id}`
- `/api/limacore/frontier/{problem_slug}`
- `/api/limacore/program/{problem_slug}`

The workspace uses HTMX polling to refresh the frontier, world/reduction panel, Aristotle farm, and activity strip without borrowing the older Lima dashboard layout.

## Inward Compression Benchmark

The deterministic local backend is strong enough to discover a balancing/compression world for the Inward Compression Conjecture:

- offset coordinates `b_i = a_i - (i-1)`
- convex compression energy descent
- unique balanced terminal profile skeleton

It does not falsely mark the problem solved until the proof frontier actually closes.
