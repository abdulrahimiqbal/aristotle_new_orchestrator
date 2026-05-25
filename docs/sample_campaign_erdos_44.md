# Sample Campaign: Erdős 44 Constructive Extension Gate

This is the first-click campaign path for the generation/orchestration pillar.

It shows how the orchestrator turns a broad mathematical research prompt into a
focused packet of proof targets, attack families, anti-goals, and next
experiments.

## Research Question

Can the current Erdős 44 Sidon-set campaign be reduced to a constructive
extension lemma instead of spending effort on generic Sidon background?

## Campaign Input

The campaign packet lives in:

```txt
docs/research_packets/erdos_44_packet.json
```

It records:

- the current frontier
- known true/false facts from prior experiments
- anti-goals
- attack families
- finite example guidance
- formal anchors
- references
- operator notes

## One-Command Inspection

Run:

```bash
PYTHONPATH=src python examples/erdos_44_sample_campaign.py
```

The command does not call an LLM or Aristotle service. It is a deterministic
first-click view over the campaign packet.

## What The Sample Demonstrates

The sample campaign makes the generate pillar inspectable:

```txt
research question
-> research packet
-> active frontier targets
-> selected attack families
-> next proof/experiment shape
-> result interpretation
```

This is not a proof of Erdős 44. It is a reproducible view of the orchestration
artifact that decides what should be attempted next and what should be avoided.

## First Target

The current packet identifies the constructive extension lemma as the gate:

```txt
Given a finite Sidon set A in [1,N], find a dense tail Sidon set B in (N,M]
that remains compatible with A.
```

The packet recommends attacking this through explicit dense Sidon templates and
counting bad placements, while avoiding already-falsified naive union lemmas.

## Profile Boundary

This sample supports the claim that the system can generate/refine formal
research targets.

It does not yet show:

- a completed Lean proof of the extension lemma
- a solved Erdős 44 campaign
- autonomous mathematical discovery without human review

The next stronger artifact would connect this campaign output to one Aristotle
run bundle and record the resulting verdict.
