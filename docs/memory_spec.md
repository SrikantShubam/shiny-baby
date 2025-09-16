# Phoenix Memory Layer (Phase-1A)

**Goal:** Turn per-table signatures into recipe choices that improve structure while enforcing guardrails.

- **Signature**: compact features of a table (structure, cues, small minhash sketch).
- **Patterns store (`out/patterns/patterns.jsonl`)**: confirmed patterns grouped by family with recipes and stats.
- **Learning events (`out/review/learning_events.jsonl`)**: append-only log of proposals, outcomes, guards.

**Flow:** Surgeon → compute signature → find candidate families → rank recipes (contextual bandit) → apply if gain ≥ min_gain & guards pass → emit event → (optional) reviewer confirmation → pattern promoted.

**Families (seed):** `contact_slab`, `period_grid`, `ledger_stub`, `micro_tables`, `generic`.

**Guards:** structural integrity; never break tabularity or headers. Learning never hard-fails the Surgeon.
