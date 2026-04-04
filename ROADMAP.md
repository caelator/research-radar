# research.radar — Multi-Phase Agentic AI Roadmap

> From scan-and-notify pipeline to worldclass agentic research intelligence platform.

## Design Principles

1. **Agent-native from the ground up** — Every interface is designed for tool-using AI agents first, human dashboards second. MCP is the primary control plane; CLI is the operator escape hatch.
2. **Embedded dual-store architecture** — SQLite for transactional state (profiles, jobs, leases, notifications). LanceDB for the knowledge corpus (items, embeddings, semantic retrieval). Both embedded, zero-server, local-first.
3. **Cooperative autonomy** — Radar owns scanning, scoring, and notification delivery. The orchestrating agent (OpenClaw) owns profile conversation, briefing composition, and strategic decisions. Neither tries to do the other's job.
4. **Bounded spend, unbounded insight** — Every LLM call is budgeted, capped, and metered. But the system's ability to surface connections, trends, and cross-domain relevance grows continuously through accumulated embeddings.
5. **Composable intelligence** — Each capability (source fetching, scoring, semantic search, trend detection) is a discrete tool an agent can invoke independently, not a monolithic pipeline an agent can only trigger end-to-end.

---

## Phase 1: Core Pipeline (current — V1)

**Goal:** Reliable scan-score-notify loop. Prove the pipeline works end-to-end.

| # | Deliverable | Status |
|---|------------|--------|
| 1 | 3-crate workspace (radar-core, radar-mcp, radar-cli) | Done |
| 2 | Core types, RadarError taxonomy, SQLite schema | Done |
| 3 | DB bootstrap (WAL, busy timeout, quick_check, migrations, backup) | Done |
| 4 | Storage helpers for all 8 entity types | Done |
| 5 | Profile CRUD + status/readiness + revision-based optimistic concurrency | Done |
| 6 | Scan job enqueue with active-job reuse, source-scope hash snapshot | Pending |
| 7 | Lease claim/renew/complete with token fencing | Done (store layer) |
| 8 | arXiv source adapter with watermarks, overlap lookback, gap_skipped | Pending |
| 9 | Keyword gate + deterministic ranking | Pending |
| 10 | LLM scorer with bounded retries and per-job spend accounting | Pending |
| 11 | End-to-end executor loop (fetch → normalize → dedup → filter → score → notify) | Pending |
| 12 | Discord notifier with at-least-once semantics | Pending |
| 13 | Telegram notifier | Pending |
| 14 | MCP tool handlers (profile_*, scan_*, matches_*, subscription_set, source_health) | Pending |
| 15 | CLI: scan-worker, scan-once, doctor, integrity-check, backup, vacuum | Pending |
| 16 | First-run UX (clean exits, diagnostics) | Pending |
| 17 | 8 tests passing (store layer) | Done |

**Exit criteria:** An agent can create a profile, trigger a scan, receive scored arXiv results, and get a Discord notification — all through MCP tools.

---

## Phase 2: Semantic Knowledge Layer (LanceDB integration)

**Goal:** Transform Radar from a notification pipeline into a searchable research knowledge base that agents can reason over.

### Architecture

```
┌─────────────────────────────────────────────────┐
│              radar-core                        │
│  ┌──────────────┐    ┌───────────────────────┐  │
│  │  SQLite       │    │  LanceDB              │  │
│  │  (state)      │    │  (knowledge corpus)   │  │
│  │               │    │                       │  │
│  │  profiles     │    │  items table          │  │
│  │  scan_jobs    │    │  ├─ canonical_id      │  │
│  │  watermarks   │    │  ├─ title             │  │
│  │  subscriptions│    │  ├─ abstract          │  │
│  │  notifications│    │  ├─ authors           │  │
│  │               │    │  ├─ source_type       │  │
│  │  item_scores  │◄──►│  ├─ embedding (vec)   │  │
│  │  (evaluations │    │  ├─ published_at      │  │
│  │   link by     │    │  └─ metadata (json)   │  │
│  │   item_id)    │    │                       │  │
│  └──────────────┘    │  concepts table       │  │
│                       │  ├─ concept_id        │  │
│                       │  ├─ label             │  │
│                       │  ├─ embedding (vec)   │  │
│                       │  └─ linked_items[]    │  │
│                       └───────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### Deliverables

| # | Deliverable | Detail |
|---|------------|--------|
| 1 | Add `lancedb` crate dependency | Rust-native, embedded, zero-server |
| 2 | `VectorStore` module in radar-core | Manages LanceDB connection, table creation, embedding ops |
| 3 | Embedding generation at ingest time | After normalize step, before keyword gate. Use a configurable embedding model (default: local model or API-based) |
| 4 | Dual-write on item ingest | SQLite gets relational record, LanceDB gets item + embedding vector |
| 5 | `corpus_search(query, profile_id?, limit, filters)` MCP tool | Semantic search across all accumulated items. Agents can ask "find papers about attention mechanism scaling" without knowing exact keywords |
| 6 | `corpus_similar(item_id, limit)` MCP tool | "Papers like this one" — returns nearest neighbors by embedding distance |
| 7 | Semantic pre-filter option | Replace or augment keyword gate with embedding similarity threshold. Profile can specify `scoring_mode: keyword | semantic | hybrid` |
| 8 | Concept extraction and clustering | Periodic background job: cluster item embeddings → extract emergent concept labels → store in concepts table |
| 9 | `corpus_concepts(profile_id?)` MCP tool | "What research themes are emerging?" — returns concept clusters with member counts and trend direction |
| 10 | Backfill pipeline | One-time job to embed all existing items when LanceDB is first enabled |

**Exit criteria:** An agent can semantically search the accumulated research corpus, find similar papers, and discover emergent concept clusters — without specifying exact keywords.

---

## Phase 3: Agentic Reasoning Tools

**Goal:** Give agents first-class tools to reason about research, not just retrieve it. Move from "here are your matches" to "here's what this means for your project."

### New MCP Tools

| Tool | Purpose | Agent use case |
|------|---------|---------------|
| `research_brief(profile_id, focus?, format?)` | Generate a structured research briefing from recent matches, trends, and concepts | Agent opening a work session asks "what's new in my research areas?" |
| `relevance_explain(item_id, context)` | Given an item and a project context string, explain why/how the research is relevant | Agent evaluating whether a paper matters for a specific codebase or initiative |
| `gap_analysis(profile_id, project_context)` | Compare accumulated research against project goals, surface under-explored areas | Agent planning next research directions |
| `trend_detect(profile_id, window_days?, min_cluster_size?)` | Detect acceleration/deceleration in research themes over time | Agent monitoring if a field is heating up or cooling down |
| `cross_pollinate(profile_ids[])` | Find items relevant across multiple profiles — "what connects AI safety to your legal research?" | Multi-domain agent finding unexpected intersections |
| `citation_graph(item_id, depth?)` | Follow citation links via Semantic Scholar API to map influence networks | Agent tracing the lineage of an important finding |
| `digest_compose(profile_id, period, audience, format)` | Compose a human-readable digest (markdown, email, Slack post) from accumulated activity | Agent preparing a weekly research roundup for a team |

### Agentic Workflow Patterns

```
Agent Session Start:
  1. agent calls profile_status() → sees 12 unread matches
  2. agent calls research_brief() → gets structured summary
  3. agent calls relevance_explain() on top 3 items → understands project impact
  4. agent calls activity_acknowledge() → marks as seen
  5. agent proceeds with informed context

Proactive Research:
  1. agent detects it's working on a novel problem
  2. agent calls scan_now(force=true, reason="exploring new approach")
  3. agent calls corpus_search("the concept it's exploring")
  4. agent synthesizes findings into its work

Cross-Domain Discovery:
  1. agent calls cross_pollinate([ai_profile, legal_profile])
  2. finds paper on AI governance that maps to both
  3. agent surfaces it to user with relevance_explain() for each domain
```

**Exit criteria:** An agent can autonomously open a session, understand what's new in research, explain why it matters to the current project, and identify gaps — without human prompting.

---

## Phase 4: Multi-Source Intelligence Network

**Goal:** Expand beyond AI research. Prove the extensible architecture by adding fundamentally different source types.

### New Source Adapters

| Source | Domain | Unique challenges |
|--------|--------|------------------|
| Semantic Scholar (enhanced) | Academic | Citation graph traversal, author network mapping, field-of-study taxonomy |
| HuggingFace Daily Papers | AI/ML | Model card integration, benchmark result extraction |
| RSS/Blog aggregator | AI labs, tech blogs | URL canonicalization, feed drift detection, tracking param stripping |
| LexisNexis / legal feeds | Legal | Case citation parsing, jurisdiction tagging, regulatory change detection |
| Product Hunt / HN | Tech/startups | Relevance decay, hype filtering, "actually useful" signal extraction |
| Patent databases | IP/Innovation | Patent family grouping, claims extraction, prior art linking |
| Government registers | Policy/regulation | Multi-jurisdiction monitoring, amendment tracking |

### Source Intelligence Features

| Feature | Detail |
|---------|--------|
| Source quality scoring | Each source earns a reliability score based on hit rate, uniqueness of contributions, and staleness |
| Adaptive polling frequency | Sources that produce more relevant hits get polled more frequently |
| Cross-source dedup (fuzzy) | LanceDB embedding similarity for fuzzy dedup across sources (same research, different source) |
| Source health dashboard | `source_health()` evolves into a rich diagnostic surface |

**Exit criteria:** At least 2 non-academic sources running in production alongside the original AI research sources, with cross-source fuzzy dedup working.

---

## Phase 5: Learning and Adaptation

**Goal:** The system gets smarter over time. Agent feedback loops refine scoring, routing, and surfacing.

### Feedback Mechanisms

| Mechanism | How it works |
|-----------|-------------|
| Implicit signal: read/skip ratio | Track which matches the agent actually reads via `match_get` vs skips |
| Implicit signal: citation in output | Detect when agent references a research item in its generated code/docs |
| Explicit signal: `match_feedback(item_id, signal)` | Agent can explicitly rate relevance: `useful`, `not_relevant`, `already_known`, `groundbreaking` |
| Profile auto-tuning | Periodic job analyzes feedback signals, suggests keyword/threshold adjustments via `profile_suggest_updates()` |

### Scoring Evolution

| Phase | Scoring method | Accuracy | Cost |
|-------|---------------|----------|------|
| V1 (Phase 1) | Keyword gate → LLM | Baseline | $$ |
| V2 (Phase 2) | Semantic similarity → LLM | Better recall | $$ |
| V3 (Phase 5) | Learned embeddings → fine-tuned scorer | Best | $ (amortized) |

The learned scoring model:
1. Accumulates (item_embedding, profile_embedding, feedback_signal) triples
2. Trains a lightweight cross-encoder or re-ranker on this data
3. Replaces or supplements LLM scoring for profiles with enough feedback data
4. Falls back to LLM scoring for cold-start profiles

**Exit criteria:** System demonstrably improves match quality over time based on accumulated feedback, measured by read/skip ratio trending upward.

---

## Phase 6: Collaborative Intelligence

**Goal:** Multiple agents and humans share research intelligence. Radar becomes a shared research brain.

### Multi-Tenant Features

| Feature | Detail |
|---------|--------|
| Profile sharing | Multiple agents/users can subscribe to the same profile's results |
| Collaborative annotations | Agents and humans can annotate items with context ("this relates to our auth rewrite") |
| Cross-workspace knowledge | Optional federated mode: share anonymized concept clusters across workspaces |
| Research channels | Themed collections that aggregate across profiles (e.g., "AI Safety Weekly" pulls from 3 profiles) |

### Agent-to-Agent Research Protocol

```
Agent A (working on auth):
  → finds paper on formal verification of auth protocols
  → calls match_annotate(item_id, "directly applicable to Triumvirate auth module")

Agent B (working on testing):
  → calls corpus_search("formal verification testing")
  → finds Agent A's annotated item
  → uses it to improve test strategy
```

**Exit criteria:** Two independent agent sessions demonstrably share research context through Radar's knowledge layer.

---

## Technical Evolution Summary

```
Phase 1    Phase 2    Phase 3    Phase 4    Phase 5    Phase 6
───────    ───────    ───────    ───────    ───────    ───────
SQLite     + LanceDB  + Reasoning + Multi-src + Learning  + Multi-agent
MCP tools  + Semantic  + Briefings + Fuzzy     + Feedback  + Annotations
arXiv      + Corpus    + Gap       + Legal,    + Auto-tune + Shared
Discord    + Concepts  + Analysis  + Patents   + Learned   + Federated
Telegram   + Similar   + Cross-    + Adaptive  + Scorer    + Channels
Keyword    + Clusters    pollinate + Polling
LLM score  + Hybrid
             score

Capability: Notify → Search → Reason → Monitor → Learn → Collaborate
```

## Dependency Graph

```
Phase 1 ──► Phase 2 ──► Phase 3
                │           │
                ▼           ▼
            Phase 4 ──► Phase 5 ──► Phase 6
```

- Phase 2 requires Phase 1 (needs items flowing to embed)
- Phase 3 requires Phase 2 (reasoning tools depend on semantic search)
- Phase 4 can start after Phase 2 (new sources need embedding pipeline)
- Phase 5 requires Phase 3 + Phase 4 (needs agent interactions + diverse data to learn from)
- Phase 6 requires Phase 5 (sharing requires stable scoring + annotations)

## Crate Evolution

| Phase | radar-core | radar-mcp | radar-cli |
|-------|-------------|------------|------------|
| 1 | Types, SQLite, sources, scoring, notifiers | 16 MCP tools | scan-worker, doctor, backup |
| 2 | + LanceDB VectorStore, embedding pipeline | + corpus_search, corpus_similar, corpus_concepts | + embed-backfill |
| 3 | + reasoning engine, trend detection | + research_brief, relevance_explain, gap_analysis, cross_pollinate, citation_graph, digest_compose | + digest-cron |
| 4 | + new source adapters, fuzzy dedup | + source_quality, adaptive_polling | + source-add CLI |
| 5 | + feedback store, learned scorer, profile auto-tuner | + match_feedback, profile_suggest_updates | + retrain, eval-accuracy |
| 6 | + annotation store, sharing protocol | + match_annotate, channel_*, profile_share | + federate |

## Non-Goals (Kept Out of All Phases)

- **Web UI / dashboard** — Agents are the interface. If humans need a view, the agent composes it.
- **Real-time streaming** — Batch scan with configurable frequency is sufficient. SSE/WebSocket adds complexity without proportional value for research monitoring.
- **General-purpose RAG** — Radar is a research intelligence service, not a generic RAG framework. It doesn't serve arbitrary document queries.
- **Model training infrastructure** — Phase 5's learned scorer uses lightweight fine-tuning, not large-scale training. If it gets complex, it should be a separate tool.
- **Social features** — No comments, likes, follows. Agent-to-agent annotation is functional, not social.
