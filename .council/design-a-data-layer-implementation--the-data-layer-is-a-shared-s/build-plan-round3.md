# Architecture Design Document: Blob + Separated Semantic Index
## The Data Layer is a Shared Storage and Retrieval Substrate
### build-plan-round3 · 2026-04-07

---

## Decision Log

### Gemini (conceptual-risk)
- **ACCEPT**: LanceDB couples storage and semantic indexing — this creates schema coordination overhead that scales with every new plugin and embedding space. The ADD must reframe LanceDB as a derived index view, not the canonical store, and introduce blob storage as the authoritative layer.
- **ACCEPT**: Content-addressable blobs are the right canonical unit — modern AI memory research (Multimodal Memory, Ollama-style content-addressed model weights, KV Cache blobs, MemTrust TEE-encrypted blobs) all converge on immutable content-addressable storage as the foundation. The ADD should make this the load-bearing design choice.
- **ACCEPT**: Schema evolution on LanceDB collections is painful — coupled storage/index means any schema change to one plugin's data ripples through the shared collection. Blob + sep-index isolates each plugin's schema to its blob namespace; the semantic index is rebuilt from blobs, not mutated in place.

### Claude (implementation-risk)
- **ACCEPT**: The migration from round2's LanceDB-centric design to blob+sep-index must be concrete and traceable. Every existing artifact in every plugin must have a clear path to the new model — no hand-waving about "we'll figure it out during migration."
- **ACCEPT**: Plugin isolation is a first-class property, not a courtesy. The blob namespace model must enforce that plugin A can never accidentally read, write, or corrupt plugin B's blobs without explicit cross-plugin API calls.
- **ACCEPT**: The "semantic index as derived view" property must be validated in V1 with a concrete rebuild workflow — prove that a plugin can delete and fully rebuild its LanceDB index from its blob store without data loss.

---

## 1. Goal and constraints

### Goal
Design the Caelator data layer as a **content-addressable blob store** with a **plugin-local semantic index** built on top. The blob store is the single source of truth for all ecosystem plugins. The semantic index is a derived, rebuildable view — not the canonical store.

### What layers actually needs from a data layer
| Need | Round1/2 assumption | Round3 reality |
|---|---|---|
| Persistent state for plugins | LanceDB tables are the store | Blob storage is the store; LanceDB is optional |
| Semantic / vector search | LanceDB collection with inline vectors | A plugin-local derived index rebuilt from blobs |
| Schema coordination across plugins | Shared LanceDB schema with version negotiation | Each plugin owns its blob schema; the index is derived |
| Immutable audit trail | Append-only LanceDB or file logs | Blobs are immutable by design; content-addressing enforces this |
| Cross-plugin queries | Single LanceDB store with JOINs | Blobs are queryable by plugin+kind+id; cross-plugin semantic search uses a shared index registry |
| Schema evolution | LanceDB migration in place | New blob schema version; old blobs untouched; index rebuilt |
| TEE / encrypted memory | Not addressed | MemTrust-style encryption wraps blob content; keys held by TEE |

### Hard constraints
- **Single-user local dev environment for V1** — no network sync, no multi-machine concurrency.
- **Plugins must not maintain isolated persistent storage silos** for shared state — they must converge on the blob store as canonical.
- **No LanceDB (or any vector DB) as a mandatory dependency** — the semantic index is a plugin-local derived view, swappable independently of the blob store.
- **V1 is design + documentation only** — no Rust implementation or plugin refactoring.

### What LanceDB was doing in round1/round2 that it shouldn't have been
The previous design used LanceDB as the canonical store, coupling two distinct responsibilities:
1. **Storage** — persisting structured data with schema enforcement
2. **Semantic indexing** — vector similarity search over content

These concerns evolve on different timescales and have different failure modes. Coupling them caused:
- Schema coordination overhead: every plugin adding a new embedding space or field required negotiating changes to the shared LanceDB collection schema.
- Inflexibility: switching embedding models or vector dimensions required a full LanceDB migration, not a rebuild.
- Confusion about canonicality: plugins couldn't tell whether their data lived in LanceDB or in their own SQLite/file stores.

---

## 2. Core insight: storage and semantic indexing are separate concerns

The blob is the fundamental storage unit. It is:
- **Content-addressable** — identified by the SHA-256 of its content, not by a plugin-generated ID.
- **Immutable** — once written, the content never changes. Updates produce a new blob with a new address.
- **Versioned by reference** — the "current" version of an entity is a pointer (URN) in the blob index, not a mutation of the blob itself.

The semantic index (LanceDB, Qdrant, or any vector DB) is a **derived view** built from blobs. It can be:
- **Rebuilt at any time** — wipe the index, re-scan all blobs, regenerate vectors.
- **Swapped** — one plugin can use LanceDB, another could theoretically use a different vector DB, as long as they both consume the same blob source of truth.
- **Isolated per plugin** — each plugin has its own semantic index namespace, keyed by `plugin_id + space_id`.
- **Encrypted independently** — blobs can be TEE-encrypted at rest; the semantic index operates on decrypted content within the plugin's trust boundary.

This separation eliminates schema coordination at the storage layer. Plugins do not negotiate shared schemas — they each own a content-addressable namespace. Cross-plugin semantic search works through a shared **blob registry** (a lightweight index of known URNs), not through a shared database.

---

## 3. Blob storage semantics

### 3.1 Content-addressing scheme

Every blob is identified by a **canonical URN**:

```
urn:caelator:{plugin_id}:{kind}:{sha256}
```

| Component | Value | Rules |
|---|---|---|
| `plugin_id` | e.g. `layers`, `research-radar`, `openclaw-pm`, `council` | lowercase alphanumeric + hyphens |
| `kind` | plugin-defined entity type, e.g. `route-correction`, `finding`, `task`, `memory` | lowercase alphanumeric + underscores |
| `sha256` | Lowercase hex SHA-256 of the serialized blob content | Exactly 64 characters |

**Examples:**
```
urn:caelator:research-radar:finding:a3f8b2c1d4e5f6789012345678901234abcd5678ef9012345678901234abcd
urn:caelator:layers:route-correction:b7c9d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0
urn:caelator:openclaw-pm:task:deadbeefcafebabe0123456789abcdef0123456789abcdef0123456789abcdef
urn:caelator:council:memory:feedfacefacedbadc0123456789abcdef0123456789abcdef0123456789abcdef
```

The URN is derived from content, not generated independently. This means:
- **Deduplication is free** — the same content always maps to the same URN.
- **Integrity is verifiable** — any consumer can re-hash the content and confirm the URN matches.
- **Immutability is enforced by construction** — you cannot update a blob, you create a new one.

### 3.2 Storage tiers

Blobs are organized into storage tiers based on size and access patterns:

#### Tier 1 — Inline blobs (small, hot)
Blobs under ~4 KB are stored **inline** inside the **blob index** (a structured store, not the vector DB). The full serialized content lives in the index row alongside its metadata. No file I/O required for read/write.

```
blob_index row: { urn, plugin_id, kind, size_bytes, content_inline, metadata_json, created_at }
```

**Use case**: Route corrections, short findings, task descriptions, council memories.

#### Tier 2 — Packed blobs (medium, warm)
Blobs between ~4 KB and ~16 MB are stored as **files on disk** in a content-addressed directory structure, with the blob index row holding a **file descriptor** (path + hash).

```
~/.caelator/blobs/{plugin_id}/{sha256[0:2]}/{sha256}.blob
```

```
blob_index row: { urn, plugin_id, kind, size_bytes, storage_tier = "packed", file_path, content_hash, metadata_json, created_at }
```

**Use case**: Long findings with embedded context, council transcripts, plugin state snapshots.

#### Tier 3 — Dedicated blobs (large, cold)
Blobs over ~16 MB are stored as **dedicated files** with optional external reference tracking. These are written once and rarely accessed.

```
blob_index row: { urn, plugin_id, kind, size_bytes, storage_tier = "dedicated", file_path, external_ref, metadata_json, created_at }
```

**Use case**: Plugin export bundles, full research-radar article snapshots.

#### Tier 4 — External blobs (off-platform, referenced)
Blobs that live outside the Caelator store (e.g., a PDF already on disk, a URL) are registered with a **content hash** but not stored. The blob index records the external location and the hash for integrity verification on access.

```
blob_index row: { urn, plugin_id, kind, storage_tier = "external", external_url, content_hash, metadata_json, created_at }
```

**Use case**: Research documents, existing files the plugin wants to reference without copying.

### 3.3 Blob index schema

The **blob index** is a small, structured store (SQLite for V1, one file at `~/.caelator/blob-index.sqlite`) that tracks all known blobs and their current "live" pointers.

```sql
CREATE TABLE blobs (
    urn          TEXT PRIMARY KEY,        -- urn:caelator:{plugin}:{kind}:{sha256}
    plugin_id    TEXT NOT NULL,
    kind         TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    storage_tier TEXT NOT NULL,           -- 'inline' | 'packed' | 'dedicated' | 'external'
    content_inline   BLOB,                -- Tier 1 only
    file_path    TEXT,                    -- Tier 2/3: path on disk
    external_url TEXT,                    -- Tier 4: external URL or path
    content_hash TEXT NOT NULL,           -- sha256 for integrity verification
    metadata     TEXT,                    -- JSON: tags, origin, provenance, encryption_key_ref
    created_at   INTEGER NOT NULL         -- Unix timestamp
);

CREATE TABLE live_refs (
    -- Tracks "current" URN for each (plugin_id, entity_key).
    -- The entity_key is plugin-defined (e.g., finding_id, task_id, route_id).
    -- When an entity is updated, a NEW blob is written and live_ref is atomically updated.
    plugin_id    TEXT NOT NULL,
    entity_key   TEXT NOT NULL,            -- plugin's native ID for this entity
    current_urn  TEXT NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (plugin_id, entity_key),
    FOREIGN KEY (current_urn) REFERENCES blobs(urn)
);

CREATE TABLE tombstones (
    -- Soft-delete log. When an entity is deleted, the live_ref is removed
    -- and a tombstone entry is created.
    urn          TEXT PRIMARY KEY,
    deleted_at   INTEGER NOT NULL,
    reason       TEXT
);

CREATE TABLE embedding_spaces (
    -- Registry of known embedding spaces per plugin.
    -- Each space has a fixed dimensionality and model.
    space_id     TEXT PRIMARY KEY,         -- e.g. "research-radar_text_v1"
    plugin_id    TEXT NOT NULL,
    model_name   TEXT NOT NULL,
    dimension    INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
);
```

### 3.4 Write semantics

**Single blob write (atomic):**
1. Serialize the content.
2. Compute `sha256(content)` → `content_hash`.
3. Compute the full URN: `urn:caelator:{plugin_id}:{kind}:{content_hash}`.
4. Determine storage tier by size.
5. Write to blob_index (and to disk for Tier 2/3).
6. Atomically update `live_refs` to point to the new URN.

**Atomicity guarantee**: Steps 1–5 are idempotent (same content → same URN → safe to retry). Step 6 is the only stateful mutation and is protected by the write coordinator.

**No in-place updates**: To update an entity, write a new blob with new content → new hash → new URN. Update `live_refs` to point to the new URN. The old blob remains accessible by its URN (and becomes eligible for tombstoning if the old URN is no longer reachable via any live_ref).

---

## 4. Separated index architecture

### 4.1 The semantic index is a derived view

The semantic index (LanceDB for V1) is a **plugin-local derived view** over blobs. It is:
- **Owned by a single plugin** — plugin A's semantic index never mixes plugin B's vectors.
- **Rebuildable at any time** — scan all blobs for a plugin, re-embed, write to the plugin's LanceDB collection.
- **Swappable** — a plugin can switch from LanceDB to Qdrant or Chroma by rebuilding from the same blob source.

### 4.2 Per-space LanceDB collections

Each `(plugin_id, space_id)` pair maps to one LanceDB collection:

```
collection name: {plugin_id}__{space_id}
e.g. research-radar__finding_text_v1, layers__route_corrections_v1
```

Collection schema (per space):

```sql
-- One row per embedded chunk within a blob
CREATE SCHEMA embedding_vectors (
    chunk_id      TEXT NOT NULL,       -- plugin-defined chunk identifier
    blob_urn      TEXT NOT NULL,       -- reference to canonical blob
    plugin_id     TEXT NOT NULL,
    space_id      TEXT NOT NULL,
    vector        FIXED_SIZE_LIST(Float32, 384|768|1024|1536),
    content_text  TEXT NOT NULL,       -- raw text that was embedded
    offset        INTEGER,              -- byte offset in original content
    created_at    INTEGER NOT NULL,
    -- PRIMARY KEY is (chunk_id, blob_urn) within each collection
);
```

### 4.3 Index rebuild workflow

```
[Blobs] ──scan──> [Embedding job] ──embed──> [LanceDB per space]
```

1. List all blobs for `(plugin_id, target_space)`.
2. For each blob, load content from blob store.
3. Chunk the content (plugin-defined chunking strategy).
4. Run each chunk through the embedding model.
5. Upsert all `(chunk_id, blob_urn, vector, content_text)` rows into the space collection.
6. On completion, record the rebuild timestamp in `embedding_spaces`.

**Chunked rebuild with write yielding**: Large rebuilds must not hold the write coordinator for the full duration. The rebuild acquires a **low-priority lease** that yields after N chunks to let normal writes proceed. Alternatively: rebuild into a shadow collection (`research-radar__finding_text_v1__rebuild_20260407`) then atomically rename on success.

**Incremental index update**: On blob write, the plugin's `SemanticIndex` API immediately indexes the new chunk (within the same write transaction via the write coordinator). Full rebuilds are for recovery and model upgrades only.

### 4.4 Why not a shared LanceDB collection

Round1/2 used a shared LanceDB collection across all plugins. This created:
- Schema coordination tax: adding a new plugin's entity type required updating the shared schema.
- Embedding space conflicts: `research-radar` might use 768-dim vectors while `openclaw-pm` uses 1024-dim — these cannot coexist in one LanceDB table.
- Ownership ambiguity: which plugin owns the collection schema? Who approves breaking changes?

With blob+sep-index, each plugin owns its embedding space collections. The blob index is the only shared state, and it has a fixed, minimal schema (urn, plugin_id, kind, tier, metadata). No coordination required.

---

## 5. Schema evolution

### The blob layer never migrates

Blobs are immutable. Once written, the serialized format is permanent. This means:
- **No schema migrations on blobs** — ever.
- **Schema version lives in the blob content** — the `metadata` JSON field carries the schema version the plugin used to serialize.
- **Backwards-compatible reads** — each plugin knows how to read its own historical schema versions.

### Schema changes produce new blobs

When a plugin evolves its entity schema:
1. Serialize the updated entity using the new schema version.
2. Write it as a new blob with a new URN.
3. Update `live_refs` to point to the new URN.
4. Old blobs remain valid and accessible.

This is the same pattern as Git commits — history is append-only, HEAD moves forward.

### The semantic index is rebuilt, not migrated

If the embedding model changes:
1. Write new blobs with the updated content (if content schema also changed).
2. Run the full rebuild job to re-embed all blobs into a new `space_id` (e.g., `finding_text_v2`).
3. Register the new space in `embedding_spaces`.
4. Switch reads to the new space.

Old index collections remain on disk until manually deleted. No data loss risk.

### Version negotiation

Each plugin declares its supported blob schema version range in `PluginContext`:

```rust
struct PluginContext {
    plugin_id: String,
    workspace_id: String,
    capabilities: Vec<Capability>,
    supported_blob_schema_range: (u32, u32), // min, max version supported
}
```

On `layers_data::open()`, the blob index verifies the plugin's declared range against what it finds in `live_refs`. If a plugin encounters a blob with a schema version outside its supported range, it returns a `SchemaVersionError` with the blob URN and version — the plugin can then decide whether to upgrade or skip that blob.

---

## 6. Plugin isolation model

### Each plugin owns its blob namespace

```
~/.caelator/
├── blob-index.sqlite          # Shared registry: urn → location + metadata
├── blobs/
│   ├── research-radar/        # Plugin-owned blob storage directory
│   │   ├── finding/
│   │   │   ├── a3/f8/         # sha256 prefix for file layout
│   │   │   │   └── a3f8b2...blob
│   │   │   └── ...
│   │   └── memory/
│   ├── layers/
│   │   └── route-correction/
│   ├── openclaw-pm/
│   │   └── task/
│   └── council/
│       └── memory/
└── semantic-index/             # LanceDB directory (plugin-local collections)
    ├── research-radar__finding_text_v1.lance
    ├── layers__route_corrections_v1.lance
    └── council__memory_v1.lance
```

### Enforcement

- **File system level**: Each plugin's `blobs/` subdirectory is written only by its plugin. The OS file permissions enforce this (0600 on directories).
- **URN namespace**: `urn:caelator:{plugin_id}:...` — the `plugin_id` in the URN must match the writing plugin's declared ID. The blob index rejects writes where `urn.plugin_id != calling_plugin.plugin_id`.
- **live_refs ownership**: Each `(plugin_id, entity_key)` row is owned by one plugin. Cross-plugin reads go through a queryable cross-plugin API, not direct table access.
- **No shared mutable state**: The only shared mutable state is `blob-index.sqlite` (which has a fixed schema) and `live_refs`. The semantic index is entirely plugin-local.

### Cross-plugin write protocol

If plugin A needs to write to plugin B's namespace (e.g., `openclaw-pm` annotating a `research-radar` finding):

1. Plugin A calls `BlobStore::register_reference(owning_plugin_id, target_urn, annotation_metadata)`.
2. This writes a **new blob in plugin A's namespace** (type: `cross_ref`) that references the target URN.
3. The target blob is **never modified** — cross-plugin references are always new blobs in the caller's namespace.

This preserves the immutability invariant: no plugin can mutate another plugin's blobs.

---

## 7. Cross-plugin queries

### Shared semantic search without shared schemas

Cross-plugin semantic search works through the **blob registry**, not through shared tables.

**Query flow:**
```
User: "Find all research findings about renewable energy from the last 30 days"
  │
  ├─► QueryPlanner parses intent
  │
  ├─► SemanticIndex.search_across_spaces([
  │     "research-radar__finding_text_v1",
  │     "council__memory_v1",
  │   ], query="renewable energy", top_k=20)
  │     │
  │     └─► LanceDB hybrid query (vector similarity + metadata filter)
  │         Returns: [(blob_urn, score, snippet)]
  │
  └─► BlobStore.resolve_urns([blob_urns...])
        │
        └─► Loads blobs from blob-index, returns full content + metadata
```

**Requirements for cross-plugin search:**
- Each plugin registers its embedding spaces in `embedding_spaces` on startup.
- The `QueryPlanner` queries multiple LanceDB collections in parallel, then resolves URNs back to blobs.
- Plugins opt-in their spaces to cross-plugin search via a capability flag in `PluginContext`.

### Limitations in V1
- Cross-plugin search is **eventually consistent** with blob writes — new blobs are indexed incrementally, so there may be a small lag before they appear in cross-plugin search results.
- No cross-plugin relational queries (e.g., "find all tasks linked to findings about X") in V1 — these require a relation graph API built on top of blob references.

---

## 8. Privacy / TEE path

### MemTrust-style encryption model

The blob store supports **TEE-encrypted blobs** as a first-class tier. The encryption is transparent to the semantic index — the index operates on plaintext content within the plugin's trust boundary.

```
[Plaintext blob content]
    │
    ├─► TEE (Trusted Execution Environment) wraps content
    │     └─► AES-256-GCM with key held inside TEE memory
    │
    └─► Encrypted blob stored at blob path
          └─► blob-index record: { urn, encrypted_ref, encryption_key_ref, metadata }
```

**Key management (V1 sketch, full spec in V2):**
- Encryption keys are stored in the TEE's secure memory (or a key derivation service on the host).
- The `encryption_key_ref` in `blob-index.sqlite` points to the key ID, not the key material.
- Decryption happens inside the TEE before content is passed to the embedding model or returned to the caller.
- Blobs can be marked `encrypted = true` in metadata; the blob store API refuses to return plaintext for encrypted blobs unless the caller provides a valid session within the TEE.

**For V1**: TEE encryption is **documented as the target architecture but not implemented**. The V1 blob store works with plaintext blobs. The encryption key management and TEE integration are tracked as V2 work items.

**Privacy implications of blob+sep-index:**
- Because blobs are immutable and content-addressed, a content hash can be published without revealing content (commitment scheme). This enables privacy-preserving deduplication: two plugins can check if they hold the same blob by comparing hashes without reading each other's data.
- Encrypted blobs plus content-addressed hashes enable a MemTrust-style **provenance chain**: "I hold blob X, which references blob Y, which was encrypted by key K" — all verifiable without decryption.

---

## 9. V1 scope — what to actually build first

### V1 deliverables

1. **This ADD** — fully specify blob+sep-index architecture, signed off by the council.
2. **Blob index SQLite schema** — `~/.caelator/blob-index.sqlite` DDL, including all tables in §3.3.
3. **Blob store reference implementation sketch** — a high-confidence pseudocode/Rust-trait sketch of `BlobStore`, `LiveRefs`, and the write coordinator. No actual LanceDB wiring.
4. **Plugin migration matrix** (see §10) — one row per plugin's existing persisted artifact, mapped to the new model.
5. **V1 validation suite** — pseudocode walkthroughs proving: (a) blob write + live_ref update is atomic, (b) semantic index can be fully rebuilt from blobs, (c) cross-plugin search resolves URNs correctly, (d) tombstoned blobs are not returned by live queries.

### V1 does NOT include
- LanceDB integration code (stub it out; prove the rebuild contract).
- TEE encryption implementation (design only).
- Cross-plugin relational graph API.
- Network sync or multi-machine concurrency.
- Production Rust crate `layers-data` (V2 deliverable).

### V1 success criteria
- Every plugin's existing persisted artifact fits into the blob model (no "and we'll add a fifth tier later").
- The semantic index rebuild workflow is proven end-to-end in pseudocode for at least one plugin (`research-radar` findings).
- The blob URN scheme is consistent with the `urn:caelator:{plugin}:{kind}:{sha256}` format across all existing artifacts.
- Plugin isolation is verified: no plugin can address another plugin's blobs without going through the cross-plugin reference API.

---

## 10. Migration strategy

### Current state
- `research-radar`: SQLite pipeline for findings; Phase 1 LanceDB integration for research-radar findings (from round1).
- `openclaw-pm`: Plugin state stored in its own local storage.
- `layers` (route corrections): State in layers' own storage.
- `council`: Memory in council's own storage.

### Migration matrix

| Plugin | Artifact | Current store | Target: blob tier | Target: semantic index | Notes |
|---|---|---|---|---|---|
| `research-radar` | Findings (short, <4KB) | SQLite | Blob index inline row | `research-radar__finding_text_v1` | ETL from SQLite → new blob + live_ref; drop old LanceDB collection and rebuild from blobs |
| `research-radar` | Findings (long, >4KB) | SQLite | Packed blob (file) | Same index | File path registered in blob_index |
| `research-radar` | Article snapshots | SQLite (full text) | Dedicated or external blob | Same index | External URL if already on disk |
| `research-radar` | Existing Phase 1 LanceDB embeddings | LanceDB | **Discard** | Rebuild from blobs | Source of truth moves to blobs; LanceDB is fully rebuilt, not migrated |
| `openclaw-pm` | Tasks | Plugin local SQLite | Blob index inline row | `openclaw-pm__task_v1` | New write path; historical tasks ETL'd as packed blobs |
| `openclaw-pm` | Task relationships | Plugin local | Blob (relation kind) | `openclaw-pm__task_v1` | Relations stored as blobs with `kind=relation` |
| `layers` | Route corrections | layers storage | Blob index inline row | `layers__route_correction_v1` | ETL existing route corrections to blobs |
| `council` | Memory nodes | council storage | Blob index inline row (small) or packed (transcript) | `council__memory_v1` | Historical memories ETL'd; new memories written directly as blobs |
| `council` | Council sessions | council storage | Packed blob | `council__memory_v1` | Full transcript stored as packed blob; summary embedded |

### Migration phases

**Phase 0 — Blob index bootstrap**
- Create `~/.caelator/` directory structure.
- Initialize `blob-index.sqlite` with schema.
- Write the `BlobStore` reference implementation sketch.

**Phase 1 — research-radar findings (greenfield path)**
- `research-radar` starts writing new findings directly as blobs (Tier 1/2 based on size).
- New findings are also indexed into `research-radar__finding_text_v1`.
- Old SQLite pipeline is still running in parallel.
- No migration of old data yet.

**Phase 2 — research-radar findings ETL**
- Write a one-time ETL script: scan SQLite → write blobs → update `live_refs`.
- After ETL, verify blob count matches SQLite row count.
- Delete SQLite pipeline.
- Rebuild LanceDB index from blobs (proves the rebuild contract).
- Delete old Phase 1 LanceDB collection.

**Phase 3 — openclaw-pm, layers, council**
- Same ETL pattern for each plugin.
- Run ETL one plugin at a time, verifying blob integrity after each.
- Verify each plugin's semantic index rebuild.

**Phase 4 — Cross-plugin search enablement**
- Register each plugin's embedding spaces in `embedding_spaces`.
- Verify cross-plugin URN resolution via `BlobStore::resolve_urns()`.
- Enable `QueryPlanner::search_across_spaces()`.

### Deduplication during migration
Because blobs are content-addressed, ETL scripts can detect duplicates for free:
```python
for row in sqlite_fetch_all("findings"):
    blob_content = serialize_finding(row)
    candidate_urn = compute_urn(blob_content)
    if not blob_index.exists(candidate_urn):
        blob_index.write(candidate_urn, blob_content, tier=row.size < 4096)
    live_refs.upsert(plugin="research-radar", entity_key=row.id, urn=candidate_urn)
```
If the same finding was somehow stored twice in SQLite, it produces the same blob URN — no duplicate stored.

---

## 11. Risks and open questions

### Risk: Blob index SQLite becomes a bottleneck
- **Severity**: Medium.
- **Mitigation**: V1 uses SQLite for the blob index because it has a fixed, small schema. If contention emerges, the blob index can be swapped for a more concurrent store (e.g., sled) without changing the blob model. The semantic index (LanceDB) is never used for blob metadata.

### Risk: Semantic index lag breaks cross-plugin search freshness
- **Severity**: Low-Medium.
- **Mitigation**: Incremental index updates happen synchronously within the write coordinator's write window. Full rebuild lag is bounded by the rebuild cadence. In V1, lag is acceptable given single-user environment.

### Risk: `live_refs` becomes stale if write coordinator crashes mid-update
- **Severity**: Medium.
- **Mitigation**: Write coordinator uses a write-ahead log (WAL) for `live_refs` updates. On recovery, replay or roll back. V1 can use a simple file lock + SQLite transaction; full WAL is V2.

### Risk: No embedding model governance leads to space proliferation
- **Severity**: Low.
- **Mitigation**: `embedding_spaces` registry requires a unique `(space_id, plugin_id, dimension)` tuple. Plugins can create new spaces but must register them. A lint check in the `SemanticIndex` API rejects unregistered spaces.

### Open question: Chunking strategy is plugin-defined
- Each plugin defines its own chunking logic for embedding. No standard chunk size or overlap policy exists across plugins. This is intentional (different content types chunk differently), but it means cross-plugin search quality depends on each plugin choosing a reasonable chunk strategy.

### Open question: Tombstone retention policy
- Round2 identified tombstone retention as needing a policy. In blob+sep-index, tombstones are simpler: when `live_refs` removes a pointer, the blob remains (immutable) but becomes unreachable via `live_refs`. A separate compaction job can reap unreferenced blobs after a configurable retention window (default: 30 days). The blob index row is not deleted — the `tombstones` table records the deletion event for audit.

### Open question: Projection discipline with blob source of truth
- Round2's `projections` table (derived read models) still applies. A projection is a blob with `kind=projection`. It is never the system of record — always derived from other blobs. The projection carries `built_from_version` in its metadata so staleness is detectable.

---

## 12. Files and storage paths

### Directory layout

```
~/.caelator/
├── blob-index.sqlite              # Blob registry + live_refs + tombstones + embedding_spaces
├── blobs/                         # Content-addressed blob files
│   ├── research-radar/
│   │   ├── finding/
│   │   │   ├── {sha256[0:2]}/{full_sha256}.blob   # Tier 2 packed blobs
│   │   │   └── ...
│   │   ├── memory/
│   │   └── transcript/
│   ├── layers/
│   │   └── route-correction/
│   ├── openclaw-pm/
│   │   └── task/
│   └── council/
│       └── memory/
├── semantic-index/               # LanceDB collections (plugin-local)
│   ├── research-radar__finding_text_v1.lance
│   ├── layers__route_correction_v1.lance
│   ├── openclaw-pm__task_v1.lance
│   └── council__memory_v1.lance
└── logs/                          # Optional: write coordinator logs, rebuild logs
    └── blob-store.log
```

### ADD output location
```
/Users/bri/Documents/GitHub/research-radar/.council/design-a-data-layer-implementation--the-data-layer-is-a-shared-s/build-plan-round3.md
```

### Related files (V2 onwards)
```
layers/crates/layers-data/src/
├── blob.rs          # BlobStore trait + SQLite implementation
├── live_refs.rs     # LiveRefs CRUD + write coordinator
├── semantic.rs      # SemanticIndex trait + LanceDB implementation
├── query.rs         # QueryPlanner
├── migration.rs     # ETL helpers + migration matrix
└── lib.rs
```

---

## 13. Control flow (summary)

```
[Plugin calls layers_data::open(plugin_context)]
        │
        ▼
[Verify schema version compatibility]
        │
        ▼
[Receive typed capability handles: BlobStore, SemanticIndex, QueryPlanner]
        │
        ▼
[Plugin writes entity]
        │
        ├─► BlobStore::write(content) → computes URN → writes blob → updates live_refs (atomic)
        │       │
        │       └─► Blob placed in Tier 1 (inline), 2 (packed), 3 (dedicated), or 4 (external)
        │
        ├─► Write coordinator ensures single-writer + crash-safe WAL
        │
        └─► SemanticIndex::index(blob_urn, content) → chunks → embeds → upserts to LanceDB
                │
                └─► Plugin's LanceDB collection: {plugin_id}__{space_id}

[Cross-plugin query]
        │
        ├─► QueryPlanner::search_across_spaces([spaces...], query)
        │       │
        │       └─► LanceDB: vector similarity + metadata filter
        │           Returns: [(blob_urn, score, snippet)]
        │
        └─► BlobStore::resolve_urns([blob_urns...])
                │
                └─► Loads full content from blob store
                    Returns: [MemoryNode { urn, content, metadata }]

[Rebuild semantic index]
        │
        └─► SemanticIndex::rebuild_space(plugin_id, space_id)
                │
                ├─► Scan all blobs for (plugin_id, space_id)
                ├─► Re-embed all chunks (yield after N chunks for write yielding)
                └─► Upsert to shadow collection → atomic rename on success
```
