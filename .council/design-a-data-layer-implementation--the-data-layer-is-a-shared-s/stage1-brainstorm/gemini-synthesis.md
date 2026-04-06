Here is the revised plan, synthesizing the original goals with the elegance and feasibility contributions. Sections that have been updated are marked with `[CHANGED]`.

## 1. Goal and constraints
- **Goal**: Design a unified, shared data layer embedded within the `layers` codebase to serve as the single source of truth for all Caelator ecosystem plugins (`council`, `openclaw-pm`, `research-radar`, `evolve`).
- **Constraint**: Must strictly use LanceDB as the underlying vector and structured data store for shared, queryable state. No alternative databases may be considered for this purpose.
- **Constraint**: The data layer must support both semantic (vector) search and structured (relational/metadata) queries natively.
- **Constraint**: Plugins must cease maintaining isolated persistent storage silos for shared state; the design must prevent data duplication across the ecosystem.
- **Constraint**: This phase is strictly for architectural design and documentation; no Rust implementation or plugin refactoring is to be executed yet.

## 2. Candidate approaches [CHANGED]

### Approach A: Single Monolithic Collection (The "Everything Bagel")
- **Concept**: A single LanceDB collection (`caelator_memory`) stores every entity across all plugins. It uses a flexible schema with a generic `vector` column, standard metadata (`id`, `plugin_source`, `timestamp`, `type`), and a flexible JSON/Blob column for plugin-specific payload data.
- **Tradeoffs**:
  - *Pros*: Trivial cross-plugin semantic search (one query searches everything). Minimal collection management overhead.
  - *Cons*: Highly sparse data structures. Loss of strong typing at the database level (requires heavy serialization/deserialization logic in Rust). 

### Approach B: Domain-Specific Collections with a Unified Relational Graph
- **Concept**: Define a fixed set of core, strongly-typed domain collections in LanceDB. All collections share a mandatory "Unified Memory Header" (standardized fields like `urn`, `parent_urn`, `tags`) to allow cross-referencing and ecosystem-wide filtering. Embeddings are stored inline.
- **Tradeoffs**:
  - *Pros*: Preserves strong Rust typing. Cleaner schema evolution per domain.
  - *Cons*: Cross-domain semantic searches require multi-collection queries and aggregation in the application logic. Inline embeddings complicate model migrations.

### Approach C: Hybrid (LanceDB for Shared State/Vectors + File-based Append Logs)
- **Concept**: Use LanceDB with strongly-typed canonical collections (`entities`, `relations`, `artifacts`, `projections`) and a dedicated, separate `embeddings` collection keyed by `(subject_urn, embedding_space, chunk_id)`. Simultaneously, exempt pure temporal audit streams (e.g., `council`'s JSONL traces, `openclaw-pm`'s event logs) from LanceDB, retaining them as file-based append-only logs.
- **Tradeoffs**:
  - *Pros*: Avoids forcing non-semantic, append-only audit data into a vector DB, reducing schema complexity by ~40%. A dedicated `embeddings` table gives one uniform retrieval surface while preserving domain typing in other collections. 
  - *Cons*: Requires defining clear boundaries between what is "shared state" (LanceDB) and what is "local audit log" (Filesystem).

## 3. Recommended approach with rationale [CHANGED]
**Recommended: Approach C (Hybrid with Dedicated Embedding Index and File-based Logs)**
- **Rationale**: Rust thrives on strong typing, and LanceDB's Arrow-backed storage is most performant when schemas are well-defined. However, forcing append-only operational logs (like `RunTrace` or `events.jsonl`) into LanceDB adds unnecessary complexity. The Hybrid approach isolates shared, queryable state into LanceDB while leaving temporal streams as simple files.
- **Dedicated Embeddings Table**: Instead of storing vectors directly in every domain collection, semantic search will flow through a central `embeddings` table. This makes multi-collection retrieval, model migration, and re-embedding much more efficient.
- **Greenfield Vector Spaces**: Since `research-radar` currently uses SQLite with keyword `LIKE` matching and does not actually have an embedding infrastructure yet, we are introducing vector spaces for the first time. We can dictate a single embedding model/space standard from day one, avoiding cross-model compatibility issues entirely.
- **Unified Memory Language**: By enforcing a standard `MemoryHeader` struct on every domain collection, plugins can still perform ecosystem-wide operations via relational queries.

## 4. V1 scope [CHANGED]
- **Deliverable**: A comprehensive markdown Architecture Design Document (ADD).
- **Schema Definition**: Definition of the "Unified Memory Language". Instead of abstract top-down names, the schema will map actual existing domain types (e.g., `sources`, `entries`, `profiles`, `project_state`, `tasks`) into a stable, canonical set of tables: `entities`, `relations`, `artifacts`, `embeddings`, and `projections`. The ADD must define each table’s primary key, required columns, reference fields, and ownership rules.
- **API Surface Design**: Instead of a generic `DataLayer` trait with runtime broker bindings, the design will define a typed Rust crate (`layers-data`) that plugins depend on directly as a library. This crate will expose capability-specific interfaces (`EntityStore`, `RelationStore`, `SemanticIndex`, `EventLog`, `QueryPlanner`).
- **Migration Strategy**: A mechanical, concrete ETL mapping matrix for all plugins. For example, explicitly defining how `research-radar`'s 9 SQLite tables migrate (`sources` → `rr_sources` table, `entries` → `rr_entries` with backfilled embeddings, dropping/deferring ephemeral tables like `scan_jobs`).

## 5. V2 / later
- **Implementation**: Writing the actual Rust code in the `layers` codebase to instantiate the LanceDB connection and expose the APIs.
- **Plugin Refactoring**: Updating `research-radar`, `council`, and `openclaw-pm` source code to consume the new `layers` API and deleting their isolated local storage mechanisms.
- **Advanced Graph Queries**: Implementing multi-hop relational queries across LanceDB collections within the `layers` API layer.
- **Data Sync**: Mechanisms for synchronizing local LanceDB storage across different developer machines or to a cloud backup.

## 6. Out of scope / do not build
- Evaluation, benchmarking, or consideration of any database other than LanceDB (e.g., SQLite, Qdrant, Chroma).
- Writing any Rust implementation code for the data layer or modifying existing plugin codebases during this design phase.
- Design of network-level synchronization protocols or multi-user concurrent access models (assume single-user local dev environment for now).
- Designing generic data ingestors for non-Caelator external tools.

## 7. Files, binaries, and storage [CHANGED]
- `docs/architecture/001-unified-data-layer.md`: The canonical output of this phase, detailing the schema, API, and migration plan.
- **Storage Paths**: The design will explicitly accommodate both user-global and workspace-relative storage. 
  - Global data (formerly `~/.research-radar/data.db`) will reside at `~/.caelator/store/*.lance`.
  - Project-scoped data (formerly `.openclaw/pm/`) will reside in a workspace-relative LanceDB instance (e.g., `<workspace>/.caelator/store/*.lance`) or utilize a strict workspace-keying scheme within the global store. 
- `crates/layers-data/src/`: The directory within the `layers` codebase where the unified schema definitions, capability traits, and LanceDB library implementations will reside in V2.

## 8. Control flow [CHANGED]
1. **Initialization**: The Caelator runtime starts. Plugins use direct compile-time crate dependencies to call `layers_data::open(path)`, returning typed capability handles (`EntityStore`, `SemanticIndex`, etc.).
2. **Schema Verification**: `layers_data` ensures all required canonical collections exist and are up-to-date with the current schema.
3. **Identity & Namespace**: Every plugin must establish a scoped `PluginContext { plugin_id, workspace_id, capabilities }`. 
4. **Write Path**: `research-radar` indexes a new file. It constructs an entity implementing the Unified Memory trait, attaching a globally unique `urn`, `workspace_id`, and `origin_plugin`. It calls `entity_store.insert(doc)`.
5. **Read Path**: `council` needs context. It calls `semantic_index.search(query_vector)`. The index layer resolves the `embeddings` table matches back to their canonical domain records and returns normalized `MemoryNode` structs.

## 9. Risks and open questions [CHANGED]
- **LanceDB Rust API Maturity**: LanceDB's Rust SDK is evolving rapidly. Does it currently support all the complex filtering and cross-collection operations we require without dropping down to raw DataFusion queries?
- **Concurrency and File Locking**: `openclaw-pm` relies heavily on `fs2` exclusive file locks and atomic temp-file-rename writes for state mutations. LanceDB uses an MVCC concurrency model. The design must validate whether LanceDB's concurrency guarantees can safely replace `openclaw-pm`'s strict locking patterns without silently degrading atomicity.
- **Schema Evolution**: How do we handle database migrations when the Unified Memory schema needs to change, given that multiple plugins depend on it simultaneously?
- **Embedding Model Alignment**: To ensure a unified semantic store, the architecture must treat embedding provenance as a strict schema constraint. We must define approved embedding spaces up front, require vectors to declare `space_id`, and disallow cross-space semantic searches.

## 10. Validation plan [CHANGED]
- **Schema Desk-Check**: Map existing data structures from all plugins to the proposed canonical LanceDB schema to ensure no data loss or awkward serialization is required.
- **Migration Mapping Matrix**: Produce a full migration mapping matrix for `research-radar`, `openclaw-pm`, `council`, and `evolve`. This must show the current persisted entity, the target unified schema rows, and the deprecated local store path, with at least one worked end-to-end example per plugin.
- **API Walkthrough**: Write pseudocode for the top 3 most common plugin operations using the proposed `layers-data` capability interfaces to verify developer ergonomics.
