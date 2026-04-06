## 1. Goal and constraints
- **Goal**: Design a unified, shared data layer embedded within the `layers` codebase to serve as the single source of truth for all Caelator ecosystem plugins (`council`, `openclaw-pm`, `research-radar`, `evolve`).
- **Constraint**: Must strictly use LanceDB as the underlying vector and structured data store. No alternative databases may be considered.
- **Constraint**: The data layer must support both semantic (vector) search and structured (relational/metadata) queries natively.
- **Constraint**: Plugins must cease maintaining isolated persistent storage silos; the design must prevent data duplication across the ecosystem.
- **Constraint**: This phase is strictly for architectural design and documentation; no Rust implementation or plugin refactoring is to be executed yet.

## 2. Candidate approaches

### Approach A: Single Monolithic Collection (The "Everything Bagel")
- **Concept**: A single LanceDB collection (`caelator_memory`) stores every entity across all plugins. It uses a flexible schema with a generic `vector` column, standard metadata (`id`, `plugin_source`, `timestamp`, `type`), and a flexible JSON/Blob column for plugin-specific payload data.
- **Tradeoffs**:
  - *Pros*: Trivial cross-plugin semantic search (one query searches everything). Minimal collection management overhead.
  - *Cons*: Highly sparse data structures. Loss of strong typing at the database level (requires heavy serialization/deserialization logic in Rust). Mixing disparate embedding types (e.g., code snippets vs. task descriptions) in a single vector space degrades semantic search quality.

### Approach B: Domain-Specific Collections with a Unified Relational Graph
- **Concept**: Define a fixed set of core, strongly-typed domain collections in LanceDB (e.g., `Documents/Snippets` for `research-radar`, `Tasks/Plans` for `openclaw-pm`, `Conversations/Events` for `council`). All collections share a mandatory "Unified Memory Header" (standardized fields like `urn`, `parent_urn`, `tags`, `embedding`) to allow cross-referencing and ecosystem-wide filtering.
- **Tradeoffs**:
  - *Pros*: Preserves strong Rust typing. Maintains separate, optimized vector spaces for different semantic domains. Cleaner schema evolution per domain.
  - *Cons*: Cross-domain semantic searches require multi-collection queries and aggregation in the `layers` application logic. Slightly more complex initialization and schema management.

## 3. Recommended approach with rationale
**Recommended: Approach B (Domain-Specific Collections with a Unified Relational Graph)**
- **Rationale**: Rust thrives on strong typing, and LanceDB's Arrow-backed storage is most performant when schemas are well-defined and dense. A monolithic JSON-blob approach defeats the purpose of a structured store.
- **Unified Memory Language**: By enforcing a standard `MemoryHeader` struct on every domain collection, plugins can still perform ecosystem-wide operations (e.g., "delete all data related to project X" or "find all entities tagged with 'urgent'").
- **Vector Space Integrity**: `research-radar` embeds source code, while `openclaw-pm` embeds natural language tasks. Forcing these into the same vector space degrades cosine similarity reliability. Separate collections ensure semantic boundaries are respected while the `layers` API provides unified access.

## 4. V1 scope
- **Deliverable**: A comprehensive markdown Architecture Design Document (ADD).
- **Schema Definition**: Definition of the "Unified Memory Language" (the shared Rust traits and struct headers) and the specific LanceDB table schemas for the core domains (`Nodes`, `Edges`, `Documents`, `Tasks`).
- **API Surface Design**: Interface definitions for the `layers` data broker (e.g., `pub trait DataLayer { fn query_semantic(...); fn insert_node(...); }`) and the plugin binding mechanism.
- **Migration Strategy**: A concrete, step-by-step documented path for migrating `research-radar` from its isolated SQLite/custom store to the new `layers` LanceDB substrate.

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

## 7. Files, binaries, and storage
- `docs/architecture/001-unified-data-layer.md`: The canonical output of this phase, detailing the schema, API, and migration plan.
- `~/.caelator/store/*.lance`: The standardized local filesystem path where the `layers` core will eventually initialize and manage the LanceDB datasets. Plugins will no longer write to their own `~/.plugin-name/` directories.
- `crates/layers-core/src/data/`: The conceptual directory within the `layers` codebase where the unified schema definitions and LanceDB broker implementations will reside in V2.

## 8. Control flow
1. **Initialization**: The Caelator runtime starts. The `layers` core initializes the single LanceDB connection pool pointing to `~/.caelator/store/`.
2. **Schema Verification**: `layers` ensures all required unified collections (`Tasks`, `Documents`, etc.) exist and are up-to-date with the current schema.
3. **Plugin Binding**: A plugin (e.g., `research-radar`) is loaded. It requests a `DataLayerHandle` from the `layers` host via the plugin API.
4. **Write Path**: `research-radar` indexes a new file. It constructs a `Document` struct (implementing the Unified Memory trait) and calls `handle.insert(doc)`. `layers` routes this to the `Documents` LanceDB collection.
5. **Read Path**: `council` needs context. It calls `handle.semantic_search(query_vector, vec!["Documents", "Tasks"])`. `layers` executes the vector search against the specified collections, normalizes the results into a unified `MemoryNode` format, and returns them to `council`.

## 9. Risks and open questions
- **LanceDB Rust API Maturity**: LanceDB's Rust SDK is evolving rapidly. Does it currently support all the complex filtering and cross-collection operations we require without dropping down to raw DataFusion queries?
- **Schema Evolution**: How do we handle database migrations when the Unified Memory schema needs to change, given that multiple plugins depend on it simultaneously?
- **Embedding Model Alignment**: If `research-radar` and `openclaw-pm` use different embedding models (e.g., text-embedding-3-small vs a local BERT), cross-collection semantic search will yield garbage results. The data layer design must dictate or track embedding model provenance.

## 10. Validation plan
- **Schema Desk-Check**: Map existing data structures from `research-radar` (e.g., its AST snippets) and `openclaw-pm` (e.g., task dependencies) to the proposed LanceDB schema to ensure no data loss or awkward serialization is required.
- **API Walkthrough**: Write pseudocode for the top 3 most common plugin operations (insert document, find related tasks, retrieve conversation history) using the proposed `layers` API to verify developer ergonomics.
- **Migration Dry-Run**: Document the exact script/process required to read `research-radar`'s current storage and write it to the proposed LanceDB schema to ensure the migration path is viable and performant.
