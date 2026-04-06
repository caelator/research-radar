## Problem Brief
- Design a shared data layer architecture embedded within the `layers` codebase to serve as the unified storage and semantic retrieval substrate for the Caelator agent ecosystem.
- Establish a unified memory schema that allows disparate plugins (e.g., council, openclaw-pm, research-radar, evolve) to communicate, store, and query data using a common language rather than maintaining isolated storage silos.
- "Done" is defined as a comprehensive architectural design document detailing the unified schema, LanceDB table/collection structure, plugin API surface, binding mechanisms, and a concrete migration strategy for existing plugins.

## Constraints
- Must use LanceDB as the exclusive underlying vector and structured data store.
- The data layer implementation must be housed canonically within the `layers` codebase.
- The schema must support both semantic (vector) search and structured querying across all ecosystem plugins natively.
- Must eliminate persistent storage duplication across plugins (plugins cannot maintain separate persistent databases for shared conceptual entities).

## Success Criteria
- The design includes a fully specified, typed schema (the "unified memory language") capable of representing the data needs of council, openclaw-pm, research-radar, and evolve.
- The document provides a concrete API surface design showing exactly how a plugin connects to, mutates, and queries the `layers` data layer.
- A clear LanceDB collection/table layout is defined, optimizing for both vector embeddings (leveraging research-radar's existing usage) and relational/metadata filtering.
- A step-by-step migration path is documented for transitioning at least one existing plugin (e.g., research-radar) from its isolated storage to the new shared substrate.

## Out of Scope
- Writing the actual Rust/implementation code for the data layer itself (this is purely an architectural design phase).
- Modifying or refactoring existing plugin source code to use the new data layer during this iteration.
- Designing synchronization mechanisms for external, non-Caelator databases or remote cloud data stores.
- Evaluating or benchmarking alternative vector databases or storage engines (LanceDB is strictly mandated).
