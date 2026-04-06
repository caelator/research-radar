1. **Section: 4. V1 scope**
   - **Suggest:** Expand the listed schema deliverables from `Nodes`, `Edges`, `Documents`, `Tasks` to a concrete minimum canonical set such as `entities`, `relations`, `artifacts`, `events`, `embeddings`, and `projections`, and require the ADD to define each table’s primary key, required columns, foreign-key-like reference fields, and ownership rules.
   - **Why:** The current V1 scope names are too plugin-shaped and will force later redesign when `council` and `evolve` need conversation/event and derived-state storage. A stable shared language needs canonical primitives first, then plugin views on top.

2. **Section: 3. Recommended approach with rationale**
   - **Suggest:** Add a concrete rule that embeddings are not stored inline on every domain row; instead, use a separate `embeddings` collection keyed by `(subject_urn, embedding_space, embedding_model, embedding_version, chunk_id)` and make all semantic search flow through that index layer.
   - **Why:** Storing vectors directly in each domain collection makes multi-collection retrieval, model migration, and re-embedding expensive and inconsistent. A dedicated embedding table preserves domain typing while giving one uniform retrieval surface.

3. **Section: 8. Control flow**
   - **Suggest:** Insert an explicit identity-and-namespace step between `Plugin Binding` and `Write Path`: every plugin must receive a scoped `PluginContext { plugin_id, workspace_id, capabilities }`, and every write must carry a globally unique `urn` plus `tenant/workspace` and `origin_plugin` fields enforced by `layers`.
   - **Why:** Without enforced namespacing at bind time, plugins will collide on IDs, leak data across workspaces, and recreate silo behavior through ad hoc conventions. This is a control-plane concern, not something to leave implicit in schema docs.

4. **Section: 4. V1 scope**
   - **Suggest:** Require the API surface design to include capability-specific interfaces instead of one broad `DataLayer` trait, for example `EntityStore`, `RelationStore`, `SemanticIndex`, `EventLog`, and `QueryPlanner`, plus one plugin-facing `DataLayerHandle` that exposes only allowed capabilities.
   - **Why:** A single catch-all trait will become unstable quickly and make plugin bindings too coarse. Splitting the surface now gives clearer ownership boundaries, safer plugin integration, and a more realistic path to incremental implementation inside `layers`.

5. **Section: 9. Risks and open questions**
   - **Suggest:** Convert `Embedding Model Alignment` from an open question into a design decision: define approved embedding spaces up front, require each vector to declare `space_id`, and state that cross-space search is disallowed unless `layers` provides an explicit fusion strategy.
   - **Why:** If this stays unresolved, the whole “unified semantic store” claim is shaky. Cross-collection retrieval quality will silently fail unless the architecture treats embedding provenance and compatibility as first-class schema constraints.

6. **Section: 10. Validation plan**
   - **Suggest:** Add one required validation artifact: a full migration mapping matrix for `research-radar`, `openclaw-pm`, `council`, and `evolve` showing current persisted entity -> target unified schema rows -> deprecated local store path, with at least one worked end-to-end example per plugin.
   - **Why:** The current validation plan is too centered on `research-radar`. That risks designing a shared substrate that only one plugin fits cleanly, which would surface too late in implementation when other plugins need incompatible shapes or duplicate persistence.
