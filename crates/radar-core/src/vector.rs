use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::{ExecutableQuery, QueryBase};

use crate::embedding::{EmbeddingBackend, embedding_text};
use crate::error::{RadarError, Result};
use crate::types::SourceCandidate;

const ITEMS_TABLE: &str = "items";
const CONCEPTS_TABLE: &str = "concepts";

/// LanceDB vector store for semantic search over research items.
pub struct VectorStore {
    db: lancedb::Connection,
    dims: usize,
}

impl VectorStore {
    /// Open or create a LanceDB database at the given path.
    pub async fn open(path: &Path, dims: usize) -> Result<Self> {
        let db = lancedb::connect(path.to_str().unwrap_or("~/.radar/lance"))
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("LanceDB open failed: {e}")))?;
        Ok(Self { db, dims })
    }

    /// Open a temporary LanceDB (for testing). Uses a temp directory that will
    /// be cleaned up when the process exits.
    pub async fn open_temp(dims: usize) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("radar-lance-test-{}", uuid::Uuid::new_v4()));
        Self::open(&dir, dims).await
    }

    /// Get or create the items table.
    async fn items_table(&self) -> Result<lancedb::Table> {
        match self.db.open_table(ITEMS_TABLE).execute().await {
            Ok(t) => Ok(t),
            Err(_) => {
                // Create with a sentinel row then delete it — LanceDB memory mode
                // requires at least one row to persist the table.
                let schema = items_schema(self.dims);
                let zeros: Vec<Option<f32>> = vec![Some(0.0); self.dims];
                let embedding_array = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(zeros)],
                    self.dims as i32,
                );
                let batch = RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![
                        Arc::new(StringArray::from(vec!["__sentinel__"])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(embedding_array) as ArrayRef,
                    ],
                )
                .map_err(|e| RadarError::Other(format!("sentinel batch failed: {e}")))?;
                let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));
                let table = self
                    .db
                    .create_table(ITEMS_TABLE, Box::new(batches))
                    .execute()
                    .await
                    .map_err(|e| RadarError::Other(format!("create items table failed: {e}")))?;
                // Delete the sentinel row
                table
                    .delete("canonical_id = '__sentinel__'")
                    .await
                    .map_err(|e| RadarError::Other(format!("delete sentinel failed: {e}")))?;
                Ok(table)
            }
        }
    }

    /// Get or create the concepts table.
    async fn concepts_table(&self) -> Result<lancedb::Table> {
        match self.db.open_table(CONCEPTS_TABLE).execute().await {
            Ok(t) => Ok(t),
            Err(_) => {
                let schema = concepts_schema(self.dims);
                let zeros: Vec<Option<f32>> = vec![Some(0.0); self.dims];
                let embedding_array = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(zeros)],
                    self.dims as i32,
                );
                let batch = RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![
                        Arc::new(StringArray::from(vec!["__sentinel__"])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(embedding_array) as ArrayRef,
                    ],
                )
                .map_err(|e| RadarError::Other(format!("sentinel batch failed: {e}")))?;
                let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));
                let table = self
                    .db
                    .create_table(CONCEPTS_TABLE, Box::new(batches))
                    .execute()
                    .await
                    .map_err(|e| RadarError::Other(format!("create concepts table failed: {e}")))?;
                table
                    .delete("concept_id = '__sentinel__'")
                    .await
                    .map_err(|e| RadarError::Other(format!("delete sentinel failed: {e}")))?;
                Ok(table)
            }
        }
    }

    /// Insert an item with its embedding vector.
    pub async fn upsert_item(
        &self,
        canonical_id: &str,
        title: &str,
        abstract_text: Option<&str>,
        source_type: &str,
        published_at: Option<&str>,
        embedding: &[f32],
    ) -> Result<()> {
        let table = self.items_table().await?;

        let schema = items_schema(self.dims);

        let embedding_array = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vec![Some(
                embedding.iter().copied().map(Some).collect::<Vec<_>>(),
            )],
            self.dims as i32,
        );

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![canonical_id])) as ArrayRef,
                Arc::new(StringArray::from(vec![title])) as ArrayRef,
                Arc::new(StringArray::from(vec![abstract_text.unwrap_or("")])) as ArrayRef,
                Arc::new(StringArray::from(vec![source_type])) as ArrayRef,
                Arc::new(StringArray::from(vec![published_at.unwrap_or("")])) as ArrayRef,
                Arc::new(embedding_array) as ArrayRef,
            ],
        )
        .map_err(|e| RadarError::Other(format!("batch creation failed: {e}")))?;

        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(items_schema(self.dims)));
        table
            .add(Box::new(batches))
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("item insert failed: {e}")))?;
        Ok(())
    }

    /// Embed and insert a source candidate.
    pub async fn ingest_candidate<E: EmbeddingBackend>(
        &self,
        candidate: &SourceCandidate,
        embedder: &E,
    ) -> Result<()> {
        let text = embedding_text(&candidate.title, candidate.abstract_text.as_deref());
        let embedding = embedder.embed(&text).await?;
        let published = candidate.published_at.map(|dt| dt.to_rfc3339());
        self.upsert_item(
            &candidate.canonical_id,
            &candidate.title,
            candidate.abstract_text.as_deref(),
            candidate.source_type.as_str(),
            published.as_deref(),
            &embedding,
        )
        .await
    }

    /// Semantic search: find items similar to a query string.
    pub async fn search<E: EmbeddingBackend>(
        &self,
        query: &str,
        embedder: &E,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let query_vec = embedder.embed(query).await?;
        self.search_by_vector(&query_vec, limit).await
    }

    /// Search by raw embedding vector.
    pub async fn search_by_vector(
        &self,
        query_vec: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let table = self.items_table().await?;

        let results = table
            .vector_search(query_vec.to_vec())
            .map_err(|e| RadarError::Other(format!("vector search setup failed: {e}")))?
            .limit(limit)
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("vector search failed: {e}")))?;

        let mut items = Vec::new();
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| RadarError::Other(format!("collect search results failed: {e}")))?;

        for batch in &batches {
            let ids = batch
                .column_by_name("canonical_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let titles = batch
                .column_by_name("title")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let abstracts = batch
                .column_by_name("abstract_text")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

            if let (Some(ids), Some(titles)) = (ids, titles) {
                for i in 0..batch.num_rows() {
                    items.push(VectorSearchResult {
                        canonical_id: ids.value(i).to_string(),
                        title: titles.value(i).to_string(),
                        abstract_text: abstracts.map(|a| a.value(i).to_string()),
                        distance: distances.map(|d| d.value(i)).unwrap_or(0.0),
                    });
                }
            }
        }

        Ok(items)
    }

    /// Find items similar to a given item by canonical_id.
    pub async fn find_similar<E: EmbeddingBackend>(
        &self,
        canonical_id: &str,
        embedder: &E,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        // We need to look up the item's embedding, but for simplicity we re-embed
        // from the stored text. In production, we'd fetch the vector directly.
        let table = self.items_table().await?;

        // Search for the item to get its text
        let results = table
            .query()
            .only_if(format!(
                "canonical_id = '{}'",
                canonical_id.replace('\'', "''")
            ))
            .limit(1)
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("item lookup failed: {e}")))?;

        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| RadarError::Other(format!("collect lookup failed: {e}")))?;

        let (title, abstract_text) = batches
            .first()
            .and_then(|b| {
                let titles = b
                    .column_by_name("title")?
                    .as_any()
                    .downcast_ref::<StringArray>()?;
                let abstracts = b
                    .column_by_name("abstract_text")?
                    .as_any()
                    .downcast_ref::<StringArray>()?;
                if b.num_rows() > 0 {
                    Some((titles.value(0).to_string(), abstracts.value(0).to_string()))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                RadarError::NotFound(format!("item {canonical_id} not in vector store"))
            })?;

        let text = embedding_text(&title, Some(&abstract_text));
        let query_vec = embedder.embed(&text).await?;
        // Get limit+1 to exclude self
        let mut results = self.search_by_vector(&query_vec, limit + 1).await?;
        results.retain(|r| r.canonical_id != canonical_id);
        results.truncate(limit);
        Ok(results)
    }

    /// Insert a concept with its embedding.
    pub async fn upsert_concept(
        &self,
        concept_id: &str,
        label: &str,
        item_ids_json: &str,
        embedding: &[f32],
    ) -> Result<()> {
        let table = self.concepts_table().await?;

        let schema = concepts_schema(self.dims);

        let embedding_array = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vec![Some(
                embedding.iter().copied().map(Some).collect::<Vec<_>>(),
            )],
            self.dims as i32,
        );

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![concept_id])) as ArrayRef,
                Arc::new(StringArray::from(vec![label])) as ArrayRef,
                Arc::new(StringArray::from(vec![item_ids_json])) as ArrayRef,
                Arc::new(embedding_array) as ArrayRef,
            ],
        )
        .map_err(|e| RadarError::Other(format!("concept batch creation failed: {e}")))?;

        let batches =
            RecordBatchIterator::new(vec![Ok(batch)], Arc::new(concepts_schema(self.dims)));
        table
            .add(Box::new(batches))
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("concept insert failed: {e}")))?;
        Ok(())
    }

    /// List all concepts.
    pub async fn list_concepts(&self) -> Result<Vec<ConceptRecord>> {
        let table = self.concepts_table().await?;

        let results = table
            .query()
            .execute()
            .await
            .map_err(|e| RadarError::Other(format!("concept query failed: {e}")))?;

        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| RadarError::Other(format!("collect concepts failed: {e}")))?;

        let mut concepts = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("concept_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let labels = batch
                .column_by_name("label")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let items = batch
                .column_by_name("item_ids")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            if let (Some(ids), Some(labels), Some(items)) = (ids, labels, items) {
                for i in 0..batch.num_rows() {
                    concepts.push(ConceptRecord {
                        concept_id: ids.value(i).to_string(),
                        label: labels.value(i).to_string(),
                        item_ids_json: items.value(i).to_string(),
                    });
                }
            }
        }

        Ok(concepts)
    }

    /// Get item count in the vector store.
    pub async fn item_count(&self) -> Result<usize> {
        let table = self.items_table().await?;

        let count = table
            .count_rows(None)
            .await
            .map_err(|e| RadarError::Other(format!("count items failed: {e}")))?;

        Ok(count)
    }
}

/// Result from a vector similarity search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub canonical_id: String,
    pub title: String,
    pub abstract_text: Option<String>,
    pub distance: f32,
}

/// A concept record from the concepts table.
#[derive(Debug, Clone)]
pub struct ConceptRecord {
    pub concept_id: String,
    pub label: String,
    pub item_ids_json: String,
}

fn items_schema(dims: usize) -> Schema {
    Schema::new(vec![
        Field::new("canonical_id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("abstract_text", DataType::Utf8, true),
        Field::new("source_type", DataType::Utf8, false),
        Field::new("published_at", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dims as i32,
            ),
            false,
        ),
    ])
}

fn concepts_schema(dims: usize) -> Schema {
    Schema::new(vec![
        Field::new("concept_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("item_ids", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dims as i32,
            ),
            false,
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::MockEmbeddingBackend;
    use crate::types::SourceType;

    #[tokio::test]
    async fn test_vector_store_lifecycle() {
        let store = VectorStore::open_temp(64).await.unwrap();
        assert_eq!(store.item_count().await.unwrap(), 0);

        let embedder = MockEmbeddingBackend::new(64);

        let candidate = SourceCandidate {
            canonical_id: "arxiv:2401.00001".into(),
            title: "Attention Is All You Need".into(),
            authors: Some("Vaswani et al.".into()),
            abstract_text: Some("The dominant sequence transduction models...".into()),
            url: "https://arxiv.org/abs/2401.00001".into(),
            published_at: None,
            source_type: SourceType::Arxiv,
            aliases: vec![],
            raw_json: None,
        };

        store.ingest_candidate(&candidate, &embedder).await.unwrap();
        assert_eq!(store.item_count().await.unwrap(), 1);

        // Search
        let results = store
            .search("attention mechanism", &embedder, 5)
            .await
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].canonical_id, "arxiv:2401.00001");
    }
}
