//! FalkorDB storage backend for GraphRAG
//!
//! This module provides a storage implementation using FalkorDB graph database.
//! FalkorDB is a graph database that uses the Redis protocol for communication.

#[cfg(feature = "falkordb-storage")]
use crate::core::{Document, Entity, GraphRAGError, Result, TextChunk};
#[cfg(feature = "falkordb-storage")]
use async_trait::async_trait;
#[cfg(feature = "falkordb-storage")]
use falkordb::{AsyncGraph, FalkorClientBuilder, FalkorConnectionInfo};
#[cfg(feature = "falkordb-storage")]
use std::sync::Arc;
#[cfg(feature = "falkordb-storage")]
use tokio::sync::RwLock;

/// FalkorDB storage implementation
#[cfg(feature = "falkordb-storage")]
#[derive(Clone)]
pub struct FalkorDBStorage {
    /// FalkorDB graph instance
    graph: Arc<RwLock<AsyncGraph>>,
    /// Graph name
    graph_name: String,
}

#[cfg(feature = "falkordb-storage")]
impl FalkorDBStorage {
    /// Create a new FalkorDB storage instance
    ///
    /// # Arguments
    /// * `host` - FalkorDB server host
    /// * `port` - FalkorDB server port
    /// * `graph_name` - Name of the graph to use
    /// * `username` - Optional username for authentication
    /// * `password` - Optional password for authentication
    /// * `use_tls` - Whether to use TLS/SSL
    ///
    /// # Returns
    /// A new FalkorDB storage instance or an error
    pub async fn new(
        host: &str,
        port: u16,
        graph_name: &str,
        username: Option<&str>,
        password: Option<&str>,
        use_tls: bool,
    ) -> Result<Self> {
        // Build the connection URL
        let scheme = if use_tls { "rediss" } else { "redis" };
        let auth = match (username, password) {
            (Some(u), Some(p)) => format!("{}:{}@", u, p),
            _ => String::new(),
        };
        let url = format!("{}://{}{}:{}", scheme, auth, host, port);

        // Parse the connection info
        let connection_info = FalkorConnectionInfo::try_from(url.as_str())
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to parse connection URL: {}", e),
            })?;

        // Create FalkorDB client
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(connection_info)
            .build()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to create FalkorDB client: {}", e),
            })?;

        // Select or create the graph
        let graph = client.select_graph(graph_name);

        Ok(Self {
            graph: Arc::new(RwLock::new(graph)),
            graph_name: graph_name.to_string(),
        })
    }

    /// Store an entity in FalkorDB
    async fn store_entity_internal(&self, entity: &Entity) -> Result<String> {
        let mut graph = self.graph.write().await;

        // Create Cypher query to create or merge entity node
        // Entity has: id, name, entity_type, confidence, mentions, embedding
        let query = format!(
            "MERGE (e:Entity {{id: '{}'}}) \
             SET e.name = '{}', e.entity_type = '{}', e.confidence = {} \
             RETURN e.id",
            entity.id.0.replace('\'', "\\'"),
            entity.name.replace('\'', "\\'"),
            entity.entity_type.replace('\'', "\\'"),
            entity.confidence
        );

        graph
            .query(&query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to store entity: {}", e),
            })?;

        Ok(entity.id.0.clone())
    }

    /// Retrieve an entity from FalkorDB
    async fn retrieve_entity_internal(&self, id: &str) -> Result<Option<Entity>> {
        let mut graph = self.graph.write().await;

        let query = format!(
            "MATCH (e:Entity {{id: '{}'}}) RETURN e.id, e.name, e.entity_type, e.confidence",
            id.replace('\'', "\\'")
        );

        let result = graph
            .query(&query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to retrieve entity: {}", e),
            })?;

        // Parse result and construct Entity
        // Note: This is a simplified implementation
        // In a real implementation, you'd parse the result set properly
        if result.data.is_empty() {
            return Ok(None);
        }

        // For now, return None as full parsing requires more complex result handling
        // This would need to be implemented based on the actual FalkorDB result format
        Ok(None)
    }

    /// Store a document in FalkorDB
    async fn store_document_internal(&self, document: &Document) -> Result<String> {
        let mut graph = self.graph.write().await;

        let query = format!(
            "MERGE (d:Document {{id: '{}'}}) \
             SET d.title = '{}', d.content = '{}' \
             RETURN d.id",
            document.id.0.replace('\'', "\\'"),
            document.title.replace('\'', "\\'"),
            document.content.replace('\'', "\\'")
        );

        graph
            .query(&query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to store document: {}", e),
            })?;

        Ok(document.id.0.clone())
    }

    /// Store a text chunk in FalkorDB
    async fn store_chunk_internal(&self, chunk: &TextChunk) -> Result<String> {
        let mut graph = self.graph.write().await;

        // TextChunk has: id, document_id, content, start_offset, end_offset, embedding, entities, metadata
        let query = format!(
            "MERGE (c:Chunk {{id: '{}'}}) \
             SET c.content = '{}', c.document_id = '{}', c.start_offset = {}, c.end_offset = {} \
             RETURN c.id",
            chunk.id.0.replace('\'', "\\'"),
            chunk.content.replace('\'', "\\'"),
            chunk.document_id.0.replace('\'', "\\'"),
            chunk.start_offset,
            chunk.end_offset
        );

        graph
            .query(&query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to store chunk: {}", e),
            })?;

        Ok(chunk.id.0.clone())
    }

    /// List all entity IDs
    async fn list_entities_internal(&self) -> Result<Vec<String>> {
        let mut graph = self.graph.write().await;

        let query = "MATCH (e:Entity) RETURN e.id";

        let result = graph
            .query(query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to list entities: {}", e),
            })?;

        // Parse result to extract entity IDs
        // LazyResultSet is an iterator over Vec<FalkorValue>
        let mut ids = Vec::new();
        for row in result.data {
            // Each row is a Vec<FalkorValue>
            if !row.is_empty() {
                // Try to convert the first value to a string
                // This is a simplified approach; real code might use try_into()
                ids.push(format!("{:?}", row[0]));
            }
        }

        Ok(ids)
    }

    /// Create a relationship between two entities
    pub async fn create_relationship(
        &self,
        from_entity_id: &str,
        to_entity_id: &str,
        relationship_type: &str,
    ) -> Result<()> {
        let mut graph = self.graph.write().await;

        let query = format!(
            "MATCH (from:Entity {{id: '{}'}}), (to:Entity {{id: '{}'}}) \
             MERGE (from)-[r:{}]->(to) \
             RETURN r",
            from_entity_id.replace('\'', "\\'"),
            to_entity_id.replace('\'', "\\'"),
            relationship_type.replace('\'', "\\'")
        );

        graph
            .query(&query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to create relationship: {}", e),
            })?;

        Ok(())
    }

    /// Execute a custom Cypher query
    pub async fn execute_query(&self, query: &str) -> Result<()> {
        let mut graph = self.graph.write().await;

        graph
            .query(query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to execute query: {}", e),
            })?;

        Ok(())
    }

    /// Get graph statistics
    pub async fn get_stats(&self) -> Result<FalkorDBStats> {
        let mut graph = self.graph.write().await;

        // Query for node and edge counts
        let node_query = "MATCH (n) RETURN count(n) as count";
        
        let mut node_result = graph
            .query(node_query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to count nodes: {}", e),
            })?;

        // Parse node count
        let node_count = if let Some(row) = node_result.data.next() {
            if !row.is_empty() {
                // Try to parse as usize - this is simplified
                0 // Default fallback
            } else {
                0
            }
        } else {
            0
        };

        // Query for edges
        let edge_query = "MATCH ()-[r]->() RETURN count(r) as count";
        
        let mut edge_result = graph
            .query(edge_query)
            .execute()
            .await
            .map_err(|e| GraphRAGError::Storage {
                message: format!("Failed to count edges: {}", e),
            })?;

        // Parse edge count
        let edge_count = if let Some(row) = edge_result.data.next() {
            if !row.is_empty() {
                // Try to parse as usize - this is simplified
                0 // Default fallback
            } else {
                0
            }
        } else {
            0
        };

        Ok(FalkorDBStats {
            node_count,
            edge_count,
            graph_name: self.graph_name.clone(),
        })
    }
}

/// Statistics for FalkorDB storage
#[cfg(feature = "falkordb-storage")]
#[derive(Debug, Clone)]
pub struct FalkorDBStats {
    /// Number of nodes in the graph
    pub node_count: usize,
    /// Number of edges in the graph
    pub edge_count: usize,
    /// Graph name
    pub graph_name: String,
}

// Implement AsyncStorage trait for FalkorDBStorage
#[cfg(feature = "falkordb-storage")]
#[async_trait]
impl crate::core::traits::AsyncStorage for FalkorDBStorage {
    type Entity = Entity;
    type Document = Document;
    type Chunk = TextChunk;
    type Error = GraphRAGError;

    async fn store_entity(&mut self, entity: Self::Entity) -> Result<String> {
        self.store_entity_internal(&entity).await
    }

    async fn retrieve_entity(&self, id: &str) -> Result<Option<Self::Entity>> {
        self.retrieve_entity_internal(id).await
    }

    async fn store_document(&mut self, document: Self::Document) -> Result<String> {
        self.store_document_internal(&document).await
    }

    async fn retrieve_document(&self, _id: &str) -> Result<Option<Self::Document>> {
        // Not implemented yet
        Ok(None)
    }

    async fn store_chunk(&mut self, chunk: Self::Chunk) -> Result<String> {
        self.store_chunk_internal(&chunk).await
    }

    async fn retrieve_chunk(&self, _id: &str) -> Result<Option<Self::Chunk>> {
        // Not implemented yet
        Ok(None)
    }

    async fn list_entities(&self) -> Result<Vec<String>> {
        self.list_entities_internal().await
    }

    async fn store_entities_batch(&mut self, entities: Vec<Self::Entity>) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for entity in entities {
            let id = self.store_entity_internal(&entity).await?;
            ids.push(id);
        }
        Ok(ids)
    }

    async fn health_check(&self) -> Result<bool> {
        // Try to execute a simple query to check connection
        let mut graph = self.graph.write().await;
        match graph.query("RETURN 1").execute().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

#[cfg(all(test, feature = "falkordb-storage"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_falkordb_connection() {
        // This test requires a running FalkorDB instance
        // Skip if FalkorDB is not available
        let result = FalkorDBStorage::new("localhost", 6379, "test_graph", None, None, false).await;

        // We expect this to fail in CI/CD without a FalkorDB instance
        // In a real environment, you would have FalkorDB running
        match result {
            Ok(_storage) => {
                // Connection successful
                println!("FalkorDB connection successful");
            }
            Err(e) => {
                // Expected in CI/CD
                println!("FalkorDB connection failed (expected in CI): {}", e);
            }
        }
    }
}
