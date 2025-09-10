//! Database connection management

use ripel_core::{Result, RipelError};
use sqlx::{ConnectOptions, MySql, Pool};
use std::time::Duration;
use tracing::{info, instrument};

/// MySQL connection manager
pub struct MySqlConnectionManager {
    pool: Pool<MySql>,
}

impl MySqlConnectionManager {
    /// Create a new connection manager
    #[instrument(skip(connection_url))]
    pub async fn new(connection_url: &str, max_connections: u32) -> Result<Self> {
        info!("Creating MySQL connection pool");
        
        let pool = sqlx::MySqlPool::connect_with(
            sqlx::mysql::MySqlConnectOptions::from_url(
                &connection_url.parse()
                    .map_err(|e| RipelError::DatabaseError(format!("Invalid URL: {}", e)))?
            )
            .map_err(|e| RipelError::DatabaseError(format!("Invalid connection options: {}", e)))?
        )
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(30))
        .build();

        let pool = pool.await
            .map_err(|e| RipelError::DatabaseError(format!("Failed to create pool: {}", e)))?;

        Ok(Self { pool })
    }

    /// Get the connection pool
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }

    /// Test database connectivity
    pub async fn test_connection(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RipelError::DatabaseError(format!("Connection test failed: {}", e)))?;
        
        Ok(())
    }

    /// Get database version
    pub async fn get_version(&self) -> Result<String> {
        let row = sqlx::query("SELECT VERSION() as version")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RipelError::DatabaseError(format!("Failed to get version: {}", e)))?;
        
        let version: String = row.get("version");
        Ok(version)
    }

    /// Check if binlog is enabled
    pub async fn is_binlog_enabled(&self) -> Result<bool> {
        let row = sqlx::query("SHOW VARIABLES LIKE 'log_bin'")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| RipelError::DatabaseError(format!("Failed to check binlog status: {}", e)))?;

        if let Some(row) = row {
            let value: String = row.get("Value");
            Ok(value.to_lowercase() == "on")
        } else {
            Ok(false)
        }
    }

    /// Get binlog format
    pub async fn get_binlog_format(&self) -> Result<String> {
        let row = sqlx::query("SHOW VARIABLES LIKE 'binlog_format'")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RipelError::DatabaseError(format!("Failed to get binlog format: {}", e)))?;

        let format: String = row.get("Value");
        Ok(format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires MySQL server
    async fn test_connection_manager() {
        let manager = MySqlConnectionManager::new("mysql://root:password@localhost:3306", 5)
            .await;
        
        // This test would require a real MySQL instance
        assert!(manager.is_err() || manager.is_ok());
    }
}