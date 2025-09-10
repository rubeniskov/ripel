//! Configuration for MySQL CDC

use serde::{Deserialize, Serialize};

/// Table-specific CDC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// Table name
    pub name: String,
    
    /// Include only these columns (empty = all columns)
    pub include_columns: Vec<String>,
    
    /// Exclude these columns
    pub exclude_columns: Vec<String>,
    
    /// Custom event type override
    pub event_type_override: Option<String>,
    
    /// Whether to capture before state for updates/deletes
    pub capture_before: bool,
}

impl TableConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            include_columns: Vec::new(),
            exclude_columns: Vec::new(),
            event_type_override: None,
            capture_before: true,
        }
    }

    pub fn include_column(mut self, column: impl Into<String>) -> Self {
        self.include_columns.push(column.into());
        self
    }

    pub fn exclude_column(mut self, column: impl Into<String>) -> Self {
        self.exclude_columns.push(column.into());
        self
    }

    pub fn with_event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_type_override = Some(event_type.into());
        self
    }

    pub fn without_before_capture(mut self) -> Self {
        self.capture_before = false;
        self
    }
}

/// CDC filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    /// Database patterns to include
    pub include_databases: Vec<String>,
    
    /// Database patterns to exclude
    pub exclude_databases: Vec<String>,
    
    /// Table patterns to include
    pub include_tables: Vec<String>,
    
    /// Table patterns to exclude
    pub exclude_tables: Vec<String>,
    
    /// Operations to capture
    pub operations: Vec<String>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            include_databases: Vec::new(),
            exclude_databases: vec![
                "information_schema".to_string(),
                "performance_schema".to_string(),
                "mysql".to_string(),
                "sys".to_string(),
            ],
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            operations: vec![
                "insert".to_string(),
                "update".to_string(), 
                "delete".to_string(),
            ],
        }
    }
}

impl FilterConfig {
    /// Check if a database should be included
    pub fn should_include_database(&self, database: &str) -> bool {
        // Check exclude patterns first
        for pattern in &self.exclude_databases {
            if database == pattern || self.matches_pattern(database, pattern) {
                return false;
            }
        }

        // If include patterns are specified, check them
        if !self.include_databases.is_empty() {
            for pattern in &self.include_databases {
                if database == pattern || self.matches_pattern(database, pattern) {
                    return true;
                }
            }
            return false;
        }

        true
    }

    /// Check if a table should be included
    pub fn should_include_table(&self, table: &str) -> bool {
        // Check exclude patterns first
        for pattern in &self.exclude_tables {
            if table == pattern || self.matches_pattern(table, pattern) {
                return false;
            }
        }

        // If include patterns are specified, check them
        if !self.include_tables.is_empty() {
            for pattern in &self.include_tables {
                if table == pattern || self.matches_pattern(table, pattern) {
                    return true;
                }
            }
            return false;
        }

        true
    }

    /// Check if an operation should be captured
    pub fn should_capture_operation(&self, operation: &str) -> bool {
        self.operations.contains(&operation.to_lowercase())
    }

    /// Simple pattern matching (supports * wildcard)
    fn matches_pattern(&self, text: &str, pattern: &str) -> bool {
        if pattern.contains('*') {
            // Simple wildcard matching
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                let (prefix, suffix) = (parts[0], parts[1]);
                return text.starts_with(prefix) && text.ends_with(suffix);
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_config_builder() {
        let config = TableConfig::new("users")
            .include_column("id")
            .include_column("email")
            .exclude_column("password")
            .with_event_type("user.changed")
            .without_before_capture();

        assert_eq!(config.name, "users");
        assert_eq!(config.include_columns, vec!["id", "email"]);
        assert_eq!(config.exclude_columns, vec!["password"]);
        assert_eq!(config.event_type_override, Some("user.changed".to_string()));
        assert!(!config.capture_before);
    }

    #[test]
    fn test_filter_config() {
        let filter = FilterConfig::default();
        
        assert!(!filter.should_include_database("information_schema"));
        assert!(!filter.should_include_database("mysql"));
        assert!(filter.should_include_database("myapp"));
        
        assert!(filter.should_capture_operation("insert"));
        assert!(filter.should_capture_operation("UPDATE"));
        assert!(!filter.should_capture_operation("ddl"));
    }

    #[test]
    fn test_pattern_matching() {
        let mut filter = FilterConfig::default();
        filter.exclude_tables.push("temp_*".to_string());
        
        assert!(!filter.should_include_table("temp_data"));
        assert!(!filter.should_include_table("temp_logs"));
        assert!(filter.should_include_table("users"));
    }
}