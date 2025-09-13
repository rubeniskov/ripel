use crate::entity::EntityModel;
use crate::refs::Hop;

/// Where the computed scalar lands in the enriched parent.
#[derive(Debug)]
pub struct SourceField<'a> {
    /// Key used in the enriched object (Rust field name)
    pub field_name: &'static str,
    /// Actual DB column in THIS table used to join (e.g. "Cliente_id")
    pub column_name: &'a str,
}

/// What we dereference and which columns we must fetch.
#[derive(Debug)]
pub struct TargetEntity<'a> {
    pub entity_name:   &'a str,
    /// Rust field name inside the target entity (e.g., "id")
    pub field_name:    &'a str,
    /// Actual DB column in the TARGET table for that field (e.g., "id", or another)
    pub field_column:  &'a str,
    pub model:         &'a EntityModel,
    /// Columns we’ll project for templates (variable names; selection uses their DB columns)
    pub projected_cols: Vec<String>,
}

/// How to reach it in SQL.
#[derive(Debug)]
pub struct SqlPlan<'a> {
    pub via: &'a [Hop<'a>],
    pub alias_base: String,
    pub final_alias: String,
}

/// One planned reference resolution.
#[derive(Debug)]
pub struct RefPlan<'a> {
    pub source: SourceField<'a>,
    pub target: TargetEntity<'a>,
    pub sql:    SqlPlan<'a>,
}

#[derive(Debug, Clone)]
pub struct ProjectionLabel {
    /// e.g. "via_client_id__created_at"
    pub full_key: String,
    /// template variable name (usually Rust field name)
    pub col: String,
}
