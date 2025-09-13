use anyhow::{bail, Context, Result};
use minijinja::value::{from_args, Kwargs, Object};
use minijinja::value::ValueKind;
use minijinja::Value;
use sqlx::FromRow;
use sqlx::{mysql::MySqlRow, MySql, MySqlPool};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use super::selector::Selector;
use crate::sql::OnClause;
use crate::ObjectValue;
use super::helpers::validate_ident;


pub trait AsQuery {
    fn as_query(&self) -> anyhow::Result<&Query>;
}

pub trait QueryExt: Sized {
    type Error;
    fn join<I, S>(&self, other: &str, on: I, alias: &str) -> Result<Self>
    where 
        I: IntoIterator<Item = S>, S: TryInto<OnClause, Error = Self::Error>;
    fn select<I, S>(&self, cols: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>, S: TryInto<Selector, Error = Self::Error>;
    fn order_by(&self, col: String, asc: bool) -> Self;
    fn filter(&self, kwargs: Kwargs) -> Self;
    fn limit(&self, count: usize) -> Self;
    fn offset(&self, count: usize) -> Self;
    fn table_name(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct Join {
    pub table: Arc<str>,
    pub on: Vec<OnClause>,
    pub alias: Arc<str>,
}

/// A copy-on-write object that holds an assembled query.
#[derive(Debug, Clone)]
pub struct Query {
    table: Arc<str>,
    filters: Arc<HashMap<String, Value>>,
    limit: Option<usize>,
    offset: Option<usize>,
    select: Arc<Vec<Arc<Selector>>>,
    order_by: Option<(Arc<str>, bool)>, // (column, asc)
    joins: Vec<Join>,
}

impl Object for Query {
    /// Implements a method dispatch for the query so it can be further reduced.
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State,
        name: &str,
        args: &[Value],
    ) -> Result<Value, minijinja::Error> {
        match name {
            "filter" => {
                let (kwargs,) = from_args(args)?;
                Ok(Value::from_object(self.filter(kwargs)))
            }
            "limit" => {
                let (limit,) = from_args(args)?;
                Ok(Value::from_object(self.limit(limit)))
            }
            "offset" => {
                let (offset,) = from_args(args)?;
                Ok(Value::from_object(self.offset(offset)))
            }
            "select" => {
                let (cols,): (Vec<String>,) = from_args(args)?;
                Ok(Value::from_object(self.select(cols)
                    .map_err(|e| e.downcast::<minijinja::Error>().unwrap())
                ?))
            }
            "order_by" => {
                let (col, asc): (String, bool) = from_args(args)?;
                Ok(Value::from_object(self.order_by(col, asc)))
            }
            _ => Err(minijinja::Error::from(minijinja::ErrorKind::UnknownMethod)),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<Query table={:?}>", self.table)
    }
}

impl Query {
    /// Creates an empty query object for a specific table.
    pub fn new(table: &str) -> Self {
        Query {
            table: Arc::from(table),
            filters: Default::default(),
            limit: None,
            offset: None,
            select: Default::default(),
            order_by: None,
            joins: Vec::new(),
        }
    }
}

impl QueryExt for Query {
    type Error = anyhow::Error;

    fn table_name(&self) -> &str {
        &self.table
    }
    /// Filters the query down by the given keyword arguments.
    fn filter(&self, kwargs: Kwargs) -> Self {
        let mut rv = self.clone();
        let filters_mut = Arc::make_mut(&mut rv.filters);
        for arg in kwargs.args() {
            filters_mut.insert(arg.to_string(), kwargs.get::<Value>(arg).unwrap());
        }
        rv
    }

    /// Limits the query to `count` rows.
    fn limit(&self, count: usize) -> Self {
        let mut rv = self.clone();
        rv.limit = Some(count);
        rv
    }

    /// Offsets the query by `count` rows.
    fn offset(&self, count: usize) -> Self {
        let mut rv = self.clone();
        rv.offset = Some(count);
        rv
    }

    fn select<I, S>(&self, cols: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: TryInto<Selector, Error = Self::Error>,
    {
        let mut rv = self.clone();
        let v: Vec<Arc<Selector>> = cols
        .into_iter()
        .map(|s| s.try_into().map(Arc::new))
        .collect::<Result<_, _>>()?;
        
        rv.select = Arc::new(v);
        Ok(rv)
    }

    fn order_by(&self, col: String, asc: bool) -> Self {
        let mut rv = self.clone();
        rv.order_by = Some((Arc::from(col), asc));
        rv
    }

    fn join<I, S>(&self, other: &str, on: I, alias: &str) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: TryInto<OnClause, Error = Self::Error>,
    {
        let mut rv = self.clone();
        let on = on.into_iter()
        .map(|s| s.try_into()).collect::<Result<Vec<_>, _>>()?;
    
        rv.joins.push(Join {
            table: Arc::from(other),
            on,
            alias: Arc::from(alias),
        });
        Ok(rv)
    }

    
}


impl Query {
    fn to_sql(&self) -> Result<(String, Vec<Value>)> {
        let table = &*self.table;
        // base table must be a single identifier; but our validator already allows dotted.
        // If you want to restrict base table to a single name, call a stricter validator here.
        validate_ident(table)?;

        // SELECT
        let select = if self.select.is_empty() {
            Selector::new("*")
            .set_source("self").to_string() // avoid ambiguity when joins exist
        } else {
            self.select
                .iter()
                .map(|c| c.to_sql())
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        };

        let mut sql = format!("SELECT {select} FROM `{table}` AS self");
        let mut binds = Vec::<Value>::new();

        // JOINs (must come before WHERE)
        for j in &self.joins {
            let tbl = &*j.table;
            validate_ident(tbl)?;
            let alias = j.alias.as_ref();
            if !alias.is_empty() { validate_ident(alias)?; }

            if alias.is_empty() {
                sql.push_str(&format!(
                    " INNER JOIN `{tbl}` ON {}",
                    j.on.iter().map(|clause| clause.to_string()).collect::<Vec<_>>().join(" AND ")
                ));
            } else {
                sql.push_str(&format!(
                    " INNER JOIN `{tbl}` AS `{alias}` ON {}",
                    j.on.iter().map(|clause| clause.to_string()).collect::<Vec<_>>().join(" AND ")
                ));
            }
        }

        // WHERE (stable order)
        let mut keys: Vec<_> = self.filters.keys().cloned().collect();
        keys.sort_unstable();
        if !keys.is_empty() {
            sql.push_str(" WHERE ");
            for (i, k) in keys.iter().enumerate() {
                if i > 0 { sql.push_str(" AND "); }
                sql.push_str(&quote_ident_path(k)?);
                sql.push_str(" = ?");
                binds.push(self.filters[k].clone());
            }
        }

        // ORDER BY
        if let Some((col, asc)) = &self.order_by {
            sql.push_str(&format!(
                " ORDER BY {} {}",
                quote_ident_path(col)?,
                if *asc { "ASC" } else { "DESC" }
            ));
        }

        // LIMIT/OFFSET
        if let Some(l) = self.limit {
            sql.push_str(" LIMIT ?");
            binds.push(Value::from(l as i64));
        }
        if let Some(o) = self.offset {
            sql.push_str(" OFFSET ?");
            binds.push(Value::from(o as i64));
        }

        Ok((sql, binds))
    }
    /// Execute and return rows as `Vec<HashMap<String, sqlx::types::JsonValue>>`
    /// You can map to a typed struct if you prefer.
    pub async fn fetch_all(
        &self,
        pool: &MySqlPool,
    ) -> Result<Vec<ObjectValue>> {
        let (sql, binds) = self.to_sql()?;

        // Build the query with binds converted from MiniJinja `Value`
        let mut q = sqlx::query(&sql);
        for v in binds {
            q = bind_value(q, v)?;
        }

        let rows: Vec<MySqlRow> = q
            .fetch_all(pool)
            .await
            .with_context(|| format!("query failed: {sql}"))?;

        // Convert rows to map-of-column->serde_json::Value
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let row_values: ObjectValue = ObjectValue::from_row(&row)
                    .with_context(|| "fetch all rows".to_string())?;
            
            out.push(row_values);
        }
        Ok(out)
    }

    pub async fn fetch_one(
        &self,
        pool: &MySqlPool,
    ) -> Result<Option<ObjectValue>> {
        let (sql, binds) = self.to_sql()?;

        // Build the query with binds converted from MiniJinja `Value`
        let mut q = sqlx::query(&sql);
        for v in binds {
            q = bind_value(q, v)?;
        }

        let row: Option<MySqlRow> = q
            .fetch_optional(pool)
            .await
            .with_context(|| format!("query failed: {sql}"))?;

        if let Some(row) = row {
            let row_values: ObjectValue = ObjectValue::from_row(&row)
                    .with_context(|| "fetch one row".to_string())?;
            Ok(Some(row_values))
        } else {
            Ok(None)
        }
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_sql() {
            Ok((sql, _)) => write!(f, "Query: {}", sql),
            Err(e) => write!(f, "Query: <error: {}>", e),
        }
    }
}

/// Quote an identifier path into backticked parts: `table`.`col`
fn quote_ident_path(path: &str) -> Result<String> {
    validate_ident(path)?;
    Ok(path
        .split('.')
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join("."))
}

/// Bind a MiniJinja `Value` into a `sqlx::Query`.
fn bind_value<'q>(
    mut q: sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments>,
    v: Value,
) -> Result<sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments>> {
    match v.kind() {
        ValueKind::Undefined | ValueKind::None => {
            // treat as NULL (bind as Option::<i32>::None for lack of type info)
            q = q.bind(Option::<i32>::None);
        }
        ValueKind::Bool => q = q.bind(v.is_true()),
        ValueKind::Number => {
            // prefer i64 if fits, else f64
            if let Some(i) = v.as_i64() {
                q = q.bind(i);
            } else if let Ok(f) = f64::try_from(v.clone()) {
                q = q.bind(f);
            } else {
                bail!("unsupported numeric value")
            }
        }
        ValueKind::String => q = q.bind(v.to_string()),
        ValueKind::Seq | ValueKind::Map | ValueKind::Iterable => {
            // bind as JSON string (or use `sqlx::types::Json` if you have a JSON column)
            q = q.bind(v.to_string());
        }
        _ => bail!("unsupported value kind for bind"),
    }
    Ok(q)
}