use anyhow::{anyhow, Context, Result};
use minijinja::Environment;
use sqlx::MySqlPool;

use crate::entity::Entity;
use crate::interpolate::FromObject;
use crate::value::ObjectValue;

use super::planner::plan_refs;
use super::sql_builder::build_composite_query;
use super::hydrate::hydrate_parent;
// NEW: we’ll reuse these to show more detail
use super::helpers::primary_key_value;
use super::types::RefPlan;

/// Public entry: resolve refs (one query) -> enrich -> build T
pub async fn resolve_and_build<T>(
    base: &ObjectValue,
    env: &Environment<'_>,
    pool: &MySqlPool,
) -> Result<T>
where
    T: Entity + FromObject,
{
    let enriched = resolve_refs_one_shot_nested::<T>(base, env, pool).await?;
    T::from_object(&enriched, env)
}

/// High-level one-shot: plan -> build query -> execute -> hydrate
pub async fn resolve_refs_one_shot_nested<T>(
    src: &ObjectValue,
    env: &Environment<'_>,
    pool: &MySqlPool,
) -> Result<ObjectValue>
where
    T: Entity,
{
    let model = T::MODEL;

    let plans = plan_refs(model, env)
        .with_context(|| format!("planning references for `{}`", model.rust_name))?;

    if plans.is_empty() {
        return Ok(src.clone());
    }

    let (query, labels) = build_composite_query(model, src, &plans)
        .with_context(|| "building composite SQL for references")?;

    // --- Better error: include PK, SQL, and plan summaries ---
    let (pk_col, pk_val) = primary_key_value(model, src)
        .with_context(|| format!("determining primary key from `{}`", model.rust_name))?;
    let sql = query.to_string();
    let plan_summ = summarize_plans(&plans);

    let composite = query
        .fetch_one(pool)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "no row returned while resolving references\n\
                 entity: `{}` (table: `{}`)\n\
                 pk: {} = {}\n\
                 sql:\n  {}\n\
                 plans ({}):\n{}",
                model.rust_name,
                model.table_name,
                pk_col,
                format_dynval(&pk_val),
                sql,
                plans.len(),
                indent_lines(&plan_summ, 2)
            )
        })?;

    hydrate_parent(src.clone(), env, plans, composite, labels)
}

// ---- small helpers for richer error text ----

fn summarize_plans(plans: &[RefPlan<'_>]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for p in plans {
        let via = if p.sql.via.is_empty() {
            "direct FK".to_string()
        } else {
            p.sql
                .via
                .iter()
                .map(|h| format!("{}({}={})", h.table, h.lhs, h.rhs))
                .collect::<Vec<_>>()
                .join(" -> ")
        };
        // show how we join and what we finally select from
        let _ = writeln!(
            out,
            "- {} -> {}.{}  [alias_base: {}, final_alias: {}, via: {}]",
            p.source.field_name,
            p.target.entity_name,
            p.target.field_name,
            p.sql.alias_base,
            p.sql.final_alias,
            via
        );
    }
    out
}

fn indent_lines(s: &str, spaces: usize) -> String {
    let pad = " ".repeat(spaces);
    s.lines()
        .map(|l| format!("{pad}{l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_dynval(v: &crate::value::DynamicValue) -> String {
    // prefer Display if implemented; otherwise Debug
    // If your DynamicValue has a nicer formatter, use it.
    format!("{v:?}")
}
