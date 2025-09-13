use anyhow::{Context, Result};
use minijinja::Environment;
use std::collections::HashMap;

use crate::interpolate::{compile_template, eval_expression, get_col};
use crate::value::ObjectValue;

use super::types::{RefPlan, ProjectionLabel};
use super::helpers::find_table_field_by_name;

/// Take the DB row and write computed scalars into the parent object.
pub fn hydrate_parent(
    mut parent: ObjectValue,
    env: &Environment<'_>,
    plans: Vec<RefPlan<'_>>,
    composite: ObjectValue,
    labels: Vec<ProjectionLabel>,
) -> Result<ObjectValue> {
    // alias_base -> {col -> value}
    let mut buckets: HashMap<String, ObjectValue> = HashMap::new();

    for ProjectionLabel { full_key, col, .. } in labels {
        if let Some((prefix, _rest)) = full_key.rsplit_once("__") {
            if let Some(v) = composite.get(&full_key).cloned() {
                buckets
                    .entry(prefix.to_string())
                    .or_default()
                    .insert(&col, v);
            }
        }
    }

    for plan in plans {
        let mut nested = buckets
            .remove(&plan.sql.alias_base)
            .unwrap_or_default();

        // original parent in template scope
        nested.insert("parent", parent.clone().into());

        // referenced TableField in the target entity
        let tf = find_table_field_by_name(plan.target.model, plan.target.field_name)
            .ok_or_else(|| anyhow::anyhow!(
                "`{}` is not a TableField in `{}`",
                plan.target.field_name, plan.target.entity_name
            ))?;

        let dyn_val = match tf.template {
            Some(tpl) => {
                let expr = compile_template(env, tpl)
                    .with_context(|| format!("cannot compile template `{}`", tpl))?;
                eval_expression(&nested, &expr).with_context(|| {
                    format!(
                        "evaluating template for `{}.{}`",
                        plan.target.entity_name, plan.target.field_name
                    )
                })?
            }
            None => get_col(&nested, plan.target.field_name).with_context(|| {
                format!(
                    "reading `{}` from nested `{}`",
                    plan.target.field_name, plan.target.entity_name
                )
            })?,
        };

        parent.insert(plan.source.field_name, dyn_val);
    }

    Ok(parent)
}
