use anyhow::{anyhow, Context, Result};
use minijinja::Environment;

use crate::entity::EntityModel;
use crate::registry::get_entity_by_name;

use super::types::{RefPlan, SourceField, TargetEntity, SqlPlan};
use super::helpers::{
    iter_ref_fields, parse_reference, variable_fields_for,
    final_alias_for_chain, find_table_field_by_name,
};

pub fn plan_refs<'a>(model: &'a EntityModel, env: &Environment<'_>) -> Result<Vec<RefPlan<'a>>> {
    let mut plans = Vec::new();

    for rf in iter_ref_fields(model) {
        let (ref_entity_name, ref_field_name) = parse_reference(rf.reference)
            .with_context(|| format!("invalid reference `{}`", rf.reference))?;

        let ref_model = get_entity_by_name(ref_entity_name)
            .with_context(|| format!("unknown referenced entity `{ref_entity_name}`"))?;

        let projected_cols = variable_fields_for(ref_model, env);

        // THIS entity FK column (same-named TableField)
        let this_tf = find_table_field_by_name(model, rf.name)
            .ok_or_else(|| anyhow!("no TableField named `{}` in `{}`", rf.name, model.rust_name))?;

        // TARGET entity join column (the referenced field's DB column)
        let ref_tf = find_table_field_by_name(ref_model, ref_field_name)
            .ok_or_else(|| anyhow!("`{}` is not a TableField in `{}`", ref_field_name, ref_entity_name))?;

        let alias_base  = format!("via_{}", rf.name);
        let final_alias = if rf.via.is_empty() {
            format!("{}_tgt", alias_base)     // stable alias for direct join
        } else {
            final_alias_for_chain(&alias_base, rf.via)
        };

        plans.push(RefPlan {
            source: SourceField {
                field_name:  rf.name,          // for enriched object key
                column_name: this_tf.column,   // **DB column** (e.g., "Cliente_id")
            },
            target: TargetEntity {
                entity_name:   ref_entity_name,
                field_name:    ref_field_name, // rust field
                field_column:  ref_tf.column,  // **DB column** (e.g., "id")
                model:         ref_model,
                projected_cols,
            },
            sql: SqlPlan {
                via: rf.via,
                alias_base,
                final_alias,
            },
        });
    }

    Ok(plans)
}
