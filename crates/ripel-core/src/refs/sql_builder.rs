use anyhow::Result;

use crate::entity::EntityModel;
use crate::sql::{Query, QueryExt};

use super::types::{RefPlan, ProjectionLabel};
use super::helpers::{
    last_ident, split_rhs, hop_alias, label, primary_key_value, find_table_field_by_name,
};

/// Build single composite query and return labels for hydration.
pub fn build_composite_query<'a>(
    model: &'a EntityModel,
    src: &crate::value::ObjectValue,
    plans: &[RefPlan<'a>],
) -> Result<(Query, Vec<ProjectionLabel>)> {
    let mut labels = Vec::<ProjectionLabel>::new();

    let mut q = Query::new(model.table_name);
    let mut select = vec![String::from("self.*")];

    let (pk_col, pk_val) = primary_key_value(model, src)?;

    for plan in plans {
        if plan.sql.via.is_empty() {
            // ----- Direct FK join: self.<FK COLUMN> = target.<TARGET COLUMN> -----
            let alias = plan.sql.final_alias.clone();
            let on = format!("self.{fk} = {alias}.{rhs}",
                fk   = plan.source.column_name,    // e.g., "Cliente_id"
                alias = alias,
                rhs  = plan.target.field_column);  // e.g., "id"

            q = q.join(
                plan.target.model.table_name,
                [on, format!("self.{pk} = {val}", pk = pk_col, val = pk_val)],
                &alias,
            )?;
        } else {
            // ----- Multi-hop via chain (unchanged) -----
            let mut prev_alias = "self".to_string();

            for (step, hop) in plan.sql.via.iter().enumerate() {
                let (rhs_path, rhs_alias_opt) = split_rhs(hop.rhs);
                let rhs_col    = last_ident(rhs_path); // provided in Hop.rhs
                let this_alias = rhs_alias_opt
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| hop_alias(&plan.sql.alias_base, step));

                let on = format!("{prev}.{rhs} = {alias}.{lhs}",
                    prev = &prev_alias, rhs = rhs_col, alias = this_alias, lhs = hop.lhs);
                    
                let pk_val = if pk_val.is_number() {
                    pk_val.to_string()
                } else {
                    format!("'{pk_val}'", pk_val = pk_val)
                };
                q = q.join(
                    hop.table,
                    [on, format!("self.{pk} = {val}", pk = pk_col, val = pk_val)],
                    &this_alias,
                )?;

                prev_alias = this_alias;
            }
        }

        // Select all required columns from the final alias with stable labels
        for var in &plan.target.projected_cols {
            // Try to map template variable -> DB column; if not found, assume it is a column already.
            let db_col = find_table_field_by_name(plan.target.model, var)
                .map(|tf| tf.column)
                .unwrap_or(var);

            let full_key = label(&plan.sql.alias_base, var); // keep variable name in the label
            select.push(format!("{}.{c}:{full_key}", plan.sql.final_alias, c = db_col));
            labels.push(ProjectionLabel {
                full_key,
                col: var.clone(),
            });
        }
    }

    q = q.select(select)?;
    Ok((q, labels))
}
