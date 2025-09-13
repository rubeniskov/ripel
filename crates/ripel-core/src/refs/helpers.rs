use anyhow::{anyhow, Result};
use minijinja::Environment;

use crate::entity::{EntityModel, FieldModel, TableField, ReferenceField};
use crate::interpolate::{compile_template, get_variables};
use crate::value::{ObjectValue, DynamicValue};
use crate::refs::Hop;

pub fn iter_table_fields<'a>(
    model: &'a EntityModel,
) -> impl Iterator<Item = &'a TableField> + 'a {
    model.fields.iter().filter_map(|f| match f {
        FieldModel::TableField(t) => Some(t),
        _ => None,
    })
}

pub fn iter_ref_fields<'a>(
    model: &'a EntityModel,
) -> impl Iterator<Item = &'a ReferenceField> + 'a {
    model.fields.iter().filter_map(|f| match f {
        FieldModel::ReferenceField(r) => Some(r),
        _ => None,
    })
}

pub fn find_table_field_by_name<'a>(
    model: &'a EntityModel,
    name: &str,
) -> Option<&'a TableField> {
    iter_table_fields(model).find(|t| t.name == name)
}

/// Union of variables referenced by templates of the model’s TableFields.
pub fn variable_fields_for(
    model: &EntityModel,
    env: &Environment<'_>,
) -> Vec<String> {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    for f in iter_table_fields(model) {
        if let Some(tpl) = f.template {
            if let Ok(expr) = compile_template(env, tpl) {
                for v in get_variables(env, &expr) {
                    set.insert(v);
                }
            }
        }
    }
    set.into_iter().collect()
}

pub fn parse_reference(s: &str) -> Result<(&str, &str)> {
    let (e, f) = s
        .split_once('.')
        .ok_or_else(|| anyhow!("expected Entity.field in `{s}`"))?;
    Ok((e.trim(), f.trim()))
}

pub fn split_rhs(rhs: &str) -> (&str, Option<&str>) {
    match rhs.split_once(',') {
        Some((p, a)) => (p.trim(), Some(a.trim())),
        None => (rhs.trim(), None),
    }
}

pub fn last_ident(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

pub fn hop_alias(base: &str, step: usize) -> String {
    format!("{base}_h{step}")
}

pub fn label(prefix: &str, col: &str) -> String {
    format!("{prefix}__{col}")
}

pub fn final_alias_for_chain(base: &str, via: &[Hop]) -> String {
    if via.is_empty() {
        "self".into()
    } else {
        let last = via.len() - 1;
        let (_p, a) = split_rhs(via[last].rhs);
        a.map(|s| s.to_string())
            .unwrap_or_else(|| format!("{base}_h{last}"))
    }
}

pub fn primary_key_value<'a>(
    model: &'a EntityModel,
    src: &ObjectValue,
) -> Result<(&'a str, DynamicValue)> {
    let tf = iter_table_fields(model)
        .find(|t| t.primary_key)
        .ok_or_else(|| anyhow!("no primary key in `{}`", model.rust_name))?;
    let val = src
        .get(tf.column)
        .cloned()
        .ok_or_else(|| anyhow!("source row missing primary key `{}`", tf.column))?;
    Ok((tf.column, val))
}
