
use std::collections::HashSet;

use minijinja::{value::Object, Environment, Expression, Value};
use anyhow::{anyhow, Result, Context};

use crate::value::{DynamicValue};

pub fn get_col<T: Object + Clone>(obj: &T, key: &str) -> Result<DynamicValue> {
    let obj = &std::sync::Arc::new(obj.clone());
    let dv = obj
        .get_value(&Value::from(key))
        .ok_or_else(|| anyhow!("missing column `{key}`"))?;
    
    DynamicValue::try_from(dv)
        .map_err(|e| anyhow!("cannot convert column `{}` value to target type: {}", key, e))
}

/// Evaluate a MiniJinja expression against the ObjectValue context and parse to T.
/// Common for templated ids: returns string → parsed into T (e.g., Ulid implements FromStr).
pub fn eval_template<T: Object + Clone + 'static>(
    ctx: &T,
    env: &Environment<'_>,
    tmpl: &str,
) -> Result<DynamicValue> {
    let out = env
        .compile_expression(tmpl)?
        .eval(Value::from_object(ctx.clone()))
        .with_context(|| format!("evaluating expression `{}`", tmpl))?;
    Ok(DynamicValue::from(out))
}


pub fn eval_expression<T: Object + Clone + 'static>(
    ctx: &T,
    expr: &Expression<'_, '_>,
) -> Result<DynamicValue> {
    let out = expr
        .eval(Value::from_object(ctx.clone()))
        .with_context(|| format!("evaluating expression `{:#?}`", expr))?;
    Ok(DynamicValue::from(out))
}

pub fn compile_template<'env, 'source>(
    env: &'env Environment<'env>,
    expr: &'source str,
) -> Result<Expression<'env, 'source>>
where
    'env: 'source,
{
    env.compile_expression(expr)
        .with_context(|| format!("compiling expression `{}`", expr))
}

pub fn get_variables(env: &Environment<'_>, expression: &Expression<'_, '_>) -> HashSet<String> {
    let globals = env.globals().map(|(key, _)| key.to_owned()).collect::<HashSet<_>>();
    let variables = expression.undeclared_variables(true);

    variables.difference(&globals).cloned().collect()
}

/// Entities can build themselves from ObjectValue, interpolating templates.
pub trait FromObject: Sized {
    fn from_object<T: Object + Clone + 'static>(obj: &T, env: &Environment<'_>) -> Result<Self>;
}