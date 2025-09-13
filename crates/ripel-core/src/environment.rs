

use minijinja::{value::Kwargs, Environment, Error, ErrorKind, State, Value};
use ulid::Ulid;

use crate::{sql::{Query, QueryExt}, value::DynamicValue};

fn resolve_arg(state: &State, v: Value) -> Value {
    if let Some(name) = v.as_str() {
        if let Some(found) = state.lookup(name) {
            return found;
        }
    }
    v
}

/// Like [`Query::new`] but wraps it in a [`Value`].
fn query(table: &str) -> Value {
    Value::from_object(Query::new(table))
}

/// Filters a query by some keyword arguments as filter function.
fn filter_filter(obj: &Query, kwargs: Kwargs) -> Result<Value, Error> {
    Ok(Value::from_object(obj.filter(kwargs)))
}

/// Applies a limit to a query as filter function.
fn limit_filter(obj: &Query, limit: usize) -> Result<Value, Error> {
    Ok(Value::from_object(obj.limit(limit)))
}

/// Applies an offset to a query as filter function.
fn offset_filter(obj: &Query, offset: usize) -> Result<Value, Error> {
    Ok(Value::from_object(obj.offset(offset)))
}

fn parse_unix_millis(v: &Value) -> Result<u64, Error> {
    // numeric?
    if let Some(i) = v.as_i64() {
        // Heuristic: treat very large numbers as ms already
        return Ok(if i >= 1_000_000_000_000 {
            i as u64
        } else {
            (i as u64) * 1000
        });
    }
    // string?
    let s = v.to_string();
    if let Ok(i) = s.parse::<i128>() {
        return Ok(if i >= 1_000_000_000_000 {
            i as u64
        } else {
            (i as u64) * 1000
        });
    }
    if let Ok(dt) = time::OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
    {
        let ms = dt.unix_timestamp() as u64 * 1000 + (dt.nanosecond() / 1_000_000) as u64;
        return Ok(ms);
    }
    Err(Error::new(
        ErrorKind::InvalidOperation,
        format!("cannot coerce {s:?} to millis"),
    ))
}

fn ulid(state: &State, random: Value, created_at: Value) -> Result<Value, Error> {
    let random = resolve_arg(state, random);
    let created_at = resolve_arg(state, created_at);
    let ms = parse_unix_millis(&created_at)?;
    // ULID = 48-bit ms (big-endian) + 80-bit randomness
    let mut bytes = [0u8; 16];

    // put the lower 48 bits of ms into the first 6 bytes (big-endian)
    let ms_be = ms.to_be_bytes(); // 8 bytes
    bytes[0..6].copy_from_slice(&ms_be[2..]); // take the last 6

    // deterministic randomness: hash(id) -> first 10 bytes
    // (optionally use a keyed hash with your salt)
    let hash = blake3::hash(random.to_string().as_bytes());
    bytes[6..16].copy_from_slice(&hash.as_bytes()[..10]);

    // Convert name to u128 hash indepondent generation

    let ulid = Ulid::from_bytes(bytes).to_string();
    let dyn_value = DynamicValue::try_from(ulid)
        .map_err(|e| Error::new(ErrorKind::InvalidOperation, format!("cannot convert ULID to DynamicValue: {}", e)))?;

    Ok(Value::from(dyn_value))
}

pub fn default_env() -> Environment<'static> {
    let mut env = Environment::default();
    env.add_function("query", query);
    env.add_function("ulid", ulid);
    env.add_function("filter", filter_filter);
    env.add_function("limit", limit_filter);
    env.add_function("offset", offset_filter);
    env
}