#![cfg(feature = "jinja")]

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use minijinja::value::{Value as JValue, ValueKind as JKind};

use crate::{sql::{AsQuery, Query}, DynamicValue, ObjectValue, ValueRepr};

pub use minijinja::value::Object;

/// Convert a `minijinja::Value` into your engine-agnostic `DynamicValue`.
fn jinja_to_dynamic(v: &JValue) -> DynamicValue {
    match v.kind() {
        JKind::Undefined => DynamicValue(ValueRepr::Undefined(crate::UndefinedType::Default)),
        JKind::None => DynamicValue(ValueRepr::None),
        JKind::Bool => DynamicValue(ValueRepr::Bool(v.is_true())),
        JKind::Number => {
            if let Ok(i) = i64::try_from(v.clone()) {
                DynamicValue(ValueRepr::I64(i))
            } else if let Ok(u) = u64::try_from(v.clone()) {
                DynamicValue(ValueRepr::U64(u))
            } else if let Ok(f) = f64::try_from(v.clone()) {
                DynamicValue(ValueRepr::F64(f))
            } else {
                // Extremely defensive fallback
                DynamicValue::from(v.to_string())
            }
        }
        JKind::String => {
            if let Some(s) = v.as_str() {
                DynamicValue::from(s)
            } else {
                DynamicValue::from(v.to_string())
            }
        }
        JKind::Bytes => {
            if let Some(b) = v.as_bytes() {
                DynamicValue(ValueRepr::Bytes(Arc::new(b.to_vec())))
            } else {
                // Very rare; fall back to lossy string
                DynamicValue::from(v.to_string())
            }
        }
        JKind::Map => {
            let mut map: BTreeMap<smol_str::SmolStr, DynamicValue> = BTreeMap::new();
            // Iterate keys, then look up values
            if let Ok(keys) = v.try_iter() {
                for key in keys {
                    let kstr = key
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| key.to_string());
                    if let Ok(val) = v.get_item(&key) {
                        map.insert(smol_str::SmolStr::new(&kstr), jinja_to_dynamic(&val));
                    }
                }
            }
            DynamicValue(ValueRepr::Object(ObjectValue::with_map(map)))
        }
        JKind::Seq | JKind::Iterable => {
            // Represent sequences as object with numeric string keys ("0","1",...)
            let mut map: BTreeMap<smol_str::SmolStr, DynamicValue> = BTreeMap::new();
            if let Ok(iter) = v.try_iter() {
                for (idx, item) in iter.enumerate() {
                    map.insert(
                        smol_str::SmolStr::new(idx.to_string()),
                        jinja_to_dynamic(&item),
                    );
                }
            }
            DynamicValue(ValueRepr::Object(ObjectValue::with_map(map)))
        }
        JKind::Plain => DynamicValue::from(v.to_string()),
        JKind::Invalid => DynamicValue(ValueRepr::Invalid(Arc::new(anyhow!(
            "minijinja invalid value"
        )))),
        _ => panic!("jinja_to_dynamic: unhandled kind {:?}", v.kind()),
    }
}

/// Convert your `DynamicValue` into a `minijinja::Value`.
fn dynamic_to_jinja(v: &DynamicValue) -> JValue {
    match &v.0 {
        ValueRepr::None => JValue::from(()),
        ValueRepr::Undefined(_) => JValue::UNDEFINED,

        ValueRepr::Bool(b) => JValue::from(*b),
        ValueRepr::I64(n) => JValue::from(*n),
        ValueRepr::U64(n) => JValue::from(*n),
        ValueRepr::F64(f) => JValue::from(*f),

        // MiniJinja does not have native 128-bit numbers → stringify.
        ValueRepr::I128(n) => JValue::from(n.get().to_string()),
        ValueRepr::U128(n) => JValue::from(n.get().to_string()),

        ValueRepr::String(s, _ty) => JValue::from(&**s),
        ValueRepr::SmallStr(s) => JValue::from(s.as_str()),

        ValueRepr::Bytes(b) => JValue::from_bytes((b.as_ref()).clone()),

        ValueRepr::Invalid(e) => JValue::from(format!("<invalid: {e}>")),

        ValueRepr::Object(obj) => {
            // Convert to a plain map<String, JValue>.
            let mut map: BTreeMap<String, JValue> = BTreeMap::new();
            for (k, dv) in obj.iter() {
                map.insert(k.to_string(), dynamic_to_jinja(dv));
            }
            JValue::from(map)
        }
    }
}

/// Convenience: convert a MiniJinja map/sequence Value to your ObjectValue directly.
/// Non-map/seq values become a one-field object with key `"_value"`.
pub fn jinja_to_object(v: &JValue) -> ObjectValue {
    match v.kind() {
        JKind::Map => {
            let mut map = BTreeMap::new();
            if let Ok(keys) = v.try_iter() {
                for key in keys {
                    let kstr = key
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| key.to_string());
                    if let Ok(val) = v.get_item(&key) {
                        map.insert(smol_str::SmolStr::new(&kstr), jinja_to_dynamic(&val));
                    }
                }
            }
            ObjectValue::with_map(map)
        }
        JKind::Seq | JKind::Iterable => {
            let mut map = BTreeMap::new();
            if let Ok(iter) = v.try_iter() {
                for (idx, item) in iter.enumerate() {
                    map.insert(
                        smol_str::SmolStr::new(idx.to_string()),
                        jinja_to_dynamic(&item),
                    );
                }
            }
            ObjectValue::with_map(map)
        }
        _ => {
            let mut map = BTreeMap::new();
            map.insert(smol_str::SmolStr::new("_value"), jinja_to_dynamic(v));
            ObjectValue::with_map(map)
        }
    }
}

impl From<DynamicValue> for JValue {
    fn from(v: DynamicValue) -> Self {
        dynamic_to_jinja(&v)
    }
}

impl From<&JValue> for DynamicValue {
    fn from(v: &JValue) -> Self {
        jinja_to_dynamic(v)
    }
}

impl From<JValue> for DynamicValue {
    fn from(v: JValue) -> Self {
        jinja_to_dynamic(&v)
    }
}

impl minijinja::value::Object for ObjectValue {
   fn get_value(self: &Arc<Self>, key: &minijinja::Value) -> Option<minijinja::Value> {
        let key_str = key.as_str()?;
        self.get(key_str).map(|v| minijinja::Value::from(v.clone()))
   }
}

impl minijinja::value::Object for &ObjectValue {
   fn get_value(self: &Arc<Self>, key: &minijinja::Value) -> Option<minijinja::Value> {
        let key_str = key.as_str()?;
        self.get(key_str).map(|v| minijinja::Value::from(v.clone()))
   }
}

impl AsQuery for minijinja::value::Value {
    fn as_query(&self) -> anyhow::Result<&Query> {
        self.downcast_object_ref::<Query>()
            .ok_or_else(|| anyhow::anyhow!("expression did not evaluate to a Query"))
    }
}