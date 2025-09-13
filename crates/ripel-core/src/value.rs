//! Engine-agnostic dynamic value representation.
//!
//! This module defines a small, immutable value type (`DynamicValue`) with a compact,
//! clone-cheap internal representation (`ValueRepr`). It supports scalars, strings,
//! bytes, and nested key/value objects, with ergonomic accessors and sensible
//! cross-kind equality for numbers.

use anyhow::Error;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// Coarse classification of values.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[non_exhaustive]
pub enum ValueKind {
    Undefined,
    None,
    Bool,
    Number,
    String,
    Bytes,
    Object, // nested map<String, DynamicValue>
    Invalid,
}

impl fmt::Display for ValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match *self {
            ValueKind::Undefined => "undefined",
            ValueKind::None => "none",
            ValueKind::Bool => "bool",
            ValueKind::Number => "number",
            ValueKind::String => "string",
            ValueKind::Bytes => "bytes",
            ValueKind::Object => "object",
            ValueKind::Invalid => "invalid value",
        })
    }
}

/// String flavor
#[derive(Copy, Clone, Debug)]
pub enum StringType {
    Normal,
    Safe,
}

/// Undefined flavor
#[derive(Copy, Clone, Debug)]
pub enum UndefinedType {
    Default,
    Silent,
}

/// 16-byte values, packed to avoid increasing `DynamicValue` size.
#[derive(Copy, Debug)]
#[repr(packed)]
pub struct Packed<T: Copy>(pub T);
impl<T: Copy> Clone for Packed<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy> Packed<T> {
    #[inline]
    pub fn get(&self) -> T {
        // SAFETY: Packed<T> may be unaligned; read with read_unaligned and return by value.
        unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(self.0)) }
    }
}

/// Nested key/value object: stable iteration via `BTreeMap`, cheap clone via `Arc`.
#[derive(Clone, Debug)]
pub struct ObjectValue(Arc<BTreeMap<smol_str::SmolStr, DynamicValue>>);

impl Default for ObjectValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectValue {
    pub fn new() -> Self {
        Self(Arc::new(BTreeMap::new()))
    }
    pub fn with_map(map: BTreeMap<smol_str::SmolStr, DynamicValue>) -> Self {
        Self(Arc::new(map))
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, key: &str) -> Option<&DynamicValue> {
        self.0.get(key)
    }
    pub fn keys(&self) -> impl Iterator<Item = &smol_str::SmolStr> {
        self.0.keys()
    }
    pub fn iter(&self) -> impl Iterator<Item = (&smol_str::SmolStr, &DynamicValue)> {
        self.0.iter()
    }

    /// Persistent-style insert. Reuses allocation if uniquely owned.
    pub fn insert(&mut self, key: impl Into<smol_str::SmolStr>, value: DynamicValue) -> &Self {
        let map = Arc::make_mut(&mut self.0); // clones only if needed
        map.insert(key.into(), value);
        self
    }

    pub fn expand(&mut self, other: &ObjectValue) -> &Self {
        let map = Arc::make_mut(&mut self.0); // clones only if needed
        for (k, v) in other.iter() {
            map.insert(k.clone(), v.clone());
        }
        self
    }
}

impl FromIterator<(smol_str::SmolStr, DynamicValue)> for ObjectValue {
    fn from_iter<T: IntoIterator<Item = (smol_str::SmolStr, DynamicValue)>>(iter: T) -> Self {
        let mut map = BTreeMap::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        Self::with_map(map)
    }
}

/// Internal representation for `DynamicValue`.
#[derive(Clone)]
pub enum ValueRepr {
    None,
    Undefined(UndefinedType),
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Invalid(Arc<Error>),
    U128(Packed<u128>),
    I128(Packed<i128>),
    String(Arc<str>, StringType),
    SmallStr(smol_str::SmolStr),
    Bytes(Arc<Vec<u8>>),
    Object(ObjectValue),
}

impl fmt::Debug for ValueRepr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueRepr::Undefined(_) => f.write_str("undefined"),
            ValueRepr::None => f.write_str("none"),
            ValueRepr::Bool(v) => v.fmt(f),
            ValueRepr::U64(v) => v.fmt(f),
            ValueRepr::I64(v) => v.fmt(f),
            ValueRepr::F64(v) => v.fmt(f),
            ValueRepr::Invalid(e) => write!(f, "<invalid value: {e}>"),
            ValueRepr::U128(v) => (v.get()).fmt(f),
            ValueRepr::I128(v) => (v.get()).fmt(f),
            ValueRepr::String(s, _) => s.fmt(f),
            ValueRepr::SmallStr(s) => s.as_str().fmt(f),
            ValueRepr::Bytes(b) => {
                write!(f, "b'")?;
                for &byte in b.iter() {
                    if byte == b'"' {
                        write!(f, "\"")?;
                    } else {
                        write!(f, "{}", byte.escape_ascii())?;
                    }
                }
                write!(f, "'")
            }
            ValueRepr::Object(obj) => {
                // Compact, stable debug print
                f.write_str("{")?;
                let mut first = true;
                for (k, v) in obj.iter() {
                    if !first {
                        f.write_str(", ")?;
                    } else {
                        first = false;
                    }
                    write!(f, "{:?}: {:?}", k.as_str(), v)?;
                }
                f.write_str("}")
            }
        }
    }
}

/// Public dynamic value wrapper.
#[derive(Clone)]
pub struct DynamicValue(pub ValueRepr);

impl DynamicValue {
    #[inline]
    pub fn undefined() -> Self {
        Self(ValueRepr::Undefined(UndefinedType::Default))
    }
    #[inline]
    pub fn none() -> Self {
        Self(ValueRepr::None)
    }
    #[inline]
    pub fn invalid(err: Error) -> Self {
        Self(ValueRepr::Invalid(Arc::new(err)))
    }

    #[inline]
    pub fn small<S: Into<smol_str::SmolStr>>(s: S) -> Self {
        Self(ValueRepr::SmallStr(s.into()))
    }

    #[inline]
    pub fn big<S: Into<Arc<str>>>(s: S) -> Self {
        Self(ValueRepr::String(s.into(), StringType::Normal))
    }

    #[inline]
    pub fn from_safe_string(s: String) -> Self {
        Self(ValueRepr::String(Arc::from(s), StringType::Safe))
    }

    #[inline]
    pub fn from_bytes(b: Vec<u8>) -> Self {
        Self(ValueRepr::Bytes(Arc::new(b)))
    }

    #[inline]
    pub fn from_object(obj: ObjectValue) -> Self {
        Self(ValueRepr::Object(obj))
    }

    // --- classification & presence ---
    pub fn kind(&self) -> ValueKind {
        match &self.0 {
            ValueRepr::Undefined(_) => ValueKind::Undefined,
            ValueRepr::None => ValueKind::None,
            ValueRepr::Bool(_) => ValueKind::Bool,
            ValueRepr::I64(_)
            | ValueRepr::U64(_)
            | ValueRepr::F64(_)
            | ValueRepr::I128(_)
            | ValueRepr::U128(_) => ValueKind::Number,
            ValueRepr::String(..) | ValueRepr::SmallStr(_) => ValueKind::String,
            ValueRepr::Bytes(_) => ValueKind::Bytes,
            ValueRepr::Object(_) => ValueKind::Object,
            ValueRepr::Invalid(_) => ValueKind::Invalid,
        }
    }
    pub fn is_undefined(&self) -> bool {
        matches!(self.0, ValueRepr::Undefined(_))
    }
    pub fn is_none(&self) -> bool {
        matches!(self.0, ValueRepr::None)
    }
    pub fn is_some(&self) -> bool {
        !self.is_none() && !self.is_undefined()
    }

    pub fn is_number(&self) -> bool {
        matches!(
            self.0,
            ValueRepr::U64(_)
                | ValueRepr::I64(_)
                | ValueRepr::F64(_)
                | ValueRepr::I128(_)
                | ValueRepr::U128(_)
        )
    }
    pub fn is_integer(&self) -> bool {
        matches!(
            self.0,
            ValueRepr::U64(_) | ValueRepr::I64(_) | ValueRepr::I128(_) | ValueRepr::U128(_)
        )
    }

    // --- borrowed views / scalar copies ---
    pub fn as_str(&self) -> Option<&str> {
        match &self.0 {
            ValueRepr::String(s, _) => Some(s),
            ValueRepr::SmallStr(s) => Some(s.as_str()),
            ValueRepr::Bytes(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }
    pub fn to_str(&self) -> Option<Arc<str>> {
        match &self.0 {
            ValueRepr::String(s, _) => Some(s.clone()),
            ValueRepr::SmallStr(s) => Some(Arc::from(s.as_str())),
            ValueRepr::Bytes(b) => Some(Arc::from(String::from_utf8_lossy(b))),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.0 {
            ValueRepr::String(s, _) => Some(s.as_bytes()),
            ValueRepr::SmallStr(s) => Some(s.as_str().as_bytes()),
            ValueRepr::Bytes(b) => Some(b),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match &self.0 {
            ValueRepr::Bool(b) => Some(*b),
            ValueRepr::I64(n) => Some(*n != 0),
            ValueRepr::U64(n) => Some(*n != 0),
            ValueRepr::I128(n) => Some(n.0 != 0),
            ValueRepr::U128(n) => Some(n.0 != 0),
            ValueRepr::F64(f) => Some(*f != 0.0),
            ValueRepr::String(s, _) => Some(!s.is_empty()),
            ValueRepr::SmallStr(s) => Some(!s.is_empty()),
            ValueRepr::Bytes(b) => Some(!b.is_empty()),
            ValueRepr::None
            | ValueRepr::Undefined(_)
            | ValueRepr::Invalid(_)
            | ValueRepr::Object(_) => None,
        }
    }
    pub fn as_i64(&self) -> Option<i64> {
        match &self.0 {
            ValueRepr::I64(n) => Some(*n),
            ValueRepr::U64(n) => (*n <= i64::MAX as u64).then_some(*n as i64),
            ValueRepr::F64(f) => f.is_finite().then_some(*f as i64),
            ValueRepr::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }
    pub fn as_u64(&self) -> Option<u64> {
        match &self.0 {
            ValueRepr::U64(n) => Some(*n),
            ValueRepr::I64(n) => (*n >= 0).then_some(*n as u64),
            ValueRepr::F64(f) => (f.is_finite() && *f >= 0.0).then_some(*f as u64),
            ValueRepr::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match &self.0 {
            ValueRepr::F64(f) => Some(*f),
            ValueRepr::I64(n) => Some(*n as f64),
            ValueRepr::U64(n) => Some(*n as f64),
            ValueRepr::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn len(&self) -> Option<usize> {
        match &self.0 {
            ValueRepr::String(s, _) => Some(s.chars().count()),
            ValueRepr::SmallStr(s) => Some(s.as_str().chars().count()),
            ValueRepr::Bytes(b) => Some(b.len()),
            ValueRepr::Object(obj) => Some(obj.len()),
            _ => None,
        }
    }

    /// Lookup by key on nested objects. Returns `Undefined` if key is missing.
    pub fn get_attr(&self, key: &str) -> DynamicValue {
        match &self.0 {
            ValueRepr::Undefined(_) => DynamicValue::undefined(),
            ValueRepr::Object(obj) => obj
                .get(key)
                .cloned()
                .unwrap_or_else(DynamicValue::undefined),
            _ => DynamicValue::undefined(),
        }
    }

    pub fn to_lossy_string(&self) -> String {
        match &self.0 {
            ValueRepr::String(s, _) => s.to_string(),
            ValueRepr::SmallStr(s) => s.to_string(),
            ValueRepr::I64(n) => n.to_string(),
            ValueRepr::U64(n) => n.to_string(),
            ValueRepr::F64(f) => f.to_string(),
            ValueRepr::I128(n) => n.get().to_string(),
            ValueRepr::U128(n) => n.get().to_string(),
            ValueRepr::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            ValueRepr::Bool(b) => b.to_string(),
            ValueRepr::None => "none".to_string(),
            ValueRepr::Undefined(_) => "undefined".to_string(),
            ValueRepr::Invalid(e) => format!("invalid: {e}"),
            ValueRepr::Object(obj) => {
                let mut s = String::from("{");
                let mut first = true;
                for (k, v) in obj.iter() {
                    if !first {
                        s.push_str(", ");
                    } else {
                        first = false;
                    }
                    // naive escaping is fine here; this is lossy stringification
                    s.push('"');
                    s.push_str(k.as_str());
                    s.push_str("\": ");
                    s.push_str(&v.to_string());
                }
                s.push('}');
                s
            }
        }
    }
}

impl fmt::Debug for DynamicValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
impl fmt::Display for DynamicValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ValueRepr::Undefined(_) => Ok(()),
            ValueRepr::Bool(v) => v.fmt(f),
            ValueRepr::U64(v) => v.fmt(f),
            ValueRepr::I64(v) => v.fmt(f),
            ValueRepr::F64(v) => {
                if v.is_nan() {
                    f.write_str("NaN")
                } else if v.is_infinite() {
                    write!(f, "{}inf", if v.is_sign_negative() { "-" } else { "" })
                } else {
                    let mut s = v.to_string();
                    if !s.contains('.') {
                        s.push_str(".0");
                    }
                    write!(f, "{s}")
                }
            }
            ValueRepr::None => f.write_str("none"),
            ValueRepr::Invalid(e) => write!(f, "<invalid value: {e}>"),
            ValueRepr::I128(v) => write!(f, "{}", v.get()),
            ValueRepr::U128(v) => write!(f, "{}", v.get()),
            ValueRepr::String(s, _) => write!(f, "{s}"),
            ValueRepr::SmallStr(s) => write!(f, "{}", s.as_str()),
            ValueRepr::Bytes(b) => write!(f, "{}", String::from_utf8_lossy(b)),
            ValueRepr::Object(obj) => {
                // Lightweight JSON-ish print
                let mut s = String::from("{");
                let mut first = true;
                for (k, v) in obj.iter() {
                    if !first {
                        s.push_str(", ");
                    } else {
                        first = false;
                    }
                    s.push('"');
                    s.push_str(k.as_str());
                    s.push_str("\": ");
                    s.push_str(&v.to_string());
                }
                s.push('}');
                f.write_str(&s)
            }
        }
    }
}

/* ----------------------- Equality across kinds ----------------------- */

impl PartialEq for DynamicValue {
    fn eq(&self, other: &Self) -> bool {
        use ValueRepr::*;

        #[derive(Copy, Clone, Debug)]
        enum Num {
            I(i128),
            U(u128),
            F(f64),
        }

        fn to_num(v: &ValueRepr) -> Option<Num> {
            match v {
                I64(n) => Some(Num::I(*n as i128)),
                U64(n) => Some(Num::U(*n as u128)),
                I128(n) => Some(Num::I(n.0)),
                U128(n) => Some(Num::U(n.0)),
                F64(f) => Some(Num::F(*f)),
                _ => Option::None,
            }
        }
        fn num_eq(a: Num, b: Num) -> bool {
            use Num::*;
            match (a, b) {
                (I(x), I(y)) => x == y,
                (U(x), U(y)) => x == y,
                (I(x), U(y)) | (U(y), I(x)) => {
                    if x < 0 {
                        return false;
                    }
                    (x as u128) == y
                }
                (F(x), F(y)) => x == y, // f64 semantics (NaN != NaN)
                (I(x), F(y)) | (F(y), I(x)) => {
                    if !y.is_finite() {
                        return false;
                    }
                    let yi = y as i128;
                    (yi as f64) == y && x == yi
                }
                (U(x), F(y)) | (F(y), U(x)) => {
                    if !y.is_finite() || y < 0.0 {
                        return false;
                    }
                    let yu = y as u128;
                    (yu as f64) == y && x == yu
                }
            }
        }

        match (&self.0, &other.0) {
            (None, None) => true,
            (Undefined(_), Undefined(_)) => true,
            (None, Undefined(_)) | (Undefined(_), None) => false,

            (Bool(a), Bool(b)) => a == b,
            (Bytes(a), Bytes(b)) => a.as_slice() == b.as_slice(),

            (String(a, _), String(b, _)) => a == b,
            (SmallStr(a), SmallStr(b)) => a.as_str() == b.as_str(),
            (String(a, _), SmallStr(b)) | (SmallStr(b), String(a, _)) => a.as_ref() == b.as_str(),

            (Invalid(a), Invalid(b)) => Arc::ptr_eq(a, b),

            (Object(a), Object(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter()
                    .zip(b.iter())
                    .all(|((ka, va), (kb, vb))| ka == kb && va == vb)
            }

            (a, b) => match (to_num(a), to_num(b)) {
                (Some(na), Some(nb)) => num_eq(na, nb),
                _ => false,
            },
        }
    }
}

impl Eq for DynamicValue {}

impl From<()> for DynamicValue {
    fn from(_: ()) -> Self {
        Self::none()
    }
}
impl From<bool> for DynamicValue {
    fn from(v: bool) -> Self {
        Self(ValueRepr::Bool(v))
    }
}
impl From<i64> for DynamicValue {
    fn from(v: i64) -> Self {
        Self(ValueRepr::I64(v))
    }
}
impl From<i32> for DynamicValue {
    fn from(v: i32) -> Self {
        Self(ValueRepr::I64(v as i64))
    }
}
impl From<i16> for DynamicValue {
    fn from(v: i16) -> Self {
        Self(ValueRepr::I64(v as i64))
    }
}
impl From<u64> for DynamicValue {
    fn from(v: u64) -> Self {
        Self(ValueRepr::U64(v))
    }
}
impl From<u32> for DynamicValue {
    fn from(v: u32) -> Self {
        Self(ValueRepr::U64(v as u64))
    }
}

impl From<u16> for DynamicValue {
    fn from(v: u16) -> Self {
        Self(ValueRepr::U64(v as u64))
    }
}
impl From<f64> for DynamicValue {
    fn from(v: f64) -> Self {
        Self(ValueRepr::F64(v))
    }
}
impl From<f32> for DynamicValue {
    fn from(v: f32) -> Self {
        Self(ValueRepr::F64(v as f64))
    }
}
impl From<&str> for DynamicValue {
    fn from(s: &str) -> Self {
        if s.len() <= 24 {
            DynamicValue::small(smol_str::SmolStr::new(s))
        } else {
            DynamicValue::big(Arc::<str>::from(s))
        }
    }
}

impl From<String> for DynamicValue {
    fn from(s: String) -> Self {
        if s.len() <= 24 {
            DynamicValue::small(smol_str::SmolStr::new(&s))
        } else {
            DynamicValue::big(Arc::<str>::from(s.as_str()))
        }
    }
}

impl From<Vec<u8>> for DynamicValue {
    fn from(b: Vec<u8>) -> Self {
        DynamicValue::from_bytes(b)
    }
}

impl From<ObjectValue> for DynamicValue {
    fn from(o: ObjectValue) -> Self {
        DynamicValue::from_object(o)
    }
}

impl TryFrom<&DynamicValue> for i64 {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_i64()
            .ok_or_else(|| anyhow::anyhow!("cannot convert to i64: {}", v.to_string()))
    }
}

impl TryFrom<DynamicValue> for i64 {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        i64::try_from(&v)
    }
}

impl TryFrom<DynamicValue> for String {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        v.to_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("cannot convert to string: {}", v.to_string()))
    }
}

impl TryFrom<&DynamicValue> for u32 {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_u64()
            .and_then(|n| (n <= u32::MAX as u64).then_some(n as u32))
            .ok_or_else(|| anyhow::anyhow!("cannot convert to u32: {}", v.to_string()))
    }
}


impl TryFrom<DynamicValue> for u32 {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        u32::try_from(&v)
    }
}

impl TryFrom<&DynamicValue> for u64 {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_u64()
            .ok_or_else(|| anyhow::anyhow!("cannot convert to u64: {}", v.to_string()))
    }
}

impl TryFrom<DynamicValue> for u64 {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        u64::try_from(&v)
    }
}

impl TryFrom<&DynamicValue> for i32 {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_i64()
            .and_then(|n| (n >= i32::MIN as i64 && n <= i32::MAX as i64).then_some(n as i32))
            .ok_or_else(|| anyhow::anyhow!("cannot convert to i32: {}", v.to_string()))
    }
}

impl TryFrom<DynamicValue> for i32 {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        i32::try_from(&v)
    }
}

impl TryFrom<&DynamicValue> for bool {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_bool()
            .ok_or_else(|| anyhow::anyhow!("cannot convert to bool: {}", v.to_string()))
    }
}

impl TryFrom<DynamicValue> for bool {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        bool::try_from(&v)
    }
}

impl TryFrom<&DynamicValue> for f64 {
    type Error = Error;
    fn try_from(v: &DynamicValue) -> Result<Self, Self::Error> {
        v.as_f64()
            .ok_or_else(|| anyhow::anyhow!("cannot convert to f64: {}", v.to_string()))
    }
}

impl TryFrom<DynamicValue> for f64 {
    type Error = Error;
    fn try_from(v: DynamicValue) -> Result<Self, Self::Error> {
        f64::try_from(&v)
    }
}

