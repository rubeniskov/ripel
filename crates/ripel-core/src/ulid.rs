use anyhow::{bail, Result};

use crate::value::{DynamicValue, StringType, ValueRepr};

impl TryFrom<&DynamicValue> for ulid::Ulid {
    type Error = anyhow::Error;
    fn try_from(value: &DynamicValue) -> Result<Self> {
        match &value.0 {
            ValueRepr::String(s, _) => Ok(ulid::Ulid::from_string(s).expect("invalid ULID string")),
            _ => bail!("cannot convert non-string value to ULID")
        }
    }
}

impl TryFrom<DynamicValue> for ulid::Ulid {
    type Error = anyhow::Error;
    fn try_from(value: DynamicValue) -> Result<Self> {
        ulid::Ulid::try_from(&value)
    }
}

impl TryFrom<&ulid::Ulid> for DynamicValue {
    type Error = anyhow::Error;
    fn try_from(v: &ulid::Ulid) -> Result<Self> {
        use std::sync::Arc;
        Ok(Self(ValueRepr::String(Arc::<str>::from(v.to_string()), StringType::Normal)))
    }
}

impl TryFrom<ulid::Ulid> for DynamicValue {
    type Error = anyhow::Error;
    fn try_from(v: ulid::Ulid) -> Result<Self> {
        DynamicValue::try_from(&v)
    }
}

