use crate::helper::{mysql_time_bin_to_iso8601};
use crate::{DynamicValue, ObjectValue};
use anyhow::{anyhow, bail, Result};
use smol_str::SmolStr;
use sqlx::{
    decode::Decode,
    error::Error as SqlxError,
    mysql::{MySql, MySqlRow, MySqlValue, MySqlValueRef},
    Column, FromRow, Row, TypeInfo, Value, ValueRef,
};
use std::convert::TryFrom;
use std::{collections::BTreeMap, io};

#[cfg(feature = "time")]
use time::{format_description::well_known::Rfc3339, Date, OffsetDateTime, PrimitiveDateTime};

impl<'a> TryFrom<MySqlValueRef<'a>> for DynamicValue {
    type Error = anyhow::Error;

    fn try_from(vr: MySqlValueRef<'a>) -> Result<Self, Self::Error> {
        if vr.is_null() {
            return Ok(DynamicValue::none());
        }

        let owned = sqlx::ValueRef::to_owned(&vr);
        let tn = owned.as_ref().type_info().name().to_string();

        macro_rules! dec {
            ($t:ty) => {{
                <$t as Decode<'_, MySql>>::decode(owned.as_ref()).map_err(|e| {
                    anyhow!(
                        "decode {} as {} failed: {}",
                        tn,
                        std::any::type_name::<$t>(),
                        e
                    )
                })
            }};
        }

        #[cfg(not(any(feature = "rust_decimal", feature = "bigdecimal", feature = "time")))]
        #[inline]
        fn temporal_feature(tn: &str, feature: &str) -> anyhow::Error {
            anyhow!(
                "cannot decode MySQL temporal type '{}' without the '{}' feature. \
                 Enable it with `--features {feature}` or add `features = [\"{feature}\"]` for this crate.",
                tn,
                feature
            )
        }

        match tn.as_str() {
            // numerics + bool
            "BIGINT" => Ok(DynamicValue::from(dec!(i64)?)),
            "INT" | "MEDIUMINT" => Ok(DynamicValue::from(dec!(i32)? as i64)),
            "SMALLINT" => Ok(DynamicValue::from(dec!(i16)? as i64)),
            "BIGINT UNSIGNED" => Ok(DynamicValue::from(dec!(u64)?)),
            "INT UNSIGNED" => Ok(DynamicValue::from(dec!(u32)? as u64)),
            "SMALLINT UNSIGNED" => Ok(DynamicValue::from(dec!(u16)? as u64)),
            "DOUBLE" => Ok(DynamicValue::from(dec!(f64)?)),
            "FLOAT" => Ok(DynamicValue::from(dec!(f32)? as f64)),
            "BOOLEAN" => Ok(DynamicValue::from(dec!(bool)?)),

            // bytes & text
            "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
                Ok(DynamicValue::from(dec!(Vec<u8>)?))
            }
            "VARCHAR" | "TEXT" | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "CHAR" => {
                Ok(DynamicValue::from(dec!(String)?))
            }

            "ENUM" | "SET" => {
                // Enum and Set are represented as strings
                Ok(DynamicValue::from(dec!(String)?))
            }

            "DATE" => {
                #[cfg(feature = "time")]
                {
                    let d: Date = dec!(Date)?;
                    Ok(DynamicValue::from(d.to_string()))
                }
                #[cfg(not(feature = "time"))]
                {
                    bail!(temporal_feature("DATE", "time"));
                }
            }

            "DATETIME" | "DATETIME(3)" | "DATETIME(6)" => {
                #[cfg(feature = "time")]
                {
                    let pdt: PrimitiveDateTime = dec!(PrimitiveDateTime)?;
                    let odt: OffsetDateTime = pdt.assume_utc();
                    Ok(DynamicValue::from(odt.format(&Rfc3339)?))
                }
                #[cfg(not(feature = "time"))]
                {
                    bail!(temporal_feature("DATETIME", "time"));
                }
            }

            "TIME" | "TIME(3)" | "TIME(6)" => {
                // decode MySQL TIME binary payload, emit ISO-8601 duration
                let raw: Vec<u8> = dec!(Vec<u8>)?;
                let s = mysql_time_bin_to_iso8601(&raw)?;
                Ok(DynamicValue::from(s))
            }

            "TIMESTAMP" | "TIMESTAMP(3)" | "TIMESTAMP(6)" => {
                #[cfg(feature = "time")]
                {
                    let odt: OffsetDateTime = dec!(OffsetDateTime)?;
                    Ok(DynamicValue::from(odt.format(&Rfc3339)?))
                }
                #[cfg(not(feature = "time"))]
                {
                    bail!(temporal_feature("TIMESTAMP", "time"));
                }
            }

            "YEAR" => Ok(DynamicValue::from(dec!(i32)? as i64)),

            s if s.starts_with("DECIMAL") || s.starts_with("NUMERIC") => {
                #[cfg(feature = "rust_decimal")]
                {
                    let d: rust_decimal::Decimal = dec!(rust_decimal::Decimal)?;
                    // canonical string (no trailing zeros, no exponent)
                    let s = d.normalize().to_string();
                    Ok(DynamicValue::from(s))
                }
                #[cfg(all(not(feature = "rust_decimal"), feature = "bigdecimal"))]
                {
                    let d: bigdecimal::BigDecimal = dec!(bigdecimal::BigDecimal)?;
                    // BigDecimal::to_string() is plain decimal; do a quick trim of trailing zeros.
                    let mut s = d.to_string();
                    crate::canonicalize_decimal_string_in_place(&mut s);
                    return Ok(DynamicValue::from(s));
                }
                // Fallback if neither feature is on: make it a helpful error
                #[cfg(all(not(feature = "rust_decimal"), not(feature = "bigdecimal")))]
                {
                    bail!(temporal_feature(
                        "DECIMAL/NUMERIC",
                        "rust_decimal or bigdecimal"
                    ));
                }
            }

            other => bail!("Unsupported MySQL type: {other}"),
        }
    }
}

// the rest of your From/FromRow impls stay the same…
impl TryFrom<&MySqlValue> for DynamicValue {
    type Error = anyhow::Error;
    fn try_from(v: &MySqlValue) -> Result<Self, Self::Error> {
        DynamicValue::try_from(v.as_ref())
    }
}


impl TryFrom<&MySqlRow> for DynamicValue {
    type Error = anyhow::Error;
    fn try_from(row: &MySqlRow) -> Result<Self, Self::Error> {
        Ok(DynamicValue::from(ObjectValue::try_from(row)?))
    }
}

impl<'r> FromRow<'r, MySqlRow> for DynamicValue {
    fn from_row(row: &'r MySqlRow) -> Result<Self, SqlxError> {
        Ok(DynamicValue::from(ObjectValue::from_row(row)?))
    }
}

impl TryFrom<&MySqlRow> for ObjectValue {
    type Error = anyhow::Error;
    fn try_from(row: &MySqlRow) -> Result<Self, Self::Error> {
        let mut map = BTreeMap::new();
        for col in row.columns() {
            let name = col.name();
            let dv = row
                .try_get_raw(name)
                .map_err(|e| anyhow!("error accessing column {name}: {e}"))
                .and_then(DynamicValue::try_from)?;
            map.insert(SmolStr::new(name), dv);
        }
        Ok(ObjectValue::with_map(map))
    }
}

impl<'r> FromRow<'r, MySqlRow> for ObjectValue {
    fn from_row(row: &'r MySqlRow) -> Result<Self, SqlxError> {
        let mut map = BTreeMap::new();
        for col in row.columns() {
            let name = col.name();
            let raw = row.try_get_raw(name)?;
            let dv = DynamicValue::try_from(raw).map_err(|e| SqlxError::ColumnDecode {
                index: name.into(),
                source: Box::new(io::Error::other(e.to_string())),
            })?;
            map.insert(SmolStr::new(name), dv);
        }
        Ok(ObjectValue::with_map(map))
    }
}


