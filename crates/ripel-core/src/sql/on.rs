use std::{str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Result};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::sql::helpers::{quote_ident_path, validate_ident};

#[derive(Debug, Clone)]
pub enum RightOperand {
    Ident(Arc<str>),   // dotted identifier path
    Null,
    Number(Arc<str>),  // keep original text (e.g., 1, 3.14, -2e10)
    Str(Arc<str>),     // unquoted inner string (we'll quote for SQL)
}


#[derive(Debug, Clone)]
pub struct OnClause {
    left: Arc<str>,
    operator: Arc<str>, // normalized (UPPERCASE, single spaces)
    right: RightOperand,    // either ident path or "NULL"
}

impl OnClause {
    pub fn new(left: &str, operator: &str, right: &str) -> Result<Self> {
        let op = normalize_op(operator)?;
        let right = parse_right(right)?;
        validate_on(&op, left, &right)?;
        Ok(Self {
            left: Arc::from(left),
            operator: Arc::from(op.as_str()),
            right,
        })
    }

    /// Render to SQL with quoting; handles `IS/IS NOT NULL`.
    pub fn to_sql(&self) -> Result<String> {
        let op = &*self.operator;
        let lq = quote_ident_path(&self.left)?;
        match &self.right {
            RightOperand::Null => match op {
                "IS" => Ok(format!("{lq} IS NULL")),
                "IS NOT" => Ok(format!("{lq} IS NOT NULL")),
                _ => bail!("operator `{op}` not valid with NULL"),
            },
            RightOperand::Ident(p) => {
                let rq = quote_ident_path(p)?;
                Ok(format!("{lq} {op} {rq}"))
            }
            RightOperand::Number(n) => Ok(format!("{lq} {op} {n}")),
            RightOperand::Str(s) => {
                // single-quote and escape internal single quotes by doubling
                let escaped = s.replace('\'', "''");
                Ok(format!("{lq} {op} '{}'", escaped))
            }
        }
    }
}

impl FromStr for OnClause {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self> {
        // Split by the operator. Order matters: longest/most specific first.
        static OP_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)\s*(is\s+not|is|<=|>=|<>|!=|=|<|>)\s*").unwrap()
        });

        let s = input.trim();
        let m = OP_RE
            .find(s)
            .ok_or_else(|| anyhow!("invalid ON clause: `{input}`"))?;

        let left_raw  = s[..m.start()].trim();
        let op_raw    = s[m.start()..m.end()].trim();
        let right_raw = s[m.end()..].trim();

        if left_raw.is_empty() || right_raw.is_empty() {
            bail!("invalid ON clause: `{input}`");
        }

        let op    = normalize_op(op_raw)?;
        let right = parse_right(right_raw)?;
        validate_on(&op, left_raw, &right)?;

        Ok(Self {
            left: Arc::from(left_raw),
            operator: Arc::from(op),
            right,
        })
    }
}

fn normalize_op(op: &str) -> Result<String> {
    let up = op.trim().to_ascii_uppercase();
    let norm = up.split_whitespace().collect::<Vec<_>>().join(" ");
    match norm.as_str() {
        "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "IS" | "IS NOT" => Ok(norm),
        _ => bail!("unsupported operator `{op}`"),
    }
}

fn parse_right(raw: &str) -> Result<RightOperand> {
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok(RightOperand::Null);
    }
    // quoted string?
    if raw.len() >= 2 {
        let (first, last) = (raw.as_bytes()[0], raw.as_bytes()[raw.len()-1]);
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            let inner = &raw[1..raw.len()-1];
            return Ok(RightOperand::Str(Arc::from(inner)));
        }
    }
    // number?
    if raw.as_bytes()[0].is_ascii_digit() || raw.starts_with(['+', '-']) {
        // keep as-is; DB will parse it
        if raw.chars().all(|c|
            c.is_ascii_digit() || matches!(c, '+'|'-'|'.'|'e'|'E')
        ) {
            return Ok(RightOperand::Number(Arc::from(raw)));
        }
    }
    // fallback: identifier path
    validate_ident(raw)?; // ensure it's a dotted identifier
    Ok(RightOperand::Ident(Arc::from(raw)))
}

impl std::fmt::Display for OnClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_sql() {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

impl TryFrom<&str> for OnClause {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<Self> {
        Self::from_str(s)
    }
}

impl TryFrom<String> for OnClause {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self> {
        Self::from_str(&s)
    }
}


fn validate_on(op: &str, left: &str, right: &RightOperand) -> Result<()> {
    validate_ident(left)?;
    match right {
        RightOperand::Null => match op {
            "IS" | "IS NOT" => Ok(()),
            _ => bail!("only IS / IS NOT allowed with NULL in ON clause"),
        },
        // any operator is fine for ident/number/str
        RightOperand::Ident(_) | RightOperand::Number(_) | RightOperand::Str(_) => Ok(())
    }
}