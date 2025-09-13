use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use regex::Regex;

/// Very simple identifier guard: letters, digits, underscore only.
pub (crate) fn validate_ident(path: &str) -> Result<()> {
    static SEG: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());
    if path.is_empty() { bail!("invalid identifier: empty"); }
    for part in path.split('.') {
        if part.is_empty() { bail!("invalid identifier: empty segment in `{path}`"); }
        if !SEG.is_match(part) { bail!("invalid identifier segment `{part}` in `{path}`"); }
    }
    Ok(())
}

pub (crate) fn quote_ident_path(path: &str) -> Result<String> {
    validate_ident(path)?;
    Ok(path.split('.').map(|p| format!("`{p}`")).collect::<Vec<_>>().join("."))
}