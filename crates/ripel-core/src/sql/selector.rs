use std::{str::FromStr, sync::Arc};

use anyhow::{bail, Result};

use super::helpers::validate_ident;


pub struct Selector {
    source: Option<Arc<str>>,
    column: Arc<str>,
    alias: Option<Arc<str>>,
}

impl Selector {
    pub fn new(column: &str) -> Self {
        Selector {
            source: None,
            column: Arc::from(column),
            alias: None,
        }
    }

    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn set_source(mut self, source: &str) -> Self {
        self.source = Some(Arc::from(source));
        self
    }

    pub fn set_column(mut self, column: &str) -> Self {
        self.column = Arc::from(column);
        self
    }

    pub fn set_alias(mut self, alias: &str) -> Self {
        self.alias = Some(Arc::from(alias));
        self
    }

    pub fn to_sql(&self) -> Result<String> {
        // wildcard column
        if &*self.column == "*" {
            if self.alias.is_some() {
                bail!("cannot alias a wildcard selector (`*` or `src.*`)");
            }
            if let Some(src) = &self.source {
                // validate the source ident; column is `*` so skip ident validation for it
                validate_ident(src)?;
                Ok(format!("`{}`.*", src))
            } else {
                Ok("*".to_string())
            }
        } else {
            // normal column
            validate_ident(&self.column)?;
            let expr = if let Some(src) = &self.source {
                validate_ident(src)?;
                format!("`{}`.`{}`", src, self.column)
            } else {
                format!("`{}`", self.column)
            };

            if let Some(alias) = &self.alias {
                validate_ident(alias)?;
                Ok(format!("{} AS `{}`", expr, alias))
            } else {
                Ok(expr)
            }
        }
    }
}

impl FromStr for Selector {
    type Err = anyhow::Error;

    /// Parse a selector from a string like:
    ///   "*"
    ///   "src.*"
    ///   "col"
    ///   "src.col"
    ///   "src.col:alias"
    ///
    /// Notes:
    /// - Wildcards (`*` or `src.*`) cannot be aliased.
    /// - `:alias` requires a source (i.e., only allowed with `src.col:alias`).
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        if input.is_empty() {
            bail!("empty selector");
        }

        // split alias with ':' (your chosen syntax)
        let (lhs, alias_opt) = if let Some((l, a)) = input.rsplit_once(':') {
            let a = a.trim();
            if a.is_empty() {
                bail!("empty alias after ':'");
            }
            validate_ident(a)?;
            (l.trim(), Some(Arc::<str>::from(a)))
        } else {
            (input, None)
        };

        // handle bare wildcard first
        if lhs == "*" {
            if alias_opt.is_some() {
                bail!("cannot alias a wildcard selector (`*` or `src.*`)");
            }
            return Ok(Selector { source: None, column: Arc::from("*"), alias: None });
        }

        // split on '.', allow at most one
        let mut parts = lhs.split('.').map(str::trim);
        let first = parts.next().ok_or_else(|| anyhow::anyhow!("missing column"))?;
        let second = parts.next();
        let extra = parts.next();
        if extra.is_some() {
            bail!("selector supports at most one dot: `source.column` or `source.*`");
        }

        match (second, alias_opt) {
            // "column"
            (None, None) => {
                if first == "*" {
                    // already handled, but guard anyway
                    bail!("bare '*' should not reach here");
                }
                validate_ident(first)?;
                Ok(Selector { source: None, column: Arc::from(first), alias: None })
            }
            // "column:alias" is not allowed (you require a source for alias)
            (None, Some(_)) => {
                bail!("alias requires a source: use `source.column:alias`");
            }
            // "source.something" (maybe wildcard)
            (Some(col), alias) => {
                if first.is_empty() || col.is_empty() {
                    bail!("empty source/column in `{}`", input);
                }
                validate_ident(first)?;

                if col == "*" {
                    // "source.*"
                    if alias.is_some() {
                        bail!("cannot alias a wildcard selector (`*` or `src.*`)");
                    }
                    return Ok(Selector { source: Some(Arc::from(first)), column: Arc::from("*"), alias: None });
                }

                // "source.column[:alias]"
                validate_ident(col)?;
                Ok(Selector {
                    source: Some(Arc::from(first)),
                    column: Arc::from(col),
                    alias,
                })
            }
        }
    }
}

impl TryFrom<&str> for Selector {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Selector::from_str(value)
    }
}

impl TryFrom<String> for Selector {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Selector::from_str(&value)
    }
}

impl std::fmt::Debug for Selector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Selector")
            .field("source", &self.source.as_deref())
            .field("column", &self.column)
            .field("alias", &self.alias.as_deref())
            .finish()
    }
}

impl std::fmt::Display for Selector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_sql().map_err(|_| std::fmt::Error).and_then(|s| f.write_str(&s))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    // Convenience: parse + render to SQL
    fn sql(s: &str) -> anyhow::Result<String> {
        Selector::from_str(s)?.to_sql()
    }

    #[test]
    fn parse_wildcard_bare() -> anyhow::Result<()> {
        let sel = Selector::from_str("*")?;
        assert_eq!(sel.source(), None);
        assert_eq!(sel.column(), "*");
        assert_eq!(sel.alias(), None);
        assert_eq!(sel.to_sql()?, "*");
        Ok(())
    }

    #[test]
    fn parse_wildcard_with_source() -> anyhow::Result<()> {
        let sel = Selector::from_str("self.*")?;
        assert_eq!(sel.source(), Some("self"));
        assert_eq!(sel.column(), "*");
        assert_eq!(sel.alias(), None);
        assert_eq!(sel.to_sql()?, "`self`.*");
        Ok(())
    }

    #[test]
    fn wildcard_alias_is_rejected() {
        assert!(Selector::from_str("*:x").is_err());
        assert!(Selector::from_str("self.*:x").is_err());

        // Also through the builder: alias + wildcard must fail in to_sql
        let sel = Selector::new("*").set_alias("x");
        assert!(sel.to_sql().is_err());
        let sel = Selector::new("*").set_source("self").set_alias("x");
        assert!(sel.to_sql().is_err());
    }

    #[test]
    fn parse_simple_column() -> anyhow::Result<()> {
        let sel = Selector::from_str("id")?;
        assert_eq!(sel.source(), None);
        assert_eq!(sel.column(), "id");
        assert_eq!(sel.alias(), None);
        assert_eq!(sel.to_sql()?, "`id`");
        Ok(())
    }

    #[test]
    fn parse_qualified_column() -> anyhow::Result<()> {
        let sel = Selector::from_str("self.id")?;
        assert_eq!(sel.source(), Some("self"));
        assert_eq!(sel.column(), "id");
        assert_eq!(sel.alias(), None);
        assert_eq!(sel.to_sql()?, "`self`.`id`");
        Ok(())
    }

    #[test]
    fn parse_with_alias_requires_source() {
        // alias without source is not allowed
        assert!(Selector::from_str("id:alias").is_err());
    }

    #[test]
    fn parse_qualified_with_alias() -> anyhow::Result<()> {
        let sel = Selector::from_str("self.id:the_id")?;
        assert_eq!(sel.source(), Some("self"));
        assert_eq!(sel.column(), "id");
        assert_eq!(sel.alias(), Some("the_id"));
        assert_eq!(sel.to_sql()?, "`self`.`id` AS `the_id`");
        Ok(())
    }

    #[test]
    fn trimming_and_whitespace() -> anyhow::Result<()> {
        // spaces around parts should be tolerated
        assert_eq!(sql("  *  ")? , "*");
        assert_eq!(sql("  self  .  *  ")? , "`self`.*");
        assert_eq!(sql(" self . id : the_id ")? , "`self`.`id` AS `the_id`");
        Ok(())
    }

    #[test]
    fn too_many_dots_is_error() {
        assert!(Selector::from_str("a.b.c").is_err());
        assert!(Selector::from_str("a.b.c:alias").is_err());
    }

    #[test]
    fn invalid_identifiers_are_rejected() {
        // depends on validate_ident(), but these should be obviously invalid
        assert!(Selector::from_str("bad name").is_err());          // space
        assert!(Selector::from_str("self.bad name").is_err());     // space
        assert!(Selector::from_str("self.id:bad alias").is_err()); // space in alias
        assert!(Selector::from_str("sel;ect.id").is_err());        // punctuation
        assert!(Selector::from_str("self.id:`oops`").is_err());    // backticks in alias
    }

    #[test]
    fn builder_chain_roundtrip() -> anyhow::Result<()> {
        let sel = Selector::new("id").set_source("self").set_alias("x");
        assert_eq!(sel.to_sql()?, "`self`.`id` AS `x`");
        Ok(())
    }

    #[test]
    fn display_formats_like_sql() -> anyhow::Result<()> {
        let sel = Selector::from_str("self.id:the_id")?;
        let sql = sel.to_sql()?;
        let mut s = String::new();
        write!(&mut s, "{sel}").unwrap();
        assert_eq!(s, sql);
        Ok(())
    }

    #[test]
    fn tryfrom_impls() -> anyhow::Result<()> {
        let s: Selector = "self.code:alias".try_into()?;
        assert_eq!(s.to_sql()?, "`self`.`code` AS `alias`");

        let s2: Selector = String::from("self.*").try_into()?;
        assert_eq!(s2.to_sql()?, "`self`.*");
        Ok(())
    }
}
