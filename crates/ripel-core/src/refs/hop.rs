use anyhow::Result;


#[derive(Debug, Clone)]
pub struct Hop<'a> {
    pub table: &'a str,
    pub lhs:   &'a str,
    pub rhs:   &'a str,
}

impl<'a> Hop<'a> {
    pub fn table(&self) -> &str { self.table }
    pub fn lhs(&self) -> &str { self.lhs }
    pub fn rhs(&self) -> &str { self.rhs }
    pub fn from_str(s: &'a str) -> Result<Self> {
        let (table, lhs, rhs) = parse_hop_literal(s)?;
        Ok(Self { table, lhs, rhs })
    }
}

impl std::fmt::Display for Hop<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({}={})", self.table, self.lhs, self.rhs)
    }
}

pub fn parse_hop_literal(s: &str) -> Result<(&str, &str, &str)> {
    let part = s.trim();
    let (table, rest) = part.split_once('(')
        .ok_or_else(|| anyhow::anyhow!("invalid hop segment `{part}`: missing '('"))?;
    let rest = rest.strip_suffix(')')
        .ok_or_else(|| anyhow::anyhow!("invalid hop segment `{part}`: missing ')'"))?;
    let (lhs, rhs) = rest.split_once('=')
        .ok_or_else(|| anyhow::anyhow!("invalid predicate `{rest}`: expected `lhs=rhs`"))?;
    let table = table.trim();
    let lhs   = lhs.trim();
    let rhs   = rhs.trim();
    if lhs.is_empty() || rhs.is_empty() {
        return Err(anyhow::anyhow!("empty lhs/rhs in hop segment `{part}`"));
    }
    if table.is_empty() {
        return Err(anyhow::anyhow!("empty table name in hop segment `{part}`"));
    }

    Ok((table, lhs, rhs))
}
