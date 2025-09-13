use anyhow::{bail, Result};

// TIME binary → ISO 8601 duration formatter (does NOT require the time crate itself)
pub(crate) fn mysql_time_bin_to_iso8601(buf: &[u8]) -> Result<String> {
    if buf.is_empty() {
        bail!("empty TIME payload");
    }
    let len = buf[0] as usize;
    if len == 0 {
        return Ok("PT0S".to_string());
    }
    if buf.len() != 1 + len || !(len == 8 || len == 12) {
        bail!("unexpected TIME payload length: {}", buf.len());
    }

    let is_neg = buf[1] != 0;
    let days = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]);
    let hour = buf[6] as u64;
    let minute = buf[7] as u64;
    let second = buf[8] as u64;
    let micros = if len == 12 {
        u32::from_le_bytes([buf[9], buf[10], buf[11], buf[12]])
    } else {
        0
    };

    let total_hours = hour + (days as u64) * 24;

    use core::fmt::Write;
    let mut out = String::new();
    if is_neg {
        out.push('-');
    }
    out.push_str("PT");
    if total_hours > 0 {
        let _ = write!(out, "{}H", total_hours);
    }
    if minute > 0 || total_hours > 0 {
        let _ = write!(out, "{}M", minute);
    }
    if micros > 0 {
        let mut frac = format!("{:06}", micros);
        while frac.ends_with('0') {
            frac.pop();
        }
        let _ = write!(out, "{}.{}S", second, frac);
    } else {
        let _ = write!(out, "{}S", second);
    }

    Ok(out)
}

#[allow(dead_code)]
pub(crate) fn canonicalize_decimal_string_in_place(s: &mut String) {
    if s.find('.').is_some() {
        // trim trailing zeros after decimal point
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        // keep "-0" as "0"
        if s == "-0" {
            s.clear();
            s.push('0');
        }
    }
}
