//! Byte classes from RFC 9110 and RFC 3986.

/// `tchar`: the bytes of a method or a header name.
pub(crate) const fn is_tchar(b: u8) -> bool {
    matches!(b,
        b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.'
        | b'^' | b'_' | b'`' | b'|' | b'~'
        | b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z')
}

/// A header value byte: visible ASCII, space, tab or obs-text; never CR, LF or NUL.
pub(crate) const fn is_value_byte(b: u8) -> bool {
    b == b'\t' || (b >= 0x20 && b != 0x7f)
}

/// Visible ASCII plus space and tab, the bytes `HeaderValue::to_str` accepts.
pub(crate) const fn is_visible_ascii(b: u8) -> bool {
    b == b'\t' || (b >= 0x20 && b < 0x7f)
}
