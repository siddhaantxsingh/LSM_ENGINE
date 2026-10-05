//! Little-endian fixed-width and LEB128 varint encoding helpers.

use crate::error::{Result, corruption};

pub fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn get_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}

pub fn get_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Decodes a varint, returning (value, bytes consumed).
pub fn get_varint(b: &[u8]) -> Result<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0;
    for (i, &byte) in b.iter().enumerate() {
        if shift > 63 {
            break;
        }
        v |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok((v, i + 1));
        }
        shift += 7;
    }
    corruption("bad varint")
}

pub fn put_length_prefixed(buf: &mut Vec<u8>, s: &[u8]) {
    put_varint(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

/// Reads a length-prefixed slice; returns (slice, bytes consumed).
pub fn get_length_prefixed(b: &[u8]) -> Result<(&[u8], usize)> {
    let (len, n) = get_varint(b)?;
    let end = n + len as usize;
    if end > b.len() {
        return corruption("truncated length-prefixed slice");
    }
    Ok((&b[n..end], end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 1 << 35, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            assert_eq!(get_varint(&b).unwrap(), (v, b.len()));
        }
    }
}
