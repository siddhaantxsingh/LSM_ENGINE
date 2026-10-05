//! Internal keys: `user_key ++ trailer`, where the 8-byte little-endian
//! trailer packs `(sequence << 8) | kind`.
//!
//! Ordering: user key ascending, then sequence **descending**, so the newest
//! version of a key sorts first and a seek to `(key, snapshot_seq)` lands on
//! the newest version visible to that snapshot.

use std::cmp::Ordering;

pub type SeqNum = u64;
pub const MAX_SEQ: SeqNum = (1 << 56) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Delete = 0,
    Put = 1,
}

impl Kind {
    pub fn from_u8(b: u8) -> Option<Kind> {
        match b {
            0 => Some(Kind::Delete),
            1 => Some(Kind::Put),
            _ => None,
        }
    }
}

/// Kind used when seeking: Put (1) > Delete (0), and since higher trailers
/// sort first, seeking with Put lands before any entry with the same seq.
pub const SEEK_KIND: Kind = Kind::Put;

pub fn make_internal_key(user_key: &[u8], seq: SeqNum, kind: Kind) -> Vec<u8> {
    let mut k = Vec::with_capacity(user_key.len() + 8);
    k.extend_from_slice(user_key);
    k.extend_from_slice(&((seq << 8) | kind as u64).to_le_bytes());
    k
}

pub fn user_key(ikey: &[u8]) -> &[u8] {
    &ikey[..ikey.len() - 8]
}

pub fn trailer(ikey: &[u8]) -> u64 {
    u64::from_le_bytes(ikey[ikey.len() - 8..].try_into().unwrap())
}

pub fn seq(ikey: &[u8]) -> SeqNum {
    trailer(ikey) >> 8
}

pub fn kind(ikey: &[u8]) -> Kind {
    Kind::from_u8((trailer(ikey) & 0xff) as u8).unwrap_or(Kind::Put)
}

pub fn cmp_internal(a: &[u8], b: &[u8]) -> Ordering {
    match user_key(a).cmp(user_key(b)) {
        Ordering::Equal => trailer(b).cmp(&trailer(a)), // descending trailer
        o => o,
    }
}

/// Owned internal key with the internal ordering, for use in BTreeMap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalKey(pub Vec<u8>);

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_internal(&self.0, &other.0)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering() {
        let a1 = make_internal_key(b"a", 1, Kind::Put);
        let a2 = make_internal_key(b"a", 2, Kind::Put);
        let ab = make_internal_key(b"ab", 1, Kind::Put);
        assert_eq!(cmp_internal(&a2, &a1), Ordering::Less, "newer first");
        assert_eq!(cmp_internal(&a1, &ab), Ordering::Less, "prefix key first");
        assert_eq!(user_key(&ab), b"ab");
        assert_eq!(seq(&a2), 2);
        assert_eq!(kind(&a2), Kind::Put);
    }
}
