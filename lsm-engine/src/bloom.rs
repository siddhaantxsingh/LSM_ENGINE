//! Bloom filter, one per SSTable (built over all user keys in the table).
//!
//! k hash functions are simulated with double hashing (Kirsch–Mitzenmacher):
//! h_i = h1 + i*h2. With 10 bits/key and k = 7 the false-positive rate is
//! ~0.8%, so a Get for an absent key skips ~99% of table reads.

pub struct BloomBuilder {
    bits_per_key: usize,
    hashes: Vec<u32>,
}

fn hash(key: &[u8]) -> u32 {
    // 32-bit FNV-1a followed by a murmur-style finaliser for better mixing.
    let mut h: u32 = 0x811c9dc5;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^ (h >> 16)
}

impl BloomBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        BloomBuilder {
            bits_per_key,
            hashes: Vec::new(),
        }
    }

    pub fn add(&mut self, user_key: &[u8]) {
        self.hashes.push(hash(user_key));
    }

    /// Output: bit array followed by one byte holding k.
    pub fn finish(&self) -> Vec<u8> {
        let k = ((self.bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u8;
        let bits = (self.hashes.len() * self.bits_per_key).max(64);
        let bytes = bits.div_ceil(8);
        let bits = bytes * 8;
        let mut out = vec![0u8; bytes + 1];
        for &h0 in &self.hashes {
            let mut h = h0;
            let delta = h.rotate_left(15);
            for _ in 0..k {
                let pos = (h as usize) % bits;
                out[pos / 8] |= 1 << (pos % 8);
                h = h.wrapping_add(delta);
            }
        }
        out[bytes] = k;
        out
    }
}

pub fn may_contain(filter: &[u8], user_key: &[u8]) -> bool {
    if filter.len() < 2 {
        return true;
    }
    let k = filter[filter.len() - 1];
    let bits = (filter.len() - 1) * 8;
    let mut h = hash(user_key);
    let delta = h.rotate_left(15);
    for _ in 0..k {
        let pos = (h as usize) % bits;
        if filter[pos / 8] & (1 << (pos % 8)) == 0 {
            return false;
        }
        h = h.wrapping_add(delta);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_fp_rate() {
        let mut b = BloomBuilder::new(10);
        for i in 0..10_000u32 {
            b.add(&i.to_le_bytes());
        }
        let f = b.finish();
        for i in 0..10_000u32 {
            assert!(may_contain(&f, &i.to_le_bytes()));
        }
        let fp = (10_000..110_000u32)
            .filter(|i| may_contain(&f, &i.to_le_bytes()))
            .count();
        let rate = fp as f64 / 100_000.0;
        assert!(rate < 0.02, "false positive rate {rate}");
    }
}
