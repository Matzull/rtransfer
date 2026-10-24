//! Per-chunk compression helpers shared by both roles.
//!
//! Chunks sit inside `Msg::Data` frames; the codec is per-chunk so
//! decompression order is independent (perfect for seq-reordered applies
//! arriving over multiple links).

use std::io;

use crate::protocol::Comp;

/// Compress one chunk. Falls back to `Comp::None` when the data does not
/// compress (the wire cost of the zstd frame dominates then).
pub fn encode(buf: &[u8], comp: Comp) -> io::Result<(Comp, Vec<u8>)> {
    match comp {
        Comp::None => Ok((Comp::None, buf.to_vec())),
        Comp::Zstd(level) => {
            let out = zstd::bulk::compress(buf, level as i32)?;
            if out.len() < buf.len() && !buf.is_empty() {
                Ok((comp, out))
            } else {
                Ok((Comp::None, buf.to_vec()))
            }
        }
    }
}

/// Decompress one chunk.
pub fn decode(comp: Comp, orig_len: u64, data: Vec<u8>) -> io::Result<Vec<u8>> {
    match comp {
        Comp::None => {
            if data.len() as u64 != orig_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk size mismatch: {} != {orig_len}", data.len()),
                ));
            }
            Ok(data)
        }
        Comp::Zstd(level) => {
            if !(1..=22).contains(&level) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad zstd level"));
            }
            let out = zstd::bulk::decompress(&data, orig_len as usize)?;
            if out.len() as u64 != orig_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("decompressed size mismatch: {} != {orig_len}", out.len()),
                ));
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fall_back_when_not_compressible() {
        // Random bytes (not a repeated pattern!) defeat zstd.
        let mut data = vec![0u8; 8192];
        crate::delta::fill_random(&mut data, 11);
        let (comp, out) = encode(&data, Comp::Zstd(9)).unwrap();
        assert_eq!(comp, Comp::None);
        assert_eq!(out, data);
    }

    #[test]
    fn roundtrip() {
        let data = vec![b'x'; 96 * 1024];
        for level in [1i8, 3, 9, 16] {
            let (comp, out) = encode(&data, Comp::Zstd(level)).unwrap();
            let back = decode(comp, data.len() as u64, out).unwrap();
            assert_eq!(back, data);
        }
    }
}
