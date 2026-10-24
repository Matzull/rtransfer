//! Machine capability the tuner needs before a session starts.
//!
//! Only the core count: CPU-dependent choices (compression) are made at
//! runtime from measured throughput instead (see [`crate::compctl`]), which
//! also saves the per-session benchmark that used to cost a slow machine
//! like a small ARM board a second or two of CPU.

/// Capability measurements used by [`crate::tuner`].
#[derive(Debug, Clone, Copy)]
pub struct CpuBench {
    pub cores: u32,
}

fn available_cores() -> u32 {
    std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1)
}

/// This machine's capabilities (instant: nothing is benchmarked).
pub fn cores_only() -> CpuBench {
    CpuBench { cores: available_cores() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_at_least_one_core() {
        assert!(cores_only().cores >= 1);
    }
}
