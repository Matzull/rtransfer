//! Runtime compression choice for the sender.
//!
//! Whether zstd pays off depends on which resource is the bottleneck: on a
//! slow link fewer bytes mean a faster transfer, on a fast link (or with a
//! receiver bound by its disk) compressing only burns CPU on both ends. A
//! pre-transfer bandwidth probe guessed this badly (it measured anywhere from
//! 362 to 1645 MiB/s on the same loopback), so instead the sender *measures*:
//! [`Controller`] alternates short trials with compression on and off and
//! keeps whichever delivered more original bytes per second, re-testing the
//! other mode every few seconds as the workload changes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::protocol::Comp;

/// Measurement window of the sampler.
pub const WINDOW: Duration = Duration::from_millis(250);
/// Windows measured per trial of one mode.
const TRIAL_WINDOWS: usize = 2;
/// Windows spent in the chosen mode before re-testing the other one.
const EXPLOIT_WINDOWS: u32 = 16;
/// Required advantage before preferring compression; ties go to "off",
/// which costs less CPU on both ends.
const MARGIN: f64 = 1.10;

/// The compression decision shared by all chunk jobs of a session.
#[derive(Debug)]
pub struct CompCtl {
    level: i8,
    auto: bool,
    on: AtomicBool,
}

impl CompCtl {
    /// The user forced a codec (`--compression none|zstd:<l>`): never change it.
    pub fn fixed(comp: Comp) -> Self {
        match comp {
            Comp::None => CompCtl { level: 0, auto: false, on: AtomicBool::new(false) },
            Comp::Zstd(level) => CompCtl { level, auto: false, on: AtomicBool::new(true) },
        }
    }

    /// Let the [`Controller`] switch zstd at `level` on and off.
    pub fn auto(level: i8) -> Self {
        CompCtl { level, auto: true, on: AtomicBool::new(true) }
    }

    pub fn is_auto(&self) -> bool {
        self.auto
    }

    /// Codec for the next chunk.
    pub fn current(&self) -> Comp {
        if self.on.load(Ordering::Relaxed) {
            Comp::Zstd(self.level)
        } else {
            Comp::None
        }
    }

    fn set_on(&self, on: bool) {
        self.on.store(on, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    /// Measuring `TRIAL_WINDOWS` windows of the current mode.
    Trial,
    /// Running the chosen mode; `left` windows until the next re-test.
    Exploit { left: u32 },
}

/// Pure decision state machine: feed it the bytes delivered per window,
/// it answers whether compression should be on for the next one.
#[derive(Debug)]
pub struct Controller {
    on: bool,
    phase: Phase,
    /// The first window after a switch still drains frames encoded in the
    /// old mode; it is not attributed to either.
    skip: bool,
    samples: Vec<f64>,
    /// Latest measured throughput per mode: `[off, on]`.
    rate: [Option<f64>; 2],
}

impl Default for Controller {
    fn default() -> Self {
        // Start compressed: on slow links the first seconds matter most, and
        // on fast ones the off-trial follows within a second.
        Controller { on: true, phase: Phase::Trial, skip: true, samples: Vec::new(), rate: [None, None] }
    }
}

impl Controller {
    pub fn on(&self) -> bool {
        self.on
    }

    /// Account one window that delivered `bytes`; returns the mode to use next.
    pub fn step(&mut self, bytes: u64) -> bool {
        if bytes == 0 {
            // Idle (waiting for plans, results, a slow disk start...): no signal.
            return self.on;
        }
        if self.skip {
            self.skip = false;
            return self.on;
        }
        let r = bytes as f64;
        let idx = self.on as usize;
        match self.phase {
            Phase::Trial => {
                self.samples.push(r);
                if self.samples.len() < TRIAL_WINDOWS {
                    return self.on;
                }
                self.rate[idx] = Some(self.samples.iter().sum::<f64>() / self.samples.len() as f64);
                self.samples.clear();
                match self.rate[1 - idx] {
                    // The other mode has never been measured: test it now.
                    None => self.switch(!self.on, Phase::Trial),
                    Some(_) => {
                        let best = self.best();
                        self.switch(best, Phase::Exploit { left: EXPLOIT_WINDOWS });
                    }
                }
            }
            Phase::Exploit { left } => {
                // Keep the running mode's estimate fresh (EWMA).
                self.rate[idx] = Some(self.rate[idx].map_or(r, |old| 0.7 * old + 0.3 * r));
                if left <= 1 {
                    self.switch(!self.on, Phase::Trial);
                } else {
                    self.phase = Phase::Exploit { left: left - 1 };
                }
            }
        }
        self.on
    }

    fn best(&self) -> bool {
        match (self.rate[0], self.rate[1]) {
            (Some(off), Some(on)) => on > off * MARGIN,
            _ => self.on,
        }
    }

    fn switch(&mut self, on: bool, phase: Phase) {
        if on != self.on {
            self.skip = true;
        }
        self.on = on;
        self.phase = phase;
    }
}

/// Drive `ctl` from `delivered()` (cumulative original bytes handed to the
/// links) until cancelled or dropped. Fixed codecs need no sampler.
pub fn spawn_sampler(
    ctl: Arc<CompCtl>,
    delivered: impl Fn() -> u64 + Send + 'static,
    cancel: CancellationToken,
) -> Option<AbortOnDropHandle<()>> {
    if !ctl.is_auto() {
        return None;
    }
    Some(AbortOnDropHandle::new(tokio::spawn(async move {
        let mut ctrl = Controller::default();
        ctl.set_on(ctrl.on());
        let mut tick = tokio::time::interval(WINDOW);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut last = delivered();
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = cancel.cancelled() => return,
            }
            let now = delivered();
            let was = ctrl.on();
            let on = ctrl.step(now - last);
            last = now;
            if on != was {
                tracing::debug!(compress = on, "compression switched");
            }
            ctl.set_on(on);
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run the controller against a link that delivers `on_rate` / `off_rate`
    /// bytes per window; returns the fraction of windows spent compressed.
    fn simulate(ctrl: &mut Controller, on_rate: u64, off_rate: u64, windows: usize) -> f64 {
        let mut on_windows = 0;
        for _ in 0..windows {
            let delivered = if ctrl.on() { on_rate } else { off_rate };
            if ctrl.on() {
                on_windows += 1;
            }
            ctrl.step(delivered);
        }
        on_windows as f64 / windows as f64
    }

    #[test]
    fn slow_link_settles_on_compression() {
        let mut c = Controller::default();
        let frac = simulate(&mut c, 130, 100, 200);
        assert!(c.on());
        assert!(frac > 0.8, "compressed only {frac:.2} of the time");
    }

    #[test]
    fn fast_link_settles_uncompressed() {
        let mut c = Controller::default();
        let frac = simulate(&mut c, 50, 100, 200);
        assert!(!c.on());
        assert!(frac < 0.2, "compressed {frac:.2} of the time");
    }

    #[test]
    fn ties_prefer_off() {
        let mut c = Controller::default();
        simulate(&mut c, 104, 100, 200);
        assert!(!c.on());
    }

    #[test]
    fn retests_and_follows_a_workload_change() {
        let mut c = Controller::default();
        simulate(&mut c, 50, 100, 100);
        assert!(!c.on());
        // Now compression wins (e.g. the link got congested): re-tests pick it up.
        simulate(&mut c, 300, 100, 3 * (EXPLOIT_WINDOWS as usize + 2 * TRIAL_WINDOWS + 2));
        assert!(c.on());
    }

    #[test]
    fn idle_windows_carry_no_signal() {
        let mut c = Controller::default();
        for _ in 0..50 {
            assert!(c.step(0));
        }
        assert!(c.on());
    }

    #[test]
    fn fixed_codecs_never_change() {
        assert_eq!(CompCtl::fixed(Comp::None).current(), Comp::None);
        assert_eq!(CompCtl::fixed(Comp::Zstd(7)).current(), Comp::Zstd(7));
        assert!(!CompCtl::fixed(Comp::None).is_auto());
        assert_eq!(CompCtl::auto(3).current(), Comp::Zstd(3));
    }
}
