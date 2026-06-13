use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tracing::{debug, info, warn};

use crate::runtime::{EndCause, StagePhase, Tether, TetherState};

/// Why the daemon's stop condition triggered.
///
/// Computed from the state of the tethers *before* teardown, so a graceful
/// finalization can be told apart from a crashed or stalled stage (eg: to
/// decide the process exit code).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// the process received an OS termination signal
    Terminated,
    /// a stage finalized its work gracefully
    Finalized,
    /// a stage was dismissed from the outside
    Dismissed { stage: String },
    /// a stage ended (worker error or thread death) without finalizing
    Crashed { stage: String },
    /// a stage stopped responding (tick timeout exceeded)
    Blocked { stage: String },
}

impl StopReason {
    /// Whether the pipeline stopped by finalizing its work or by explicit
    /// request, as opposed to crashing or stalling.
    pub fn is_graceful(&self) -> bool {
        matches!(
            self,
            StopReason::Terminated | StopReason::Finalized | StopReason::Dismissed { .. }
        )
    }

    /// Which reason best explains the stop when several stages report one at
    /// the same instant. A graceful finalization outranks a crash (a finished
    /// producer routinely makes its consumers error as a cascade), a crash
    /// outranks a stall, and a stall outranks an external dismissal.
    fn precedence(&self) -> u8 {
        match self {
            StopReason::Terminated => 5,
            StopReason::Finalized => 4,
            StopReason::Crashed { .. } => 3,
            StopReason::Blocked { .. } => 2,
            StopReason::Dismissed { .. } => 1,
        }
    }

    /// Keep whichever of two reasons better explains the stop, favouring the
    /// earlier stage on a tie so the first offender is the one reported.
    fn or_stronger(self, other: Self) -> Self {
        if other.precedence() > self.precedence() {
            other
        } else {
            self
        }
    }
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopReason::Terminated => write!(f, "process received a termination signal"),
            StopReason::Finalized => write!(f, "a stage finalized its work"),
            StopReason::Dismissed { stage } => write!(f, "stage '{stage}' was dismissed"),
            StopReason::Crashed { stage } => write!(f, "stage '{stage}' ended before finalizing"),
            StopReason::Blocked { stage } => write!(f, "stage '{stage}' stopped responding"),
        }
    }
}

#[derive(Debug)]
pub struct Daemon(Vec<Tether>, Arc<AtomicBool>);

impl Daemon {
    pub fn new(tethers: Vec<Tether>) -> Self {
        let term = Arc::new(AtomicBool::new(false));

        for sig in signal_hook::consts::TERM_SIGNALS {
            signal_hook::flag::register(*sig, Arc::clone(&term))
                .expect("can't register OS signal hook");
        }

        Self(tethers, term)
    }

    pub fn tethers(&self) -> impl Iterator<Item = &Tether> {
        self.0.iter()
    }

    pub fn is_terminated(&self) -> bool {
        self.1.load(Ordering::Relaxed)
    }

    pub fn has_ended(&self) -> bool {
        self.0.iter().any(|tether| match tether.check_state() {
            TetherState::Alive(p) => {
                matches!(p, StagePhase::Ended(_))
            }
            _ => true,
        })
    }

    /// Why the daemon should stop, if it should at all.
    ///
    /// Each running stage either contributes a reason or none; the daemon
    /// reports the one that best explains the stop (see
    /// [`StopReason::precedence`]).
    pub fn stop_reason(&self) -> Option<StopReason> {
        if self.is_terminated() {
            return Some(StopReason::Terminated);
        }

        self.tethers()
            .filter_map(Self::tether_stop_reason)
            .reduce(StopReason::or_stronger)
    }

    /// How a single stage contributes to the stop decision, if at all. The
    /// terminal `Ended` phase carries the `EndCause` it ended with, so the
    /// reason reads straight off the tether's phase.
    fn tether_stop_reason(tether: &Tether) -> Option<StopReason> {
        let stage = || tether.name().to_owned();

        match tether.check_state() {
            // the stage reached its terminal phase; the cause it ended with
            // names the reason
            TetherState::Alive(StagePhase::Ended(cause))
            | TetherState::Finished(StagePhase::Ended(cause)) => Some(match cause {
                EndCause::Done => StopReason::Finalized,
                EndCause::Errored => StopReason::Crashed { stage: stage() },
                EndCause::Dismissed => StopReason::Dismissed { stage: stage() },
            }),

            // the thread is gone but never reached `Ended`, so it panicked
            // mid-work
            TetherState::Dropped | TetherState::Finished(_) => {
                Some(StopReason::Crashed { stage: stage() })
            }

            // the stage stopped ticking within its timeout
            TetherState::Blocked(_) => Some(StopReason::Blocked { stage: stage() }),

            // still working
            TetherState::Alive(_) => None,
        }
    }

    pub fn should_stop(&self) -> bool {
        match self.stop_reason() {
            Some(reason) => {
                warn!(%reason, "daemon should stop");
                true
            }
            None => false,
        }
    }

    pub fn teardown(self) {
        // first pass is to notify that we should stop
        for tether in self.0.iter() {
            let state = tether.check_state();
            info!(stage = tether.name(), ?state, "dismissing stage");

            match tether.dismiss_stage() {
                Ok(_) => (),
                Err(crate::error::Error::TetherDropped) => debug!("stage already dismissed"),
                error => warn!(?error, "couldn't dismiss stage"),
            }
        }

        // second pass is to wait for graceful shutdown
        info!("waiting for stages to end");
        for tether in self.0.into_iter() {
            tether.join_stage();
        }
    }

    pub fn block(self) -> StopReason {
        let reason = loop {
            if let Some(reason) = self.stop_reason() {
                break reason;
            }

            std::thread::sleep(Duration::from_millis(1500));
        };

        warn!(%reason, "stopping daemon");

        self.teardown();

        reason
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{spawn_stage, tests::MockStage, Policy};

    fn wait_stop_reason(daemon: &Daemon) -> StopReason {
        for _ in 0..300 {
            if let Some(reason) = daemon.stop_reason() {
                return reason;
            }

            std::thread::sleep(Duration::from_millis(10));
        }

        panic!("daemon never reached a stop condition");
    }

    fn long_running_stage() -> MockStage {
        MockStage {
            schedule_delay: Some(Duration::from_secs(60)),
            ..Default::default()
        }
    }

    #[test]
    fn stop_reason_reports_finalized_when_a_worker_completes() {
        let finalizer = MockStage {
            done_after: Some(3),
            ..Default::default()
        };

        let daemon = Daemon::new(vec![
            spawn_stage(long_running_stage(), Policy::default()),
            spawn_stage(finalizer, Policy::default()),
        ]);

        let reason = wait_stop_reason(&daemon);
        assert_eq!(reason, StopReason::Finalized);
        assert!(reason.is_graceful());

        daemon.teardown();
    }

    #[test]
    fn stop_reason_reports_crashed_when_a_worker_errors() {
        let failing = MockStage {
            failures: vec![true],
            ..Default::default()
        };

        let daemon = Daemon::new(vec![
            spawn_stage(long_running_stage(), Policy::default()),
            spawn_stage(failing, Policy::default()),
        ]);

        let reason = wait_stop_reason(&daemon);

        assert_eq!(
            reason,
            StopReason::Crashed {
                stage: "mockstage".into()
            }
        );

        assert!(!reason.is_graceful());

        daemon.teardown();
    }
}
