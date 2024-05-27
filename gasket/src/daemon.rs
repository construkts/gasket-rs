use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tracing::{debug, info, warn};

use crate::runtime::{StagePhase, Tether, TetherState};

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
                matches!(p, StagePhase::Ended)
            }
            _ => true,
        })
    }

    pub fn should_stop(&self) -> bool {
        if self.is_terminated() {
            warn!("daemon terminated by user");
            return true;
        }

        if self.has_ended() {
            warn!("pipeline ended or stalled");
            return true;
        }

        false
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

    pub fn block(self) {
        while !self.should_stop() {
            std::thread::sleep(Duration::from_millis(1500));
        }

        self.teardown();
    }
}
