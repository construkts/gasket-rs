use std::time::Duration;

use tracing::info;

use crate::runtime::{StagePhase, Tether, TetherState};

pub struct Daemon(pub Vec<Tether>);

impl Daemon {
    fn should_stop(&self) -> bool {
        self.0.iter().any(|tether| match tether.check_state() {
            TetherState::Alive(p) => {
                matches!(p, StagePhase::Ended)
            }
            _ => true,
        })
    }

    fn teardown(&self) {
        for tether in self.0.iter() {
            let state = tether.check_state();
            info!(stage = tether.name(), ?state, "dismissing stage");
            tether.dismiss_stage().expect("stage stops");

            // Can't join the stage because there's a risk of deadlock, usually
            // because a stage gets stuck sending into a port which depends on a
            // different stage not yet dismissed. The solution is to either
            // create a DAG of dependencies and dismiss in the
            // correct order, or implement a 2-phase teardown where
            // ports are disconnected and flushed before joining the
            // stage.

            //tether.join_stage();
        }
    }

    pub fn block(&self) {
        while !self.should_stop() {
            std::thread::sleep(Duration::from_millis(1500));
        }

        self.teardown();
    }
}
