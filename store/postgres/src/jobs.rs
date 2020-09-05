//! Jobs for database maintenance
use diesel::connection::SimpleConnection;
use std::sync::Arc;
use std::time::Duration;

use graph::components::store::StoreError;
use graph::prelude::{error, Logger};
use graph::util::jobs::{Job, Runner};

use crate::Store;

pub fn register(runner: &mut Runner, store: Arc<Store>) {
    runner.register(
        Box::new(VacuumDeploymentsJob::new(store)),
        Duration::from_secs(60),
    );
}

/// A job that vacuums `subgraphs.subgraph_deployment`. With a large number
/// of subgraphs, the autovacuum daemon might not run often enough to keep
/// this table, which is _very_ write-heavy, from getting bloated. We
/// therefore set up a separate job that vacuums the table once a minute
struct VacuumDeploymentsJob {
    store: Arc<Store>,
}

impl VacuumDeploymentsJob {
    fn new(store: Arc<Store>) -> VacuumDeploymentsJob {
        VacuumDeploymentsJob { store }
    }

    fn vacuum(&self) -> Result<(), StoreError> {
        let conn = self.store.get_conn()?;
        conn.batch_execute("vacuum (analyze) subgraphs.subgraph_deployment")?;
        Ok(())
    }
}

impl Job for VacuumDeploymentsJob {
    fn name(&self) -> &str {
        "Vacuum subgraphs.subgraph_deployment"
    }

    fn run(&self, logger: &Logger) {
        if let Err(e) = self.vacuum() {
            error!(
                logger,
                "Vacuum of subgraphs.subgraph_deployment failed: {}", e
            );
        }
    }
}
