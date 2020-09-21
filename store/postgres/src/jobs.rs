//! Jobs for database maintenance
use diesel::dsl::{exists, not};
use diesel::prelude::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl, RunQueryDsl,
};
use diesel::{connection::SimpleConnection, sql_query, sql_types::Text, Connection, PgConnection};

use std::time::{Duration, Instant};
use std::{collections::HashSet, sync::Arc};

use graph::prelude::{bigdecimal::ToPrimitive, error, info, Logger, SubgraphDeploymentId};
use graph::util::jobs::{Job, Runner};
use graph::{components::store::StoreError, prelude::BigDecimal};

use crate::entities::{find_schema, Schema as DeploymentSchema};
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

struct RemoveDeploymentsJob {
    store: Arc<Store>,
}

impl RemoveDeploymentsJob {
    fn next_removable_deployment(
        conn: &PgConnection,
    ) -> Result<Option<DeploymentSchema>, StoreError> {
        use crate::entities::public::deployment_schemas as ds;
        use crate::entities::public::DeploymentSchemaVersion as DSV;
        use crate::metadata::subgraph as s;
        use crate::metadata::subgraph_deployment as d;
        use crate::metadata::subgraph_deployment_assignment as a;
        use crate::metadata::subgraph_version as v;

        let sid = d::table
            .inner_join(ds::table.on(d::id.eq(ds::subgraph)))
            .select(d::id)
            // The deployment must use relational storage
            .filter(ds::version.eq(DSV::Relational))
            // The deployment must not be assigned
            .filter(not(exists(a::table.select(a::id).filter(a::id.eq(d::id)))))
            // The deployment must not be the current or pending version
            // of any subgraph
            .filter(not(exists(
                s::table
                    .inner_join(v::table.on(s::id.eq(v::subgraph)))
                    .select(s::id)
                    .filter(d::id.eq(v::deployment))
                    .filter(
                        s::current_version
                            .eq(v::id.nullable())
                            .or(s::pending_version.eq(v::id.nullable())),
                    ),
            )))
            .first::<String>(conn)
            .optional()?;
        if let Some(sid) = sid {
            let deployment =
                SubgraphDeploymentId::new(sid).expect("Deployment ids in the database are valid");
            find_schema(conn, &deployment)
        } else {
            Ok(None)
        }
    }

    // Remove any versions pointing at the deployment and return the
    // earliest `created_at` and a comma-separated list of subgraphs that used
    // the deployment
    fn remove_versions(
        conn: &PgConnection,
        deployment: &DeploymentSchema,
    ) -> Result<(Option<i32>, String), StoreError> {
        use crate::metadata::subgraph as s;
        use crate::metadata::subgraph_version as v;
        let removed = diesel::delete(v::table)
            .filter(v::deployment.eq(&deployment.subgraph))
            .returning((v::created_at, v::subgraph))
            .load::<(BigDecimal, String)>(conn)?;
        let created_at = removed
            .iter()
            .map(|(ts, _)| ts.to_i32())
            .filter_map(|ts| ts)
            .min();
        let subgraphs = removed
            .iter()
            .map(|(_, subgraph)| subgraph)
            .collect::<HashSet<_>>();
        let subgraphs = s::table
            .select(s::name)
            .filter(s::id.eq_any(subgraphs))
            .load::<String>(conn)?
            .join(",");
        Ok((created_at, subgraphs))
    }

    // Record that we removed the subgraph in `removed_deployments`
    fn record_removal(
        conn: &PgConnection,
        deployment: &DeploymentSchema,
        created_at: Option<i32>,
        subgraphs: String,
    ) -> Result<Option<i32>, StoreError> {
        use crate::metadata::removed_deployments as rd;
        use crate::metadata::subgraph_deployment as d;
        use crate::metadata::subgraph_sizes as sz;

        let (entity_count, latest_block_number) = d::table
            .select((d::entity_count, d::latest_ethereum_block_number))
            .filter(d::id.eq(&deployment.subgraph))
            .get_result::<(BigDecimal, Option<BigDecimal>)>(conn)?;
        let entity_count = entity_count.to_i32();
        let latest_block_number = latest_block_number.and_then(|l| l.to_i32());

        let sizes = sz::table
            .select((
                sz::row_estimate,
                sz::total_bytes,
                sz::index_bytes,
                sz::toast_bytes,
                sz::table_bytes,
            ))
            .filter(sz::subgraph.eq(&deployment.subgraph))
            .get_result::<(f32, BigDecimal, BigDecimal, BigDecimal, BigDecimal)>(conn)
            .optional()?;
        let (row_count, total_bytes, index_bytes, toast_bytes, table_bytes) = match sizes {
            Some((row_count, total_bytes, index_bytes, toast_bytes, table_bytes)) => (
                Some(row_count),
                Some(total_bytes),
                Some(index_bytes),
                Some(toast_bytes),
                Some(table_bytes),
            ),
            None => (None, None, None, None, None),
        };
        let row_count = row_count.map(|f| BigDecimal::from(f.floor() as f64));

        diesel::insert_into(rd::table)
            .values((
                rd::deployment.eq(&deployment.subgraph),
                rd::schema_name.eq(&deployment.name),
                rd::created_at.eq(created_at),
                rd::subgraphs.eq(subgraphs),
                rd::entity_count.eq(entity_count),
                rd::latest_ethereum_block_number.eq(latest_block_number),
                rd::row_count.eq(row_count),
                rd::total_bytes.eq(total_bytes),
                rd::index_bytes.eq(index_bytes),
                rd::toast_bytes.eq(toast_bytes),
                rd::table_bytes.eq(table_bytes),
            ))
            .execute(conn)?;
        Ok(entity_count)
    }

    fn remove_deployment(
        conn: &PgConnection,
        deployment: &DeploymentSchema,
    ) -> Result<(i32, i32), StoreError> {
        use crate::entities::public::deployment_schemas as ds;

        // The query in this file was generated by running 'make'
        // in the 'sql/' subdirectory
        // See also: ed42d219c6704a4aab57ce1ea66698e7
        // The query must be regenerated when the GraphQL schema changes
        const REMOVE_METADATA_QUERY: &str = include_str!("./sql/remove_metadata.sql");

        #[derive(QueryableByName)]
        struct MetadataCount {
            #[sql_type = "diesel::sql_types::Integer"]
            metadata_count: i32,
        }

        // Remove metadata
        let metadata_count: i32 = sql_query(REMOVE_METADATA_QUERY)
            .bind::<Text, _>(&deployment.subgraph)
            .get_result::<MetadataCount>(conn)?
            .metadata_count;

        // Drop the schema
        conn.batch_execute(&format!("drop schema {} cascade", deployment.name))?;

        // Remove from deployment_schemas
        diesel::delete(ds::table)
            .filter(ds::name.eq(&deployment.name))
            .execute(conn)?;

        // Remove any subgraph versions referring to this deployment
        let (created_at, subgraphs) = Self::remove_versions(conn, deployment)?;

        let entity_count = Self::record_removal(conn, deployment, created_at, subgraphs)?;

        Ok((entity_count.unwrap_or(0), metadata_count))
    }

    fn removal_loop(&self, logger: &Logger) -> Result<(), StoreError> {
        // To avoid holding up overall job execution, do this for no
        // more than 5 minutes
        const TIME_LIMIT: Duration = Duration::from_secs(300);

        let conn = self.store.get_conn()?;
        let start = Instant::now();

        while let Some(deployment) = Self::next_removable_deployment(&conn)? {
            info!(logger, "Remove unused deployment"; "deployment" => &deployment.subgraph);
            let (entity_count, metadata_count) =
                conn.transaction(|| Self::remove_deployment(&conn, &deployment))?;
            info!(logger, "Removed unused deployment";
                    "deployment" => &deployment.subgraph,
                    "schema" => &deployment.name,
                    "entity_count" => entity_count,
                    "metadata_count" => metadata_count);

            if start.elapsed() > TIME_LIMIT {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl Job for RemoveDeploymentsJob {
    fn name(&self) -> &str {
        "Remove unused deployments"
    }

    fn run(&self, logger: &Logger) {
        if let Err(e) = self.removal_loop(logger) {
            error!(logger, "Job `{}` failed: {}", self.name(), e);
        }
    }
}
