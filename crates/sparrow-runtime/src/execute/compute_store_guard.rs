use std::sync::Arc;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use prost_wkt_types::Timestamp;
use sparrow_api::kaskada::v1alpha::{ComputeSnapshot, ComputeSnapshotConfig, PlanHash};
use sparrow_instructions::ComputeStore;
use tempfile::TempDir;
use tracing::Instrument;

use crate::execute::error::Error;
use crate::execute::{checkpoints, ComputeResult};
use crate::stores::ObjectStoreRegistry;

pub(super) struct ComputeStoreGuard {
    dir: TempDir,
    store: Arc<ComputeStore>,
    config: ComputeSnapshotConfig,
}

// The path prefix to the local compute store db.
const STORE_PATH_PREFIX: &str = "compute_snapshot_";

impl ComputeStoreGuard {
    pub async fn try_new(
        config: ComputeSnapshotConfig,
        object_stores: &ObjectStoreRegistry,
        max_allowed_max_event_time: Timestamp,
        plan_hash: &PlanHash,
    ) -> error_stack::Result<Self, Error> {
        let dir = tempfile::Builder::new()
            .prefix(&STORE_PATH_PREFIX)
            .tempdir()
            .into_report()
            .change_context(Error::internal_msg("create snapshot dir"))?;

        // If a `resume_from` path is specified, download the existing state from s3.
        if let Some(resume_from) = &config.resume_from {
            checkpoints::download(resume_from, object_stores, dir.path(), &config)
                .instrument(tracing::info_span!("Downloading checkpoint files"))
                .await
                .change_context(Error::internal_msg("download snapshot"))?;
        } else {
            tracing::info!("No snapshot set to resume from. Using empty compute store.");
        }

        let store = ComputeStore::try_new(dir.path(), &max_allowed_max_event_time, plan_hash)
            .into_report()
            .change_context(Error::internal_msg("loading compute store"))?;
        Ok(Self { dir, store, config })
    }

    pub async fn finish(
        self,
        object_stores: &ObjectStoreRegistry,
        compute_result: ComputeResult,
    ) -> error_stack::Result<ComputeSnapshot, Error> {
        // Write the max input time to the store.
        self.store
            .put_max_event_time(&compute_result.max_input_timestamp)
            .into_report()
            .change_context(Error::Internal("failed to report max event time"))?;

        // Now that everything has completed, we attempt to get the compute store out.
        // This lets us explicitly drop the store here.
        match Arc::try_unwrap(self.store) {
            Ok(owned_compute_store) => std::mem::drop(owned_compute_store),
            Err(_) => panic!("unable to reclaim compute store"),
        };

        super::checkpoints::upload(object_stores, self.dir, self.config, compute_result)
            .await
            .change_context(Error::Internal("uploading snapshot"))
    }

    pub fn store(&self) -> Arc<ComputeStore> {
        self.store.clone()
    }

    pub fn store_ref(&self) -> &ComputeStore {
        self.store.as_ref()
    }
}
