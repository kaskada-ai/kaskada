use error_stack::{IntoReportCompat, ResultExt};
use futures::{StreamExt, TryStreamExt};
use tracing::{debug, info, info_span};

use crate::data_manager::DataManager;
use crate::execute::spawner::ComputeTaskSpawner;
use crate::execute::Error;
use crate::BatchSender;

/// Spawn a task to pre-fetch all of the data files in the manager.
///
/// The output channel sender is used to determine whether the query is still
/// processing.
pub(super) fn spawn_prefetch(
    data_manager: &DataManager,
    spawner: &mut ComputeTaskSpawner,
    output_sender: BatchSender,
) {
    let mut all_files: Vec<_> = data_manager
        .handles()
        .filter(|handle| !handle.is_ready())
        .collect();

    if all_files.is_empty() {
        info!("No files to pre-fetch");
        return;
    }

    // Sort the files by minimum time. This is a proxy for how early they'll be
    // needed so we can prefetch the files needed earlier first.
    all_files.sort_by(|a, b| a.cmp_time(b));

    spawner.spawn("prefetch".to_owned(), info_span!("Prefetch"), async move {
        info!("Pre-fetching {} files", all_files.len());

        // TODO: This will attempt to prefetch all of the files. Even if we have
        // run with preview rows and have reached the point where we shut things
        // down. We should probably introduce a more explicit "shut things down"
        // signal, that could be used to abort the prefetch.
        let mut all_files = futures::stream::iter(all_files.into_iter())
            .map(|handle| handle.prefetch())
            // TODO: Allow configuring the pre-fetch parallelism. Currently, we
            // have `RuntimeOptions` but that is per-query, not per service.
            // Service options could maybe be plumbed through?
            .buffer_unordered(4);

        loop {
            tokio::select! {
                next = all_files.try_next() => {
                    match next.into_report().change_context(Error::internal())? {
                        Some(next_file) => debug!("Pre-fetched file {:?}", next_file),
                        None => {
                            info!("Pre-fetch completed");
                            break;
                        },
                    }
                }
                _ = output_sender.closed() => {
                    info!("Output channel closed; aborting pre-fetch");
                    break;
                }
            }
        }

        Ok(())
    });
}
