//! Isolated Source qualification. The callback never sends application heartbeats.

use durable_workflow::{json, Client, Value, Worker};
use std::{fs, path::PathBuf, time::Duration};

struct CallbackStop {
    directory: PathBuf,
    grant: String,
}

impl Drop for CallbackStop {
    fn drop(&mut self) {
        fs::write(self.directory.join("rust-stopped.json"), &self.grant)
            .expect("retain actual callback future drop");
    }
}

#[tokio::main]
async fn main() -> durable_workflow::Result<()> {
    let queue = std::env::args().nth(1).expect("isolated task queue");
    let directory = PathBuf::from(std::env::var("DW_CASCADE_DIRECTORY").unwrap());
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .namespace("default")
        .build()?;
    let mut worker = Worker::new(client, &queue)
        .worker_id(format!("{queue}-rust"))
        .cooperative_cancellation(true)
        .max_concurrent_activity_tasks(1)
        .poll_timeout(Duration::from_secs(1));
    worker.register_activity("tests.polyglot-cancellation-remote", move |ctx, _| {
        let directory = directory.clone();
        async move {
            let grant = json!({
                "task_id": ctx.task_id,
                "activity_attempt_id": ctx.activity_attempt_id,
                "lease_owner": ctx.lease_owner,
                "attempt_number": ctx.attempt_number,
                "worker_pid": std::process::id(),
            })
            .to_string();
            let _stop = CallbackStop {
                directory: directory.clone(),
                grant: grant.clone(),
            };
            fs::write(directory.join("rust-entered.json"), grant).expect("retain callback entry");
            std::future::pending::<durable_workflow::Result<Value>>().await
        }
    });
    let mut shutdown = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install the qualification SIGTERM handler");
    worker
        .run_until(async {
            shutdown.recv().await;
        })
        .await
}
