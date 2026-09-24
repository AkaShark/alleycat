use clap::{Args, Subcommand};

use crate::cli;
use crate::daemon::control::Request;
use crate::push::PushStatus;

#[derive(Args, Debug)]
pub struct PushArgs {
    #[command(subcommand)]
    pub cmd: PushCmd,
}

#[derive(Subcommand, Debug)]
pub enum PushCmd {
    /// Show whether push is enabled, the Worker host, subscription count,
    /// outbox depth and the last delivery success / error.
    Status {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(args: PushArgs) -> anyhow::Result<()> {
    match args.cmd {
        PushCmd::Status { json } => {
            let resp = cli::send(Request::PushStatus).await?;
            let status: PushStatus = cli::decode_data(resp)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&status)?);
                return Ok(());
            }
            println!("push");
            println!("  enabled:        {}", status.enabled);
            println!(
                "  worker:         {}",
                status.worker_host.as_deref().unwrap_or("<none>")
            );
            println!("  subscriptions:  {}", status.subscriptions);
            println!("  outbox depth:   {}", status.outbox_depth);
            println!(
                "  last success:   {}",
                status
                    .last_success_at
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "<never>".into())
            );
            if let Some(error) = &status.last_error {
                println!(
                    "  last error:     {error} (at {})",
                    status
                        .last_error_at
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "?".into())
                );
            }
            Ok(())
        }
    }
}
