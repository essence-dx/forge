use anyhow::Result;
use clap::{Parser, Subcommand};
use forge::cli;
use mimalloc::MiMalloc;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "forge",
    version,
    about = "Blazing-fast version control for media assets"
)]
struct Args {
    #[arg(long, global = true)]
    verbose: bool,

    #[arg(long, global = true)]
    repo_dir: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Init {
        #[arg(default_value = ".")]
        path: String,
    },
    Add {
        paths: Vec<String>,
        #[arg(long)]
        force: bool,
    },
    Commit {
        #[arg(short = 'm', long)]
        message: String,
    },
    Status,
    Log {
        #[arg(short = 'n', long, default_value_t = 20)]
        count: usize,
    },
    Diff {
        path: Option<String>,
        #[arg(long)]
        commit1: Option<String>,
        #[arg(long)]
        commit2: Option<String>,
    },
    Checkout {
        commit_id: String,
    },
    #[command(name = "checkout-archive")]
    CheckoutArchive {
        #[command(subcommand)]
        command: CheckoutArchiveCommand,
    },
    Package {
        #[command(subcommand)]
        command: PackageCommand,
    },
    Dx {
        #[command(subcommand)]
        command: DxCommand,
    },
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    Push {
        #[arg(default_value = "origin")]
        remote: String,
        /// Mirror targets: all-free | youtube | pinterest | soundcloud | sketchfab | github | gitlab | bitbucket | gdrive | dropbox | mega | r2
        #[arg(long)]
        mirror: Option<String>,
        /// Enable pro paid backends (R2, B2, GCS)
        #[arg(long)]
        pro: bool,
    },
    /// Authenticate a mirror backend and save credentials
    Auth {
        /// Backend: youtube | pinterest | soundcloud | sketchfab | github | gitlab | bitbucket | gdrive | dropbox | mega | r2 | all-free
        backend: String,
        /// Provide token directly (skip interactive prompt)
        #[arg(long)]
        token: Option<String>,
    },
    /// Create a demo project and print push instructions
    #[command(name = "demo-project", alias = "vibe-demo")]
    DemoProject,
    Pull {
        #[arg(default_value = "origin")]
        remote: String,
    },
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
    #[command(name = "train-dict")]
    TrainDict {
        #[arg(long)]
        file_type: String,
        #[arg(long)]
        samples: String,
        #[arg(long)]
        output: String,
    },
}

#[derive(Subcommand, Debug)]
enum RemoteCommand {
    List,
    Add {
        name: String,
        kind: String,
        locator: String,
        #[arg(long)]
        auth_backend: Option<String>,
        #[arg(long = "map")]
        branch_map: Vec<String>,
        #[arg(long)]
        primary: bool,
    },
    Remove {
        name: String,
    },
    Plan {
        #[arg(long)]
        remote: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum PackageCommand {
    Add {
        name: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        source: String,
        #[arg(long = "file")]
        files: Vec<String>,
        #[arg(long = "dependency")]
        dependencies: Vec<String>,
        #[arg(long, default_value = "source-owned slice, no package-manager install")]
        dependency_boundary: String,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(long)]
        json: bool,
    },
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long = "write-receipt")]
        write_receipt: bool,
    },
    Lock {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        write: bool,
    },
}

#[derive(Subcommand, Debug)]
enum CheckoutArchiveCommand {
    List {
        #[arg(long)]
        json: bool,
    },
    Restore {
        receipt: String,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum DxCommand {
    Status {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum JobsCommand {
    List,
    Show { id: String },
    Retry { id: String },
}

#[derive(Subcommand, Debug)]
enum SyncCommand {
    Status,
    Plan {
        #[arg(long)]
        remote: Option<String>,
    },
    Run {
        #[arg(long)]
        remote: Option<String>,
        #[arg(long)]
        force: bool,
        #[arg(long = "allow-dirty")]
        allow_dirty: bool,
    },
}

fn init_tracing(verbose: bool) {
    let default_level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("forge={default_level}")));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing(args.verbose);

    if let Some(repo_dir) = &args.repo_dir {
        std::env::set_current_dir(repo_dir)?;
    }

    match args.command {
        Command::Init { path } => cli::init::run(&path),
        Command::Add { mut paths, force } => {
            if paths.is_empty() {
                paths.push(".".to_string());
            }
            cli::add::run(&paths, force)
        }
        Command::Commit { message } => cli::commit::run(&message),
        Command::Status => cli::status::run(),
        Command::Log { count } => cli::log::run(count),
        Command::Diff {
            path,
            commit1,
            commit2,
        } => cli::diff::run(path.as_deref(), commit1.as_deref(), commit2.as_deref()),
        Command::Checkout { commit_id } => cli::checkout::run(&commit_id),
        Command::CheckoutArchive { command } => match command {
            CheckoutArchiveCommand::List { json } => cli::checkout_archive::run_list(json),
            CheckoutArchiveCommand::Restore {
                receipt,
                path,
                overwrite,
                json,
            } => cli::checkout_archive::run_restore(&receipt, path.as_deref(), overwrite, json),
        },
        Command::Package { command } => match command {
            PackageCommand::Add {
                name,
                version,
                source,
                files,
                dependencies,
                dependency_boundary,
                json,
            } => cli::package::run_add(
                &name,
                &version,
                &source,
                &files,
                &dependencies,
                &dependency_boundary,
                json,
            ),
            PackageCommand::List { json } => cli::package::run_list(json),
            PackageCommand::Status {
                json,
                write_receipt,
            } => cli::package::run_status(json, write_receipt),
            PackageCommand::Lock { json, write } => cli::package::run_lock(json, write),
        },
        Command::Dx { command } => match command {
            DxCommand::Status { json } => cli::dx::run_status(json),
        },
        Command::Remote { command } => match command {
            RemoteCommand::List => cli::remote::run_list(),
            RemoteCommand::Add {
                name,
                kind,
                locator,
                auth_backend,
                branch_map,
                primary,
            } => cli::remote::run_add(
                &name,
                &kind,
                &locator,
                auth_backend.as_deref(),
                &branch_map,
                primary,
            ),
            RemoteCommand::Remove { name } => cli::remote::run_remove(&name),
            RemoteCommand::Plan { remote } => cli::remote::run_plan(remote.as_deref()),
        },
        Command::Sync { command } => match command {
            SyncCommand::Status => cli::sync::run_status(),
            SyncCommand::Plan { remote } => cli::sync::run_plan(remote.as_deref()),
            SyncCommand::Run {
                remote,
                force,
                allow_dirty,
            } => cli::sync::run_execute(remote.as_deref(), force, allow_dirty),
        },
        Command::Push {
            remote,
            mirror,
            pro,
        } => cli::push::run(&remote, mirror.as_deref(), pro),
        Command::Pull { remote } => cli::pull::run(&remote),
        Command::Auth { backend, token } => cli::auth::run(&backend, token.as_deref()),
        Command::DemoProject => cli::demo_project::run(),
        Command::Jobs { command } => match command {
            JobsCommand::List => cli::jobs::run_list(),
            JobsCommand::Show { id } => cli::jobs::run_show(&id),
            JobsCommand::Retry { id } => cli::jobs::run_retry(&id),
        },
        Command::TrainDict {
            file_type,
            samples,
            output,
        } => cli::train_dict::run(&file_type, &samples, &output),
    }
}
