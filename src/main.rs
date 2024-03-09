use std::process;

use clap::{Parser, Subcommand};
use deepsize::DeepSizeOf;
use log::LevelFilter;
use mimalloc::MiMalloc;

use chat_history_manager_rust::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[clap(about = "Parse and load a given file using whichever loader is appropriate")]
    Parse { path: String },
    #[clap(about = "(For debugging purposes only) Ask UI which user is \"myself\" and print it to the log")]
    RequestMyself,
}

/** Starts a server by default. */
fn main() {
    init_logger();

    let args = Args::parse();
    if let Err(e) = execute_command(args.command) {
        eprintln!("Error: {}", error_to_string(&e));
        let backtrace = e.backtrace();
        // Backtrace is defined as just "&impl Debug + Display", so to make sure we actually have a backtrace
        // we have to use a rather dirty workaround - if backtrace is not available, its string representation
        // will be just one line like "disabled backtrace" or "unsupported backtrace".
        // See anyhow::backtrace::capture::<impl Display for Backtrace>
        let backtrace = backtrace.to_string();
        if backtrace.contains('\n') {
            eprintln!();
            eprintln!("Stack trace:\n{}", e.backtrace());
        }
        process::exit(1);
    }
}

fn execute_command(command: Option<Command>) -> EmptyRes {
    let server_port: u16 = 50051;

    match command {
        None => {
            start_server(server_port)?;
        }
        Some(Command::Parse { path }) => {
            let parsed = parse_file(&path).with_context(|| format!("Failed to parse {path}"))?;
            let size: usize = parsed.deep_size_of();
            log::info!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
        Some(Command::RequestMyself) => {
            debug_request_myself(server_port + 1)?;
        }
    }
    Ok(())
}

fn init_logger() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Debug)
        .format(|buf, record| {
            use std::io::Write;

            let timestamp = buf.timestamp_millis();
            let level = record.level();
            let target = record.target();

            let thread = std::thread::current();
            writeln!(buf, "{} {: <5} {} - {} [{}]",
                     timestamp, level, target, record.args(),
                     thread.name().unwrap_or("<unnamed>"))
        })
        .init();
}
