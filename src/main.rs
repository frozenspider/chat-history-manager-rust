use std::env::args;
use std::process;

use deepsize::DeepSizeOf;
use log::LevelFilter;
use mimalloc::MiMalloc;

use chat_history_manager_rust::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/** Starts a server by default. */
fn main() {
    init_logger();

    let mut args = args();
    args.next(); // Consume first argument, which is a command itself.
    let command = args.next();
    if let Err(e) = execute_command(command, args.collect()) {
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

fn execute_command(command: Option<String>, args: Vec<String>) -> EmptyRes {
    let server_port: u16 = 50051;

    match command.map(|c| c.to_lowercase()).as_deref() {
        None => {
            start_server(server_port)?;
        }
        Some("parse") => {
            let path = args.get(0).context("Parse path wasn't given")?;
            let parsed = parse_file(path).context("Parsing failed!")?;
            let size: usize = parsed.deep_size_of();
            log::info!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
        Some("request_myself") => {
            debug_request_myself(server_port + 1)?;
        }
        Some(etc) => {
            panic!("Unrecognized command: {etc}")
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
