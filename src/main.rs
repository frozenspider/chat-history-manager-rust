use std::env::args;

use deepsize::DeepSizeOf;
use mimalloc::MiMalloc;

use crate::protobuf::history::{ChatWithMessages, Dataset, User};

mod protobuf;
mod json;
mod server;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use Result<T, Box<dyn std::error::Error>> maybe?
pub type Res<T> = Result<T, String>;
pub type EmptyRes = Res<()>;

#[derive(DeepSizeOf)]
pub struct InMemoryDb {
    dataset: Dataset,
    myself: User,
    users: Vec<User>,
    cwm: Vec<ChatWithMessages>,
}

/** Starts a server by default. If an argument is provided, it's used as a path and parsed. */
fn main() {
    let mut args = args();
    match args.nth(1) {
        None => {
            let server_port: u16 = 50051;
            server::start_server(server_port).unwrap();
        }
        Some(path) => {
            let parsed = json::parse_file(path.as_str()).unwrap();
            let size: usize = parsed.deep_size_of();
            println!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
    }
}
