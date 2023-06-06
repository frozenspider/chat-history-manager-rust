use std::env::args;

use mimalloc::MiMalloc;

use crate::proto::history::{ChatWithMessages, Dataset, User};

mod proto;
mod json;
mod server;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use Result<T, Box<dyn std::error::Error>> maybe?
pub type Res<T> = Result<T, String>;
pub type EmptyRes = Res<()>;

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
            server::start_server().unwrap();
        }
        Some(path) => {
            json::parse_file(path.as_str()).unwrap();
        }
    }
}
