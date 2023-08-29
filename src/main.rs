extern crate core;

use std::env::args;
use std::error::Error;
use std::path::PathBuf;

use deepsize::DeepSizeOf;
use log::LevelFilter;
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
    ds_root: PathBuf,
    myself: User,
    users: Vec<User>,
    cwm: Vec<ChatWithMessages>,
}

pub trait ChooseMyselfTrait {
    fn choose_myself(&self, users: &Vec<&User>) -> Res<usize>;
}

pub struct NoChooser;

impl ChooseMyselfTrait for NoChooser {
    fn choose_myself(&self, _pretty_names: &Vec<&User>) -> Res<usize> {
        Err("No way to choose myself!".to_owned())
    }
}

fn error_to_string<E: Error>(e: E) -> String {
    let mut s = String::new();
    s += &e.to_string();
    if let Some(src_e) = e.source() {
        s += " (caused by: ";
        s += &error_to_string(src_e);
        s += ")";
    }
    s
}

/** Starts a server by default. If an argument is provided, it's used as a path and parsed. */
fn main() {
    env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .init();

    let server_port: u16 = 50051;

    let mut args = args();
    args.next(); // Consume first argument, which is a command itself.
    match args.next().as_deref() {
        None => {
            server::start_server(server_port).unwrap();
        }
        Some("parse") => {
            let path = args.next().unwrap();
            let parsed = json::parse_file(&path, &NoChooser).unwrap();
            let size: usize = parsed.deep_size_of();
            log::debug!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
        Some("request_myself") => {
            server::make_choose_myself_request(server_port + 1).unwrap();
        }
        Some(etc) => {
            panic!("Unrecognized command: {etc}")
        }
    }
}
