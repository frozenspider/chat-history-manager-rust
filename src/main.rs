extern crate core;

use std::env::args;
use std::error::Error;
use std::path::PathBuf;

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
    s += e.to_string().as_str();
    if let Some(src_e) = e.source() {
        s += " (caused by: ";
        s += error_to_string(src_e).as_str();
        s += ")";
    }
    s
}

/** Starts a server by default. If an argument is provided, it's used as a path and parsed. */
fn main() {
    // server::make_choose_myself_request(50052).unwrap();
    let mut args = args();
    match args.nth(1) {
        None => {
            let server_port: u16 = 50051;
            server::start_server(server_port).unwrap();
        }
        Some(path) => {
            let parsed = json::parse_file(path.as_str(), &NoChooser).unwrap();
            let size: usize = parsed.deep_size_of();
            println!("Size of parsed in-memory DB: {} MB ({} B)", size / 1024 / 1024, size);
        }
    }
}
