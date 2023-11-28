extern crate core;

use std::path::Path;

use itertools::Itertools;

use crate::dao::in_memory_dao::InMemoryDao;
use crate::protobuf::history::User;
#[cfg(test)]
pub use crate::test_utils::*;
// Reexporting utility stuff
pub use crate::utils::*;
pub use crate::utils::entity_utils::*;
pub use crate::utils::entity_utils::entity_equality::*;

mod protobuf;
mod loader;
mod merge;
mod server;
mod dao;
mod utils;

//
// Entry points
//

pub fn parse_file(path: &str) -> Result<Box<InMemoryDao>> {
    thread_local! {
        static LOADER: loader::Loader = loader::Loader::new(&ReqwestHttpClient, Box::new(NoChooser), None, None);
    }
    LOADER.with(|loader| {
        loader.parse(Path::new(path))
    })
}

pub fn start_server(port: u16) -> EmptyRes {
    server::start_server(port, &ReqwestHttpClient)
}

pub fn debug_request_myself(port: u16) -> EmptyRes {
    let chosen = server::debug_request_myself(port)?;
    log::info!("Picked: {}", chosen);
    Ok(())
}

//
// Other
//

pub trait MyselfChooser {
    fn choose_myself(&self, users: &[User]) -> Result<usize>;
}

#[derive(Clone, Copy)]
pub struct NoChooser;

impl MyselfChooser for NoChooser {
    fn choose_myself(&self, _pretty_names: &[User]) -> Result<usize> {
        err!("No way to choose myself!")
    }
}

pub trait HttpClient: Send + Sync {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>>;
}

pub struct ReqwestHttpClient;

impl HttpClient for ReqwestHttpClient {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        Ok(reqwest::blocking::get(url)?.bytes()?.to_vec())
    }
}
