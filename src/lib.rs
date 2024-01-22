extern crate core;

#[macro_use]
extern crate num_derive;

use std::path::Path;

use prelude::*;

use crate::dao::in_memory_dao::InMemoryDao;

mod protobuf;
mod loader;
mod merge;
mod server;
mod dao;
mod utils;

pub mod prelude {
    pub use std::collections::{HashMap, HashSet};

    pub use crate::*;
    pub use crate::protobuf::history::*;
    #[cfg(test)]
    pub use crate::test_utils::*;
    pub use crate::utils::*;
    pub use crate::utils::entity_utils::*;
    pub use crate::utils::entity_utils::entity_equality::*;
}

//
// Entry points
//

pub fn parse_file(path: &str) -> Result<Box<InMemoryDao>> {
    thread_local! {
        static LOADER: loader::Loader = loader::Loader::new(&ReqwestHttpClient, Box::new(NoChooser));
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
