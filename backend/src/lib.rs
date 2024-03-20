use std::path::Path;

use prelude::*;

use crate::dao::in_memory_dao::InMemoryDao;
use crate::loader::Loader;

mod protobuf;
mod loader;
mod merge;
mod server;
mod dao;
mod utils;

pub mod prelude {
    pub use std::collections::{HashMap, HashSet};

    pub use num_derive::*;

    pub use crate::*;
    pub use crate::protobuf::history::*;
    #[cfg(test)]
    pub use crate::test_utils::*;
    pub use crate::utils::*;
    pub use crate::utils::entity_utils::*;
    pub use crate::utils::entity_utils::entity_equality::*;

    pub use chat_history_manager_core::message_regular;
    pub use chat_history_manager_core::message_regular_pat;
    pub use chat_history_manager_core::message_service;
    pub use chat_history_manager_core::message_service_pat;
    pub use chat_history_manager_core::message_service_pat_unreachable;
    pub use chat_history_manager_core::utils::entity_utils::*;
}

//
// Entry points
//

pub fn parse_file(path: &str, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
    thread_local! {
        static LOADER: Loader = Loader::new(&ReqwestHttpClient);
    }
    LOADER.with(|loader| {
        loader.parse(Path::new(path), myself_chooser)
    })
}

pub fn start_server(port: u16) -> EmptyRes {
    let loader = Loader::new(&ReqwestHttpClient);
    server::start_server(port, loader)
}

pub fn debug_request_myself(port: u16) -> EmptyRes {
    let chosen = server::debug_request_myself(port)?;
    log::info!("Picked: {}", chosen);
    Ok(())
}

//
// Other
//

pub trait MyselfChooser: Send {
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
