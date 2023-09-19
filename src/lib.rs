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
    loader::load(Path::new(path), &NoChooser)
}

pub fn start_server(port: u16) -> EmptyRes {
    server::start_server(port)
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
    fn choose_myself(&self, users: &[&User]) -> Result<usize>;
}

pub struct NoChooser;

impl MyselfChooser for NoChooser {
    fn choose_myself(&self, _pretty_names: &[&User]) -> Result<usize> {
        err!("No way to choose myself!")
    }
}
