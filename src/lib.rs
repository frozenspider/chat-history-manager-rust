use crate::dao::in_memory_dao::InMemoryDao;
use crate::protobuf::history::User;
// Reexporting utility stuff
pub use crate::utils::*;

mod protobuf;
mod json;
mod server;
mod dao;
mod entities;
mod utils;

#[cfg(test)]
mod test_utils;

//
// Entry points
//

pub fn parse_file(path: &str) -> Res<Box<InMemoryDao>> {
    json::parse_file(path, &NoChooser)
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

pub trait ChooseMyselfTrait {
    fn choose_myself(&self, users: &[&User]) -> Res<usize>;
}

pub struct NoChooser;

impl ChooseMyselfTrait for NoChooser {
    fn choose_myself(&self, _pretty_names: &[&User]) -> Res<usize> {
        Err("No way to choose myself!".to_owned())
    }
}
