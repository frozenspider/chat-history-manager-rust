use std::fs;

use crate::*;
use crate::dao::in_memory_dao::InMemoryDao;
use crate::loader::DataLoader;
use crate::protobuf::history::*;

use super::*;

const MRA_DBS: &str = "mra.dbs";

pub struct MailRuAgentDataLoader;

impl DataLoader for MailRuAgentDataLoader {
    fn name(&self) -> &'static str { "Mail.Ru Agent" }

    fn src_alias(&self) -> &'static str { "MRA (DBS)" }

    fn src_type(&self) -> SourceType { SourceType::Mra }

    fn looks_about_right_inner(&self, path: &Path) -> EmptyRes {
        if path_file_name(path)? != MRA_DBS {
            bail!("Given file is not {MRA_DBS}")
        }
        Ok(())
    }

    fn load_inner(&self, path: &Path, ds: Dataset, _myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        load_mra_dbs(path, ds)
    }
}

fn load_mra_dbs(path: &Path, ds: Dataset) -> Result<Box<InMemoryDao>> {
    todo!();
    Ok(Box::new(InMemoryDao::new_single(
        ds.alias.clone(),
        ds,
        path.to_path_buf(),
        todo!(), //users[0].clone(),
        todo!(), //users,
        todo!(), //cwms,
    )))
}
