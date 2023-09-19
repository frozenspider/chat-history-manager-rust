use std::path::Path;
use uuid::Uuid;
use crate::*;

mod telegram;
mod json_utils;

pub fn load(root_path: &Path, myself_chooser: &impl MyselfChooser) -> Result<Box<InMemoryDao>> {
    let uuid = Uuid::new_v4();
    telegram::parse_telegram_file(root_path, &uuid, myself_chooser)
}
