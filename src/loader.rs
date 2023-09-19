use std::path::Path;
use itertools::{Either, Itertools};
use crate::*;
use crate::loader::telegram::TelegramDataLoader;

mod telegram;
mod json_utils;

trait DataLoader {
    fn name(&self) -> &'static str;

    fn looks_about_right(&self, root_path: &Path) -> EmptyRes {
        ensure_file_presence(root_path)?;
        self.looks_about_right_inner(root_path)
    }

    fn looks_about_right_inner(&self, root_path: &Path) -> EmptyRes;

    fn load(&self, root_path: &Path, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        let root_path_str = ensure_file_presence(root_path)?;
        measure(|| {
            self.load_inner(root_path, myself_chooser)
        }, |_, t| log::info!("File {} loaded in {t} ms", root_path_str))
    }

    fn load_inner(&self, root_path: &Path, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>>;
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| "Failed to convert path to a string".into())
}

fn ensure_file_presence(root_file: &Path) -> Result<&str> {
    let root_file_str = path_to_str(root_file)?;
    if !root_file.exists() {
        bail!("File not found: {}", root_file_str)
    }
    Ok(root_file_str)
}

thread_local! {
    static LOADERS: Vec<&'static dyn DataLoader> = {
        let vec: Vec<&dyn DataLoader> = vec![&TelegramDataLoader];
        vec
    };
}

pub fn load(root_path: &Path, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
    LOADERS.with(|loaders| {
        let (named_errors, loads): (Vec<_>, Vec<_>) =
            loaders.iter()
                .partition_map(|loader| match loader.looks_about_right(root_path) {
                    Ok(()) => Either::Right(|| loader.load(root_path, myself_chooser)),
                    Err(why) => Either::Left((loader.name(), why)),
                });
        match loads.first() {
            Some(load) =>
                load(),
            None => {
                // Report why everyone rejected the file.
                err!("No loader accepted the file:\n{}",
                     named_errors.iter().map(|(name, why)| format!("{}: {}", name, why)).join("\n"))
            }
        }
    })
}
