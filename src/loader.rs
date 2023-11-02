use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::Local;
use itertools::{Either, Itertools};
use uuid::Uuid;

use crate::*;
use crate::loader::telegram::TelegramDataLoader;
use crate::loader::whatsapp_android::WhatsAppAndroidDataLoader;
use crate::loader::whatsapp_text::WhatsAppTextDataLoader;
use crate::protobuf::history::{Dataset, PbUuid};

mod telegram;
mod whatsapp_android;
mod whatsapp_text;

trait DataLoader {
    fn name(&self) -> &'static str;

    /// Used in dataset alias
    fn src_alias(&self) -> &'static str;

    /// Used as a dataset source type
    fn src_type(&self) -> &'static str;

    // TODO: Add allowed files filter

    fn looks_about_right(&self, path: &Path) -> EmptyRes {
        ensure_file_presence(path)?;
        self.looks_about_right_inner(path)
    }

    fn looks_about_right_inner(&self, path: &Path) -> EmptyRes;

    fn load(&self, path: &Path, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
        let root_path_str = ensure_file_presence(path)?;
        measure(|| {
            let now_str = Local::now().format("%Y-%m-%d");
            let ds = Dataset {
                uuid: Some(PbUuid { value: Uuid::new_v4().to_string().to_lowercase() }),
                alias: format!("{}, loaded @ {now_str}", self.src_alias()),
                source_type: self.src_type().to_owned(),
            };
            self.load_inner(path, ds, myself_chooser)
        }, |_, t| log::info!("File {} loaded in {t} ms", root_path_str))
    }

    fn load_inner(&self, path: &Path, ds: Dataset, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>>;
}

fn ensure_file_presence(root_file: &Path) -> Result<&str> {
    let root_file_str = path_to_str(root_file)?;
    if !root_file.exists() {
        bail!("File not found: {}", root_file_str)
    }
    Ok(root_file_str)
}

fn hash_to_id(str: &str) -> i64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = hasher().build_hasher();
    // Following write_str unstable implementation
    h.write(str.as_bytes());
    h.write_u8(0xff);
    (h.finish() / 2) as i64
}

thread_local! {
    static LOADERS: Vec<&'static dyn DataLoader> = {
        let vec: Vec<&dyn DataLoader> = vec![&TelegramDataLoader, &WhatsAppAndroidDataLoader, &WhatsAppTextDataLoader];
        vec
    };
}

pub fn load(root_path: &Path, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
    LOADERS.with(|loaders: &Vec<&dyn DataLoader>| {
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

fn first_line(path: &Path) -> Result<String> {
    let input = File::open(path)?;
    let buffered = BufReader::new(input);
    Ok(buffered.lines().next().ok_or(anyhow!("File is empty"))??.trim().to_owned())
}
