use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::Local;
use const_format::concatcp;
use itertools::{Either, Itertools};

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::loader::telegram::TelegramDataLoader;
use crate::loader::tinder_android::TinderAndroidDataLoader;
use crate::loader::whatsapp_android::WhatsAppAndroidDataLoader;
use crate::loader::whatsapp_text::WhatsAppTextDataLoader;
use crate::protobuf::history::{Dataset, PbUuid, SourceType};

mod telegram;
mod tinder_android;
mod whatsapp_android;
mod whatsapp_text;

trait DataLoader {
    fn name(&self) -> &'static str;

    /// Used in dataset alias
    fn src_alias(&self) -> &'static str {
        self.name()
    }

    fn src_type(&self) -> SourceType;

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
                uuid: Some(PbUuid::random()),
                alias: format!("{}, loaded @ {now_str}", self.src_alias()),
            };
            self.load_inner(path, ds, myself_chooser)
        }, |_, t| log::info!("File {} loaded in {t} ms", root_path_str))
    }

    fn load_inner(&self, path: &Path, ds: Dataset, myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>>;
}

pub struct Loader<MC: MyselfChooser> {
    loaders: Vec<Box<dyn DataLoader + Sync>>,
    myself_chooser: MC,
}

impl<MC: MyselfChooser> Loader<MC> {
    pub fn new<H: HttpClient>(http_client: &'static H, myself_chooser: MC) -> Loader<MC> {
        Loader {
            loaders: vec![
                Box::new(TelegramDataLoader),
                Box::new(TinderAndroidDataLoader { http_client }),
                Box::new(WhatsAppAndroidDataLoader),
                Box::new(WhatsAppTextDataLoader),
            ],
            myself_chooser,
        }
    }

    pub fn load(&self, root_path: &Path) -> Result<Box<dyn ChatHistoryDao>> {
        Ok(self.parse(root_path)?)
    }

    /// Parses a history in a foreign format
    pub fn parse(&self, root_path: &Path) -> Result<Box<InMemoryDao>> {
        let (named_errors, loads): (Vec<_>, Vec<_>) =
            self.loaders.iter()
                .partition_map(|loader| match loader.looks_about_right(root_path) {
                    Ok(()) => Either::Right(|| loader.load(root_path, &self.myself_chooser)),
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
    }
}

// Loader is stateless after construction, so it's safe to be shared between threads.

unsafe impl<MC: MyselfChooser> Send for Loader<MC> {}

unsafe impl<MC: MyselfChooser> Sync for Loader<MC> {}

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

fn first_line(path: &Path) -> Result<String> {
    let input = File::open(path)?;
    let buffered = BufReader::new(input);
    Ok(buffered.lines().next().with_context(|| format!("File is empty"))??.trim().to_owned())
}

// Android-specific helpers.
pub mod android {
    pub const DATABASES: &str = "databases";

    /// Boilerplate for a data loader of salvaged Android sqlite database.
    /// First construct a custom users structure, use it to read chats, then normalize the structure into
    /// plain old Vec<User>.
    /// Produced users should have myself as a first user.
    #[macro_export]
    macro_rules! android_sqlite_loader {
        (
            $loader_name:ident $(<
                $(
                    $generic_type_name:ident
                    $(: $generic_type_bound:ident $(+ $generic_type_bound2:ident)* )?
                ),+
            >)?,
            $tpe:ident,
            $name:literal,
            $db_filename:literal
        ) => {
            #[allow(dead_code)]
            const DB_FILENAME: &str = $db_filename;

            impl$(
                <$(
                    $generic_type_name
                    $(: $generic_type_bound $(+ $generic_type_bound2:ident)* )?
                ),*>
            )? DataLoader for $loader_name$(<$($generic_type_name),*>)? {
                fn name(&self) -> &'static str { concatcp!($name, " (db)") }

                fn src_alias(&self) -> &'static str { self.name() }

                fn src_type(&self) -> SourceType { SourceType::$tpe }

                fn looks_about_right_inner(&self, path: &Path) -> EmptyRes {
                    let filename = path_file_name(path)?;
                    if filename != $db_filename {
                        bail!("File is not {}", $db_filename);
                    }
                    Ok(())
                }

                fn load_inner(&self, path: &Path, ds: Dataset, _myself_chooser: &dyn MyselfChooser) -> Result<Box<InMemoryDao>> {
                    parse_android_db(self, path, ds)
                }
            }

            fn parse_android_db$(
                <$(
                    $generic_type_name
                    $(: $generic_type_bound $(+ $generic_type_bound2:ident)* )?
                ),*>
            )? (this: &$loader_name$(<$($generic_type_name),*>)?, path: &Path, ds: Dataset) -> Result<Box<InMemoryDao>> {
                let path = path.parent().unwrap();
                let ds_uuid = ds.uuid.as_ref().unwrap();

                let conn = Connection::open(path.join($db_filename))?;
                this.tweak_conn(path, &conn)?;

                let path = if path_file_name(path)? == android::DATABASES {
                    path.parent().unwrap()
                } else {
                    path
                };

                let mut users = this.parse_users(&conn, ds_uuid)?;
                let cwms = this.parse_chats(&conn, ds_uuid, &mut users, &path)?;

                let users = this.normalize_users(users, &cwms)?;
                Ok(Box::new(InMemoryDao::new(
                    format!("{} ({})", $name, path_file_name(path)?),
                    ds,
                    path.to_path_buf(),
                    users[0].clone(),
                    users,
                    cwms,
                )))
            }
        };
    }
}
