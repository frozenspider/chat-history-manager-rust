use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::default::Default;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use diesel::insert_into;
use diesel::migration::MigrationSource;
use diesel::prelude::*;
use diesel::sqlite::Sqlite;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use itertools::Either;
use uuid::Uuid;

use mapping::*;

use crate::*;

use super::*;

mod mapping;
mod utils;

#[cfg(test)]
#[path = "sqlite_dao_tests.rs"]
mod tests;

pub struct SqliteDao {
    name: String,
    db_file: PathBuf,
    conn: RefCell<SqliteConnection>,
    cache: Box<RefCell<SqliteCache>>,
}

impl SqliteDao {
    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./resources/main/migrations");
    const FILENAME: &'static str = "data.sqlite";

    pub fn create(db_file: PathBuf) -> Result<Self> {
        require!(!db_file.exists(), "File {} already exists!", path_to_str(&db_file)?);
        Self::create_load_inner(db_file)
    }

    #[allow(unused)]
    pub fn load(db_file: PathBuf) -> Result<Self> {
        require!(db_file.exists(), "File {} does not exist!", path_to_str(&db_file)?);
        Self::create_load_inner(db_file)
    }

    fn check_db_file_path(db_file: &Path) -> EmptyRes {
        require!(db_file.parent().is_some_and(|p| p.exists()),
            "Parent directory for {} does not exist!", path_to_str(db_file)?);
        require!(path_file_name(db_file)? == SqliteDao::FILENAME,
            "Incorrect file name for {}, expected {}", path_to_str(db_file)?, SqliteDao::FILENAME);
        Ok(())
    }

    fn create_load_inner(db_file: PathBuf) -> Result<Self> {
        Self::check_db_file_path(&db_file)?;
        let absolute_path = fs::canonicalize(db_file.parent().unwrap())?.join(path_file_name(&db_file)?);
        let absolute_path = absolute_path.to_str().expect("Cannot get absolute DB path!");
        let conn = RefCell::new(SqliteConnection::establish(absolute_path)?);

        // Apply migrations
        require!(!<EmbeddedMigrations as MigrationSource<Sqlite>>::migrations(&SqliteDao::MIGRATIONS)
            .normalize_error()?.is_empty(),
                "Migrations not found!");
        {
            let mut conn = conn.borrow_mut();
            let migrations = conn.pending_migrations(SqliteDao::MIGRATIONS).normalize_error()?;
            for m in migrations.iter() {
                log::info!("Applying migration: {}", m.name());
                conn.run_migration(m).normalize_error()?;
            }
        }

        Ok(SqliteDao {
            name: format!("{} database", path_file_name(&db_file)?),
            db_file,
            conn,
            cache: SqliteCache::new_wrapped(),
        })
    }

    /// Lazily initialize cache and return the reference to it.
    fn cache(&self) -> Result<Ref<SqliteCache>> {
        let mut cache = self.cache.as_ref().borrow_mut();
        if !cache.initialized {
            cache.datasets =
                schema::dataset::table
                    .select(RawDataset::as_select())
                    .load_iter(self.conn.borrow_mut().deref_mut())?
                    .flatten()
                    .map(utils::dataset::deserialize)
                    .try_collect()?;

            let ds_uuids = cache.datasets.iter().map(|ds| ds.uuid.clone().unwrap()).collect_vec();
            for ds_uuid in ds_uuids {
                let uuid = Uuid::parse_str(&ds_uuid.value)?;
                let rows: Vec<(User, bool)> = schema::user::table
                    .filter(schema::user::columns::ds_uuid.eq(uuid.as_ref()))
                    .select(RawUser::as_select())
                    .load_iter(self.conn.borrow_mut().deref_mut())?
                    .flatten()
                    .map(utils::user::deserialize)
                    .try_collect()?;
                let (mut myselves, mut users): (Vec<_>, Vec<_>) =
                    rows.into_iter().partition_map(|(users, is_myself)|
                        if is_myself { Either::Left(users) } else { Either::Right(users) });
                require!(myselves.len() > 0, "Myself not found!");
                require!(myselves.len() < 2, "More than one myself found!");
                let myself = myselves.remove(0);
                users.insert(0, myself.clone());
                cache.users.insert(ds_uuid, UserCacheForDataset {
                    myself,
                    user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
                });
            }

            cache.initialized = true;
        }
        drop(cache);

        Ok(self.cache.deref().borrow())
    }

    pub fn copy_all_from(&self, src: &impl ChatHistoryDao) -> EmptyRes {
        measure(|| {
            let mut cache = self.cache.as_ref().borrow_mut();

            let src_datasets = src.datasets()?;

            for src_ds in src_datasets.iter() {
                let ds_uuid = src_ds.uuid();
                let src_myself = src.myself(ds_uuid)?;

                measure(|| {
                    use schema::*;

                    let raw_ds = utils::dataset::serialize(src_ds);

                    self.conn.borrow_mut().transaction(|txn| {
                        insert_into(dataset::table).values(&raw_ds).execute(txn)?;

                        let raw_users: Vec<RawUser> = src.users(ds_uuid)?.iter().map(|u| {
                            require!(u.id > 0, "IDs should be positive!");
                            Ok(utils::user::serialize(u, *u == src_myself, &raw_ds.uuid))
                        }).try_collect()?;
                        insert_into(user::table).values(&raw_users).execute(txn)?;
                        Ok::<_, anyhow::Error>(())
                    })?;

                    let src_ds_root = src.dataset_root(ds_uuid);
                    let dst_ds_root = self.dataset_root(ds_uuid);

                    for src_cwm in src.chats(ds_uuid)?.iter() {
                        require!(src_cwm.chat.id > 0, "IDs should be positive!");

                        self.conn.borrow_mut().transaction(|txn| {
                            let mut raw_chat = utils::chat::serialize(&src_cwm.chat, &raw_ds.uuid)?;
                            if let Some(ref img) = src_cwm.chat.img_path_option {
                                raw_chat.img_path =
                                    copy_file(&img, &None, &subpaths::ROOT,
                                              src_cwm.chat.id, &src_ds_root, &dst_ds_root)?;
                            }
                            insert_into(chat::table).values(raw_chat).execute(txn)?;
                            insert_into(chat_member::table)
                                .values(src_cwm.chat.member_ids.iter()
                                    .map(|&user_id|
                                        RawChatMember {
                                            ds_uuid: raw_ds.uuid.clone(),
                                            chat_id: src_cwm.chat.id,
                                            user_id,
                                        })
                                    .collect_vec())
                                .execute(txn)?;
                            Ok::<_, anyhow::Error>(())
                        })?;

                        const BATCH_SIZE: usize = 1000;
                        let mut offset: usize = 0;
                        loop {
                            let src_msgs = src.scroll_messages(&src_cwm.chat, offset, BATCH_SIZE)?;
                            let full_raw_msgs: Vec<FullRawMessage> = src_msgs.iter()
                                .map(|m| utils::message::serialize_and_copy_files(
                                    m, src_cwm.chat.id, &raw_ds.uuid, &src_ds_root, &dst_ds_root))
                                .try_collect()?;

                            // copy_file

                            // Copy messages
                            self.conn.borrow_mut().transaction(|txn| {
                                // Don't see a way around cloning here.
                                let raw_messages = full_raw_msgs.iter().map(|full| full.m.clone()).collect_vec();

                                // Even though SQLite supports RETURNING clause and Diesel claims to support it too,
                                // it's not possible to INSERT RETURNING multiple values due to
                                // https://stackoverflow.com/a/77488801/466646
                                // To work around that, we have to do a separate SELECT.
                                insert_into(message::table).values(&raw_messages).execute(txn)?;
                                let mut internal_ids: Vec<i64> = schema::message::table
                                    .order_by(schema::message::columns::internal_id.desc())
                                    .limit(raw_messages.len() as i64)
                                    .select(schema::message::columns::internal_id)
                                    .load(txn)?;
                                internal_ids.reverse();

                                let mut raw_mcs = vec![];
                                let mut raw_rtes = vec![];
                                for (mut raw, internal_id) in full_raw_msgs.into_iter().zip(internal_ids) {
                                    if let Some(mut mc) = raw.mc {
                                        mc.message_internal_id = internal_id;
                                        raw_mcs.push(mc);
                                    }

                                    raw.rtes.iter_mut().for_each(|rte| rte.message_internal_id = Some(internal_id));
                                    raw_rtes.extend(raw.rtes.into_iter());
                                }

                                insert_into(message_content::table).values(raw_mcs).execute(txn)?;
                                insert_into(message_text_element::table).values(raw_rtes).execute(txn)?;

                                Ok::<_, anyhow::Error>(())
                            })?;

                            if src_msgs.len() < BATCH_SIZE { break; }
                            offset += BATCH_SIZE;
                        }
                    }
                    Ok(())
                }, |_, t| log::info!("Dataset '{}' inserted in {t} ms", ds_uuid.value))?;
            }

            // Invalidate the cache
            cache.initialized = false;
            drop(cache);

            require!(src_datasets.len() == self.datasets()?.len(), "Datasets have different sizes after merge!");

            for src_ds in src_datasets.iter() {
                let ds_uuid = src_ds.uuid();
                let src_ds_root = src.dataset_root(ds_uuid);
                let dst_ds_root = self.dataset_root(ds_uuid);
                require!(*src_ds_root != *dst_ds_root, "Source and destination dataset root paths are the same!");

                self.copy_all_sanity_check(src, ds_uuid, src_ds, &src_ds_root, &dst_ds_root)?;
            }

            Ok(())
        }, |_, t| log::info!("Dao '{}' fully copied {t} ms", src.name()))
    }

    fn copy_all_sanity_check(&self,
                             src: &impl ChatHistoryDao,
                             ds_uuid: &PbUuid,
                             src_ds: &Dataset,
                             src_ds_root: &DatasetRoot,
                             dst_ds_root: &DatasetRoot) -> EmptyRes {
        measure(|| {
            let ds = self.datasets()?.into_iter().find(|ds| ds.uuid() == ds_uuid)
                .ok_or(anyhow!("Dataset {} not found after insert!", ds_uuid.value))?;
            require!(*src_ds == ds, "Inserted dataset is not the same as original!");

            measure(|| {
                let src_users = src.users(ds_uuid)?;
                let dst_users = self.users(ds_uuid)?;
                require!(src_users.len() == dst_users.len(),
                     "User count differs:\nWas    {} ({:?})\nBecame {} ({:?})",
                     src_users.len(), src_users, dst_users.len(), dst_users);
                for (i, (src_user, dst_user)) in src_users.iter().zip(dst_users.iter()).enumerate() {
                    require!(src_user == dst_user,
                             "User #{i} differs:\nWas    {:?}\nBecame {:?}", src_user, dst_user);
                }
                Ok(())
            }, |_, t| log::info!("Users checked in {t} ms"))?;

            let src_chats = src.chats(ds_uuid)?;
            let dst_chats = self.chats(ds_uuid)?;
            require!(src_chats.len() == dst_chats.len(),
                     "Chat count differs:\nWas    {}\nBecame {}", src_chats.len(), dst_chats.len());

            for (i, (src_cwd, dst_cwd)) in src_chats.iter().zip(dst_chats.iter()).enumerate() {
                measure(|| {
                    require!(src_cwd.chat == dst_cwd.chat,
                             "Chat #{i} differs:\nWas    {:?}\nBecame {:?}", src_cwd.chat, dst_cwd.chat);

                    let src_messages = src.last_messages(&src_cwd.chat, src_cwd.chat.msg_count as usize)?;
                    let dst_messages = self.last_messages(&dst_cwd.chat, dst_cwd.chat.msg_count as usize)?;
                    require!(src_messages.len() == dst_messages.len(),
                             "Messages size for chat {} differs:\nWas    {}\nBecame {}",
                             src_cwd.chat.qualified_name(), src_chats.len(), dst_chats.len());

                    for (j, (src_msg, dst_msg)) in src_messages.iter().zip(dst_messages.iter()).enumerate() {
                        let src_pet = PracticalEqTuple::new(src_msg, src_ds_root, &src_cwd);
                        let dst_pet = PracticalEqTuple::new(dst_msg, dst_ds_root, &dst_cwd);
                        require!(src_pet.practically_equals(&dst_pet)?,
                                 "Message #{j} for chat {} differs:\nWas    {:?}\nBecame {:?}",
                                 src_cwd.chat.qualified_name(), src_msg, dst_msg);
                        //
                    }
                    Ok(())
                }, |_, t| log::info!("Chat {} checked in {t} ms", dst_cwd.chat.qualified_name()))?;
            }

            Ok(())
        }, |_, t| log::info!("Dataset '{}' checked in {t} ms", ds_uuid.value))
    }

    fn fetch_messages<F>(&self, get_raw_messages_with_content: F) -> Result<Vec<Message>>
        where F: Fn(&mut SqliteConnection) -> Result<Vec<(RawMessage, Option<RawMessageContent>)>>
    {
        utils::message::fetch(self.conn.borrow_mut().deref_mut(), get_raw_messages_with_content)
    }
}

impl ChatHistoryDao for SqliteDao {
    fn name(&self) -> &str {
        &self.name
    }

    fn storage_path(&self) -> &Path {
        self.db_file.parent().unwrap()
    }

    fn datasets(&self) -> Result<Vec<Dataset>> {
        Ok(self.cache()?.datasets.clone())
    }

    fn dataset_root(&self, ds_uuid: &PbUuid) -> DatasetRoot {
        DatasetRoot(self.db_file.parent().expect("Database file has no parent!").join(&ds_uuid.value).to_path_buf())
    }

    fn myself(&self, ds_uuid: &PbUuid) -> Result<User> {
        Ok(self.cache()?.borrow().users[ds_uuid].myself.clone())
    }

    fn users_inner(&self, ds_uuid: &PbUuid) -> Result<(Vec<User>, UserId)> {
        let cache = self.cache()?;
        let cache = cache.borrow();
        let cache = &cache.users[ds_uuid];
        let users = cache.user_by_id.values().cloned().collect_vec();
        Ok((users, UserId(cache.myself.id)))
    }

    fn user_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<User>> {
        Ok(self.cache()?.borrow().users[ds_uuid].user_by_id.get(&UserId(id)).cloned())
    }

    fn chats_inner(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();
        let cache = self.cache()?;
        let cache = cache.deref();

        let rows: Vec<ChatWithDetails> =
            utils::chat::select_by_ds(&uuid, conn)?
                .into_iter()
                .map(|raw: RawChatQ| utils::chat::deserialize(raw, conn, ds_uuid, cache))
                .try_collect()?;

        Ok(rows)
    }

    fn chat_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();
        let cache = self.cache()?;
        let cache = cache.deref();

        let mut rows: Vec<ChatWithDetails> =
            utils::chat::select_by_ds_and_id(&uuid, id, conn)?
                .into_iter()
                .map(|raw: RawChatQ| utils::chat::deserialize(raw, conn, ds_uuid, cache))
                .try_collect()?;

        if rows.is_empty() { Ok(None) } else { Ok(Some(rows.remove(0))) }
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        self.fetch_messages(|conn| {
            Ok(schema::message::table
                .filter(schema::message::columns::chat_id.eq(chat.id))
                .order_by((schema::message::columns::time_sent.asc(), schema::message::columns::internal_id.asc()))
                .left_join(schema::message_content::table)
                .offset(offset as i64)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        let mut msgs = self.fetch_messages(|conn| {
            Ok(schema::message::table
                .filter(schema::message::columns::chat_id.eq(chat.id))
                .order_by((schema::message::columns::time_sent.desc(), schema::message::columns::internal_id.desc()))
                .left_join(schema::message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        msgs.reverse();
        Ok(msgs)
    }

    fn messages_before_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        use schema::message::*;
        let mut msgs = self.fetch_messages(|conn| {
            Ok(table
                .filter(columns::chat_id.eq(chat.id))
                .filter(columns::time_sent.lt(msg.timestamp)
                    .or(columns::time_sent.eq(msg.timestamp).and(columns::internal_id.lt(msg.internal_id))))
                .order_by((columns::time_sent.desc(), columns::internal_id.desc()))
                .left_join(schema::message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        msgs.reverse();
        Ok(msgs)
    }

    fn messages_after_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        use schema::message::*;
        self.fetch_messages(|conn| {
            Ok(table
                .filter(columns::chat_id.eq(chat.id))
                .filter(columns::time_sent.gt(msg.timestamp)
                    .or(columns::time_sent.eq(msg.timestamp).and(columns::internal_id.gt(msg.internal_id))))
                .order_by((columns::time_sent.asc(), columns::internal_id.asc()))
                .left_join(schema::message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })
    }

    fn messages_between_impl(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<Vec<Message>> {
        use schema::message::*;
        self.fetch_messages(|conn| {
            Ok(table
                .filter(columns::chat_id.eq(chat.id))
                .filter(columns::time_sent.gt(msg1.timestamp)
                    .or(columns::time_sent.eq(msg1.timestamp).and(columns::internal_id.gt(msg1.internal_id))))
                .filter(columns::time_sent.lt(msg2.timestamp)
                    .or(columns::time_sent.eq(msg2.timestamp).and(columns::internal_id.lt(msg2.internal_id))))
                .order_by((columns::time_sent.asc(), columns::internal_id.asc()))
                .left_join(schema::message_content::table)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })
    }

    fn count_messages_between(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<usize> {
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        use schema::message::*;
        let count: i64 = table
            .filter(columns::chat_id.eq(chat.id))
            .filter(columns::time_sent.gt(msg1.timestamp)
                .or(columns::time_sent.eq(msg1.timestamp).and(columns::internal_id.gt(msg1.internal_id))))
            .filter(columns::time_sent.lt(msg2.timestamp)
                .or(columns::time_sent.eq(msg2.timestamp).and(columns::internal_id.lt(msg2.internal_id))))
            .order_by((columns::time_sent.asc(), columns::internal_id.asc()))
            .count()
            .get_result(conn)?;

        Ok(count as usize)
    }

    fn messages_around_date(&self,
                            _chat: &Chat,
                            _date_ts: Timestamp,
                            _limit: usize) -> Result<(Vec<Message>, Vec<Message>)> {
        // Not needed yet, so leaving this out
        todo!()
    }

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>> {
        self.fetch_messages(|conn| {
            Ok(schema::message::table
                .filter(schema::message::columns::chat_id.eq(chat.id))
                .filter(schema::message::columns::source_id.eq(Some(*source_id)))
                .left_join(schema::message_content::table)
                .limit(1)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        }).map(|mut v| v.pop())
    }

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>> {
        let mut vec = self.fetch_messages(|conn| {
            Ok(schema::message::table
                .filter(schema::message::columns::chat_id.eq(chat.id))
                .filter(schema::message::columns::internal_id.eq(*internal_id))
                .left_join(schema::message_content::table)
                .limit(1)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        if vec.is_empty() { Ok(None) } else { Ok(vec.drain(..).next()) }
    }
}

//
// Helpers
//

type UserCache = HashMap<PbUuid, UserCacheForDataset>;

pub struct UserCacheForDataset {
    pub myself: User,
    pub user_by_id: HashMap<UserId, User>,
}

#[derive(Default)]
pub struct SqliteCache {
    pub initialized: bool,
    pub datasets: Vec<Dataset>,
    pub users: UserCache,
}

impl SqliteCache {
    fn new_wrapped() -> Box<RefCell<Self>> {
        Box::new(RefCell::new(SqliteCache { initialized: false, ..Default::default() }))
    }
}

impl std::hash::Hash for PbUuid {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.value.hash(hasher)
    }
}

impl Eq for PbUuid {}

fn chat_root_rel_path(chat_id: i64) -> String {
    format!("chat_{chat_id}")
}

/// Subpath inside a directory, suffixed by " / " to be concatenated.
struct Subpath {
    path_fragment: &'static str,
    use_hashing: bool,
}

mod subpaths {
    use super::Subpath;

    pub(super) static ROOT: Subpath = Subpath { path_fragment: "", use_hashing: false };
    pub(super) static PHOTOS: Subpath = Subpath { path_fragment: "photos", use_hashing: true };
    pub(super) static STICKERS: Subpath = Subpath { path_fragment: "stickers", use_hashing: true };
    pub(super) static VOICE_MESSAGES: Subpath = Subpath { path_fragment: "voice_messages", use_hashing: false };
    pub(super) static AUDIOS: Subpath = Subpath { path_fragment: "audios", use_hashing: true };
    pub(super) static VIDEO_MESSAGES: Subpath = Subpath { path_fragment: "video_messages", use_hashing: true };
    pub(super) static VIDEOS: Subpath = Subpath { path_fragment: "videos", use_hashing: true };
    pub(super) static FILES: Subpath = Subpath { path_fragment: "files", use_hashing: false };
}

fn copy_file(src_rel_path: &str,
             thumbnail_dst_main_path: &Option<String>,
             subpath: &Subpath,
             chat_id: i64,
             src_ds_root: &DatasetRoot,
             dst_ds_root: &DatasetRoot) -> Result<Option<String>> {
    let src_file = src_ds_root.to_absolute(src_rel_path);
    let src_absolute_path = path_to_str(&src_file)?;
    let src_meta = fs::metadata(&src_file);
    if let Ok(src_meta) = src_meta {
        require!(src_meta.is_file(), "Not a file: {src_absolute_path}");
        let ext_suffix = src_file.extension().map(|ext| format!(".{}", ext.to_str().unwrap())).unwrap_or_default();

        let name: String = match thumbnail_dst_main_path {
            Some(main_path) => {
                let main_file = src_ds_root.to_absolute(main_path);
                let full_name = main_file.file_name().unwrap().to_str().unwrap();
                let base_name = if let Some(ext) = main_file.extension() {
                    // Removing extension and a dot
                    full_name.smart_slice(..-(ext.to_str().unwrap().len() as i32 + 1))
                } else {
                    full_name
                };
                require!(!base_name.is_empty());
                format!("{base_name}_thumb{ext_suffix}")
            }
            _ if subpath.use_hashing => {
                let hash = file_hash(&src_file)?;
                format!("{hash}{ext_suffix}")
            }
            None =>
                src_file.file_name().unwrap().to_str().unwrap().to_owned()
        };
        require!(!name.is_empty(), "Filename empty: ${src_absolute_path}");

        let dst_rel_path = format!("{}/{}/{}", chat_root_rel_path(chat_id), subpath.path_fragment, name);
        let dst_file = dst_ds_root.to_absolute(&dst_rel_path);
        fs::create_dir_all(dst_file.parent().unwrap()).context("Can't create dataset root path")?;

        if dst_file.exists() {
            // Assume hash collisions don't exist
            require!(subpath.use_hashing || fs::read(&src_file)? == fs::read(&dst_file)?,
                     "File already exists: {}, and it doesn't match source {}",
                     path_to_str(&dst_file)?, src_absolute_path)
        } else {
            fs::copy(src_file, dst_file)?;
        }

        Ok(Some(dst_rel_path))
    } else {
        log::info!("Referenced file does not exist: ${src_rel_path}");
        Ok(None)
    }
}
