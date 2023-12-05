use std::cell::{RefCell};
use std::default::Default;
use std::fs;
use std::ops::{DerefMut};
use std::path::{Path, PathBuf};
use chrono::Local;

use diesel::{delete, insert_into, sql_query, sql_types, update};
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
    pub name: String,
    pub db_file: PathBuf,
    conn: RefCell<SqliteConnection>,
    cache: DaoCache,
}

impl SqliteDao {
    pub const FILENAME: &'static str = "data.sqlite";

    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./resources/main/migrations");

    pub fn create(db_file: &Path) -> Result<Self> {
        require!(!db_file.exists(), "File {} already exists!", path_to_str(db_file)?);
        Self::create_load_inner(db_file)
    }

    #[allow(unused)]
    pub fn load(db_file: &Path) -> Result<Self> {
        require!(db_file.exists(), "File {} does not exist!", path_to_str(db_file)?);
        Self::create_load_inner(db_file)
    }

    fn check_db_file_path(db_file: &Path) -> EmptyRes {
        require!(db_file.parent().is_some_and(|p| p.exists()),
            "Parent directory for {} does not exist!", path_to_str(db_file)?);
        require!(path_file_name(db_file)? == SqliteDao::FILENAME,
            "Incorrect file name for {}, expected {}", path_to_str(db_file)?, SqliteDao::FILENAME);
        Ok(())
    }

    fn create_load_inner(db_file: &Path) -> Result<Self> {
        Self::check_db_file_path(db_file)?;
        let absolute_path = fs::canonicalize(db_file.parent().unwrap())?.join(path_file_name(db_file)?);
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
            name: format!("{} database", path_file_name(db_file.parent().unwrap())?),
            db_file: db_file.to_path_buf(),
            conn,
            cache: DaoCache::new(),
        })
    }

    pub fn backup_path(&self) -> PathBuf {
        self.storage_path().join(BACKUPS_DIR_NAME)
    }

    fn choose_final_backup_path(&self, ext_suffix: &str) -> Result<PathBuf> {
        let backup_path = self.backup_path();
        let now_str = Local::now().format("%Y-%m-%d_%H-%M-%S");
        let name = format!("{BACKUP_NAME_PREFIX}{now_str}{ext_suffix}");
        let result = backup_path.join(&name);
        if !result.exists() {
            Ok(result)
        } else {
            let mut suffix = 2;
            loop {
                let name = format!("{BACKUP_NAME_PREFIX}{now_str}_{suffix}{ext_suffix}");
                let result = backup_path.join(&name);
                if !result.exists() { break Ok(result); }
                suffix += 1;
            }
        }
    }

    pub fn copy_all_from(&self, src: &dyn ChatHistoryDao) -> EmptyRes {
        measure(|| {
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

                    let src_ds_root = src.dataset_root(ds_uuid)?;
                    let dst_ds_root = self.dataset_root(ds_uuid)?;

                    for src_cwd in src.chats(ds_uuid)?.iter() {
                        require!(src_cwd.chat.id > 0, "IDs should be positive!");
                        require!(src_cwd.chat.member_ids.first() == Some(&src_myself.id),
                                 "First member of chat {} was not myself!", src_cwd.chat.qualified_name());

                        self.conn.borrow_mut().transaction(|txn| {
                            let mut raw_chat = utils::chat::serialize(&src_cwd.chat, &raw_ds.uuid)?;
                            if let Some(ref img) = src_cwd.chat.img_path_option {
                                raw_chat.img_path =
                                    copy_file(img, &None, &subpaths::ROOT,
                                              src_cwd.chat.id, &src_ds_root, &dst_ds_root)?;
                            }
                            insert_into(chat::table).values(raw_chat).execute(txn)?;
                            insert_into(chat_member::table)
                                .values(src_cwd.chat.member_ids.iter()
                                    .enumerate()
                                    .map(|(order, &user_id)|
                                        RawChatMember {
                                            ds_uuid: raw_ds.uuid.clone(),
                                            chat_id: src_cwd.chat.id,
                                            user_id,
                                            order: order as i32,
                                        })
                                    .collect_vec())
                                .execute(txn)?;
                            Ok::<_, anyhow::Error>(())
                        })?;

                        const BATCH_SIZE: usize = 5_000;
                        let mut offset: usize = 0;
                        loop {
                            let src_msgs = src.scroll_messages(&src_cwd.chat, offset, BATCH_SIZE)?;

                            // Copy messages
                            self.conn.borrow_mut().transaction(|txn| {
                                self.copy_messages(txn, &src_msgs, src_cwd.chat.id,
                                                   &raw_ds.uuid, &src_ds_root, &dst_ds_root)
                            })?;

                            if src_msgs.len() < BATCH_SIZE { break; }
                            offset += BATCH_SIZE;
                        }
                    }
                    Ok(())
                }, |_, t| log::info!("Dataset '{}' inserted in {t} ms", ds_uuid.value))?;
            }

            self.invalidate_cache()?;

            require!(src_datasets.len() == self.datasets()?.len(), "Datasets have different sizes after merge!");

            for src_ds in src_datasets.iter() {
                let ds_uuid = src_ds.uuid();
                ensure_datasets_are_equal(src, ds_uuid, self, ds_uuid)?;
            }

            Ok(())
        }, |_, t| log::info!("Dao '{}' fully copied {t} ms", src.name()))
    }

    fn fetch_messages<F>(&self, get_raw_messages_with_content: F) -> Result<Vec<Message>>
        where F: Fn(&mut SqliteConnection) -> Result<Vec<(RawMessage, Option<RawMessageContent>)>>
    {
        utils::message::fetch(self.conn.borrow_mut().deref_mut(), get_raw_messages_with_content)
    }

    fn copy_messages(&self,
                     conn: &mut SqliteConnection,
                     src_msgs: &[Message],
                     chat_id: i64,
                     raw_uuid: &[u8],
                     src_ds_root: &DatasetRoot,
                     dst_ds_root: &DatasetRoot) -> EmptyRes {
        let full_raw_msgs: Vec<FullRawMessage> = src_msgs.iter()
            .map(|m| utils::message::serialize_and_copy_files(m, chat_id, raw_uuid, src_ds_root, dst_ds_root))
            .try_collect()?;

        // Don't see a way around cloning here.
        let raw_messages = full_raw_msgs.iter().map(|full| full.m.clone()).collect_vec();

        // Even though SQLite supports RETURNING clause and Diesel claims to support it too,
        // it's not possible to INSERT RETURNING multiple values due to
        // https://stackoverflow.com/a/77488801/466646
        // To work around that, we have to do a separate SELECT.
        use schema::*;
        insert_into(message::table).values(&raw_messages).execute(conn)?;
        let mut internal_ids: Vec<i64> = message::table
            .order_by(message::columns::internal_id.desc())
            .limit(raw_messages.len() as i64)
            .select(message::columns::internal_id)
            .load(conn)?;
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

        insert_into(message_content::table).values(raw_mcs).execute(conn)?;
        insert_into(message_text_element::table).values(raw_rtes).execute(conn)?;
        Ok(())
    }
}

impl WithCache for SqliteDao {
    fn get_cache_unchecked(&self) -> &DaoCache { &self.cache }

    fn init_cache(&self, inner: &mut DaoCacheInner) -> EmptyRes {
        use schema::*;
        inner.datasets =
            dataset::table
                .select(RawDataset::as_select())
                .load_iter(self.conn.borrow_mut().deref_mut())?
                .flatten()
                .map(utils::dataset::deserialize)
                .try_collect()?;

        let ds_uuids = inner.datasets.iter().map(|ds| ds.uuid.clone().unwrap()).collect_vec();
        for ds_uuid in ds_uuids {
            let uuid = Uuid::parse_str(&ds_uuid.value)?;
            let rows: Vec<(User, bool)> = user::table
                .filter(user::columns::ds_uuid.eq(uuid.as_ref()))
                .select(RawUser::as_select())
                .load_iter(self.conn.borrow_mut().deref_mut())?
                .flatten()
                .map(utils::user::deserialize)
                .try_collect()?;
            let (mut myselves, mut users): (Vec<_>, Vec<_>) =
                rows.into_iter().partition_map(|(users, is_myself)|
                    if is_myself { Either::Left(users) } else { Either::Right(users) });
            require!(!myselves.is_empty(), "Myself not found!");
            require!(myselves.len() < 2, "More than one myself found!");
            let myself = myselves.remove(0);
            users.insert(0, myself.clone());
            inner.users.insert(ds_uuid, UserCacheForDataset {
                myself,
                user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
            });
        }

        Ok(())
    }
}

impl ChatHistoryDao for SqliteDao {
    fn name(&self) -> &str {
        &self.name
    }

    fn storage_path(&self) -> &Path {
        self.db_file.parent().unwrap()
    }

    fn dataset_root(&self, ds_uuid: &PbUuid) -> Result<DatasetRoot> {
        Ok(DatasetRoot(self.db_file.parent().expect("Database file has no parent!").join(&ds_uuid.value).to_path_buf()))
    }

    fn chats_inner(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let cache = self.get_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let rows: Vec<ChatWithDetails> =
            utils::chat::select_by_ds(&uuid, conn)?
                .into_iter()
                .map(|raw: RawChatQ| utils::chat::deserialize(raw, conn, ds_uuid, &cache))
                .try_collect()?;

        Ok(rows)
    }

    fn chat_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let cache = self.get_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let mut rows: Vec<ChatWithDetails> =
            utils::chat::select_by_ds_and_id(&uuid, id, conn)?
                .into_iter()
                .map(|raw: RawChatQ| utils::chat::deserialize(raw, conn, ds_uuid, &cache))
                .try_collect()?;

        if rows.is_empty() { Ok(None) } else { Ok(Some(rows.remove(0))) }
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        self.fetch_messages(|conn| {
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .order_by(message::columns::internal_id.asc())
                .left_join(message_content::table)
                .offset(offset as i64)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        let mut msgs = self.fetch_messages(|conn| {
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .order_by(message::columns::internal_id.desc())
                .left_join(message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        msgs.reverse();
        Ok(msgs)
    }

    fn messages_before_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let mut msgs = self.fetch_messages(|conn| {
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .filter(message::columns::internal_id.lt(*msg_id))
                .order_by(message::columns::internal_id.desc())
                .left_join(message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        msgs.reverse();
        Ok(msgs)
    }

    fn messages_after_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        self.fetch_messages(|conn| {
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .filter(message::columns::internal_id.gt(*msg_id))
                .order_by(message::columns::internal_id.asc())
                .left_join(message_content::table)
                .limit(limit as i64)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })
    }

    fn messages_slice(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<Vec<Message>> {
        if *msg1_id > *msg2_id {
            return Ok(vec![]);
        }
        // To avoid getting an entire huge slice, do this in batches
        const BATCH_SIZE: usize = 5_000;
        let mut result = Vec::with_capacity((*msg2_id - *msg1_id) as usize);
        let fetch_batch = |first_id: MessageInternalId| {
            self.fetch_messages(|conn| {
                use schema::*;
                Ok(message::table
                    .filter(message::columns::chat_id.eq(chat.id))
                    .filter(message::columns::internal_id.ge(*first_id))
                    .filter(message::columns::internal_id.le(*msg2_id))
                    .order_by(message::columns::internal_id.asc())
                    .left_join(message_content::table)
                    .limit(BATCH_SIZE as i64)
                    .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                    .load(conn)?)
            })
        };
        result.extend(fetch_batch(msg1_id)?);
        loop {
            let last_id = result.last().unwrap().internal_id();
            if last_id == msg2_id { break; }
            // Need to skip first element since it's already in the list.
            result.extend(fetch_batch(last_id)?.into_iter().skip(1));
        }
        Ok(result)
    }

    fn messages_slice_len(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<usize> {
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        use schema::*;
        let count: i64 = message::table
            .filter(message::columns::chat_id.eq(chat.id))
            .filter(message::columns::internal_id.ge(*msg1_id))
            .filter(message::columns::internal_id.le(*msg2_id))
            .order_by(message::columns::internal_id.asc())
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
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .filter(message::columns::source_id.eq(Some(*source_id)))
                .left_join(message_content::table)
                .limit(1)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        }).map(|mut v| v.pop())
    }

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>> {
        let mut vec = self.fetch_messages(|conn| {
            use schema::*;
            Ok(message::table
                .filter(message::columns::chat_id.eq(chat.id))
                .filter(message::columns::internal_id.eq(*internal_id))
                .left_join(message_content::table)
                .limit(1)
                .select((RawMessage::as_select(), Option::<RawMessageContent>::as_select()))
                .load(conn)?)
        })?;
        if vec.is_empty() { Ok(None) } else { Ok(vec.drain(..).next()) }
    }

    fn as_mutable(&mut self) -> Result<&mut dyn MutableChatHistoryDao> {
        Ok(self)
    }

    fn as_shiftable(&mut self) -> Result<&mut dyn ShiftableChatHistoryDao> {
        // H2 query:
        // UPDATE messages SET
        //   time      = DATEADD(HOUR, $hrs, time),
        //   edit_time = DATEADD(HOUR, $hrs, edit_time)
        // WHERE ds_uuid = ${dsUuid}
        err!("Dataset time cannot be shifted for a persistent DB")
    }
}

impl MutableChatHistoryDao for SqliteDao {
    fn backup(&mut self) -> Result<JoinHandle<()>> {
        // Diesel does not expose backup API, so we use rusqlite for that.
        use rusqlite::*;
        use std::io::Write;

        const PAGES_PER_STEP: std::ffi::c_int = 1024;
        const PAUSE_BETWEEN_PAGES: std::time::Duration = std::time::Duration::ZERO;
        const MAX_BACKUPS: usize = 3;

        measure(|| {
            let backup_path = self.backup_path();
            if !backup_path.exists() {
                fs::create_dir(&backup_path)?;
            }

            let filename = path_file_name(&self.db_file)?;
            let backup_file = backup_path.join(filename);
            require!(!backup_file.exists(), "File {filename} already exists in the backups dir, last backup was incomplete?");

            {
                let src_conn = Connection::open(&self.db_file)?;
                let mut dst_conn = Connection::open(&backup_file)?;
                let backup = backup::Backup::new(&src_conn, &mut dst_conn)?;
                backup.run_to_completion(PAGES_PER_STEP, PAUSE_BETWEEN_PAGES, None)?;
            }

            let list_backups = move || Ok::<_, anyhow::Error>(list_all_files(&backup_path, false)?
                .into_iter()
                .filter(|f| {
                    let name = path_file_name(f).unwrap();
                    name.starts_with(BACKUP_NAME_PREFIX) && name.ends_with(".zip")
                })
                .sorted()
                .collect_vec());

            let archive_path = self.choose_final_backup_path(".zip")?;

            let zip_jh = std::thread::spawn(move || {
                // Wrapping logic in a closure to allow using ? operator
                let inner = || -> EmptyRes {
                    // TODO: This is (relatively) slow, Deflated-compression of ~170 MB DB takes ~6 sec on my machine
                    //       in release mode. Can we do better?
                    {
                        let backup_bytes = fs::read(&backup_file)?;
                        let mut archive = fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(archive_path)?;
                        let mut zip = zip::ZipWriter::new(&mut archive);

                        let options = zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
                        zip.start_file(path_file_name(&backup_file)?, options)?;
                        let mut buf = backup_bytes.as_slice();
                        while !buf.is_empty() {
                            let res = zip.write(buf)?;
                            require!(res != 0, "Failed writing a backup, zip file no longer accepts source bytes!");
                            buf = &buf[res..]
                        }
                        zip.finish()?;
                    }

                    fs::remove_file(&backup_file)?;

                    for f in list_backups()?.iter().rev().skip(MAX_BACKUPS) {
                        fs::remove_file(f)?;
                    }
                    Ok(())
                };
                measure(|| {
                    // Panic in case something bad happens
                    inner().expect("Backup compression failed!");
                }, |_, t| log::info!("Backup compressed in {t} ms"))
            });

            Ok(zip_jh)
        }, |_, t| log::info!("Backup done in {t} ms"))
    }

    fn insert_dataset(&mut self, ds: Dataset) -> Result<Dataset> {
        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let raw_ds = utils::dataset::serialize(&ds);

        insert_into(schema::dataset::dsl::dataset)
            .values(raw_ds)
            .execute(conn)?;

        Ok(ds)
    }

    fn update_dataset(&mut self, ds: Dataset) -> Result<Dataset> {
        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let raw_ds = utils::dataset::serialize(&ds);

        let ds_uuid = ds.uuid.clone().unwrap();
        let uuid = Uuid::parse_str(&ds_uuid.value).expect("Invalid UUID!");

        use schema::*;
        let updated_rows = update(dataset::dsl::dataset)
            .filter(dataset::columns::uuid.eq(uuid.as_ref()))
            .set(raw_ds)
            .execute(conn)?;
        require!(updated_rows == 1, "{updated_rows} rows changed when updaing dataset {:?}", ds);

        Ok(ds)
    }

    fn delete_dataset(&mut self, ds_uuid: PbUuid) -> EmptyRes {
        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let uuid = Uuid::parse_str(&ds_uuid.value).expect("Invalid UUID!");
        let ds_root = self.dataset_root(&ds_uuid)?;

        use schema::*;

        conn.transaction(|conn| {
            let mut delete_by_ds_uuid = |sql: &str| -> QueryResult<usize> {
                sql_query(sql)
                    .bind::<sql_types::Binary, _>(uuid.as_ref())
                    .execute(conn)
            };

            // Messages
            delete_by_ds_uuid(r"
                DELETE FROM message_content
                WHERE message_internal_id IN (
                    SELECT internal_id FROM message
                    WHERE ds_uuid = ?
                )
            ")?;
            delete_by_ds_uuid(r"
                DELETE FROM message_text_element
                WHERE message_internal_id IN (
                    SELECT internal_id FROM message
                    WHERE ds_uuid = ?
                )
            ")?;
            delete(message::dsl::message)
                .filter(message::columns::ds_uuid.eq(uuid.as_ref()))
                .execute(conn)?;

            // Chats
            delete(chat_member::dsl::chat_member)
                .filter(chat_member::columns::ds_uuid.eq(uuid.as_ref()))
                .execute(conn)?;
            delete(chat::dsl::chat)
                .filter(chat::columns::ds_uuid.eq(uuid.as_ref()))
                .execute(conn)?;

            // Users
            delete(user::dsl::user)
                .filter(user::columns::ds_uuid.eq(uuid.as_ref()))
                .execute(conn)?;

            // Finally, dataset itself
            let deleted_rows = delete(dataset::dsl::dataset)
                .filter(dataset::columns::uuid.eq(uuid.as_ref()))
                .execute(conn)?;
            require!(deleted_rows == 1, "{deleted_rows} rows changed when deleting dataset with UUID {:?}", ds_uuid);

            // Moving all dataset files to backup directory
            if ds_root.0.exists() {
                let target = self.choose_final_backup_path("")?.join(path_file_name(&ds_root.0)?);
                fs::create_dir_all(&target)?;
                fs::rename(&ds_root.0, &target)?;
            }

            Ok(())
        })
    }

    fn insert_user(&mut self, user: User, is_myself: bool) -> Result<User> {
        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let uuid = Uuid::parse_str(&user.ds_uuid.as_ref().unwrap().value).expect("Invalid UUID!");
        let raw_user = utils::user::serialize(&user, is_myself, &Vec::from(uuid.as_ref()));

        insert_into(schema::user::dsl::user)
            .values(raw_user)
            .execute(conn)?;

        Ok(user)
    }

    fn update_user(&mut self, user: User) -> Result<User> {
        let ds_uuid = user.ds_uuid.clone().unwrap();
        let is_myself = user.id() == self.myself(&ds_uuid)?.id();

        let old_name = self.get_cache()?.users[&ds_uuid].user_by_id[&user.id()].pretty_name_option();

        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let uuid = Uuid::parse_str(&ds_uuid.value).expect("Invalid UUID!");
        let raw_user = utils::user::serialize(&user, is_myself, &Vec::from(uuid.as_ref()));

        conn.transaction(|conn| {
            use schema::*;
            let updated_rows = update(user::dsl::user)
                .filter(user::columns::ds_uuid.eq(uuid.as_ref()))
                .filter(user::columns::id.eq(user.id))
                .set(raw_user)
                .execute(conn)?;
            require!(updated_rows == 1, "{updated_rows} rows changed when updaing user {:?}", user);

            // After changing user, rename private chat(s) with him accordingly. If user is self, do nothing.
            if !is_myself {
                let chat_ids: Vec<i64> = chat_member::table
                    .filter(chat_member::columns::ds_uuid.eq(uuid.as_ref()))
                    .filter(chat_member::columns::user_id.eq(user.id))
                    .select(chat_member::columns::chat_id)
                    .load(conn)?;

                use utils::EnumSerialization;
                update(chat::dsl::chat)
                    .filter(chat::columns::ds_uuid.eq(uuid.as_ref()))
                    .filter(chat::columns::id.eq_any(chat_ids))
                    .filter(chat::columns::tpe.eq(ChatType::serialize(ChatType::Personal as i32)?))
                    .set(chat::columns::name.eq(user.pretty_name_option()))
                    .execute(conn)?;
            }

            // Update user name in "members" string field

            if let Some(old_name) = old_name {
                let new_name = user.pretty_name();

                let old_mc_members: Vec<(i64, Option<String>)> = message_content::table
                    .inner_join(message::table)
                    .inner_join(chat::table
                        .on(chat::columns::ds_uuid.eq(message::columns::ds_uuid)
                            .and(chat::columns::id.eq(message::columns::chat_id))))
                    .inner_join(chat_member::table
                        .on(chat_member::columns::ds_uuid.eq(chat::columns::ds_uuid)
                            .and(chat_member::columns::chat_id.eq(chat::columns::id))))
                    .filter(chat::columns::ds_uuid.eq(uuid.as_ref()))
                    .filter(chat_member::columns::user_id.eq(user.id))
                    .filter(message_content::columns::members.like(format!("%{old_name}%")))
                    .select((message_content::columns::id, message_content::columns::members))
                    .load(conn)?;

                for (id, members_string) in old_mc_members {
                    let new_members_string = utils::serialize_arr(&utils::deserialize_arr(members_string.unwrap())
                        .into_iter()
                        .map(|s| if s == old_name { new_name.clone() } else { s })
                        .collect_vec());

                    update(message_content::table)
                        .filter(message_content::columns::id.eq(id))
                        .set(message_content::columns::members.eq(new_members_string))
                        .execute(conn)?;
                }
            }

            Ok(())
        })?;

        Ok(user)
    }

    fn insert_chat(&mut self, mut chat: Chat, src_ds_root: &DatasetRoot) -> Result<Chat> {
        if let Some(ref img) = chat.img_path_option {
            let dst_ds_root = self.dataset_root(chat.ds_uuid())?;
            chat.img_path_option = copy_file(img, &None, &subpaths::ROOT,
                                             chat.id, src_ds_root, &dst_ds_root)?;
        }

        let uuid = Uuid::parse_str(&chat.ds_uuid.as_ref().unwrap().value).expect("Invalid UUID!");
        let uuid_bytes = Vec::from(uuid.as_ref());
        let raw_chat = utils::chat::serialize(&chat, &uuid_bytes)?;

        let myself = self.myself(chat.ds_uuid())?;
        require!(chat.member_ids.first() == Some(&myself.id),
                 "First member of chat {} was not myself!", chat.qualified_name());

        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();
        insert_into(schema::chat::dsl::chat)
            .values(raw_chat)
            .execute(conn)?;

        let chat_members = chat.member_ids.iter().enumerate().map(|(order, &user_id)| RawChatMember {
            ds_uuid: uuid_bytes.clone(),
            chat_id: chat.id,
            user_id,
            order: order as i32,
        }).collect_vec();

        insert_into(schema::chat_member::dsl::chat_member)
            .values(chat_members)
            .execute(conn)?;

        Ok(chat)
    }

    fn update_chat(&mut self, chat: Chat) -> Result<Chat> {
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let uuid = Uuid::parse_str(&chat.ds_uuid.as_ref().unwrap().value).expect("Invalid UUID!");
        let uuid_bytes = Vec::from(uuid.as_ref());
        let raw_chat = utils::chat::serialize(&chat, &uuid_bytes)?;

        use schema::*;
        let updated_rows = update(chat::dsl::chat)
            .filter(chat::columns::ds_uuid.eq(uuid.as_ref()))
            .filter(chat::columns::id.eq(chat.id))
            .set(raw_chat)
            .execute(conn)?;
        require!(updated_rows == 1, "{updated_rows} rows changed when updaing chat {:?}", chat);

        Ok(chat)
    }

    fn delete_chat(&mut self, chat: Chat) -> EmptyRes {
        self.invalidate_cache()?;
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let ds_uuid = chat.ds_uuid.clone().unwrap();
        let uuid = Uuid::parse_str(&ds_uuid.value).expect("Invalid UUID!");
        let ds_root = self.dataset_root(&ds_uuid)?;

        use schema::*;

        conn.transaction(|conn| {
            let delete_by_ds_and_chat = |sql: &str, conn: &mut SqliteConnection| -> QueryResult<usize> {
                sql_query(sql)
                    .bind::<sql_types::Binary, _>(uuid.as_ref())
                    .bind::<sql_types::BigInt, _>(chat.id)
                    .execute(conn)
            };

            // Selecting all paths in advance
            let mut relative_paths = sql_query(r"
                SELECT mc.path, mc.thumbnail_path FROM message_content mc
                WHERE mc.message_internal_id IN (
                    SELECT internal_id FROM message
                    WHERE ds_uuid = ? AND chat_id = ?
                )
            ")
                .bind::<sql_types::Binary, _>(uuid.as_ref())
                .bind::<sql_types::BigInt, _>(chat.id)
                .load::<PathsWrapper>(conn)?
                .into_iter()
                .flat_map(|p| vec![p.path, p.thumbnail_path])
                .flatten()
                .collect_vec();

            if let Some(ref img_path) = chat.img_path_option {
                relative_paths.push(img_path.clone());
            }

            // Messages
            delete_by_ds_and_chat(r"
                DELETE FROM message_content
                WHERE message_internal_id IN (
                    SELECT internal_id FROM message
                    WHERE ds_uuid = ? AND chat_id = ?
                )
            ", conn)?;
            delete_by_ds_and_chat(r"
                DELETE FROM message_text_element
                WHERE message_internal_id IN (
                    SELECT internal_id FROM message
                    WHERE ds_uuid = ? AND chat_id = ?
                )
            ", conn)?;
            delete(message::dsl::message)
                .filter(message::columns::ds_uuid.eq(uuid.as_ref()))
                .filter(message::columns::chat_id.eq(chat.id))
                .execute(conn)?;

            // Chat
            delete(chat_member::dsl::chat_member)
                .filter(chat_member::columns::ds_uuid.eq(uuid.as_ref()))
                .filter(chat_member::columns::chat_id.eq(chat.id))
                .execute(conn)?;
            let deleted_rows = delete(chat::dsl::chat)
                .filter(chat::columns::ds_uuid.eq(uuid.as_ref()))
                .filter(chat::columns::id.eq(chat.id))
                .execute(conn)?;
            require!(deleted_rows == 1, "{deleted_rows} rows changed when deleting chat {:?}", chat);

            // Orphan users
            sql_query(r"
                DELETE FROM user
                WHERE ds_uuid = ? AND id NOT IN (
                    SELECT cm.user_id FROM chat_member cm
                    WHERE cm.ds_uuid = user.ds_uuid
                )
            ").bind::<sql_types::Binary, _>(uuid.as_ref()).execute(conn)?;

            // Moving all dataset files to backup directory
            let backup_ds_root = self.choose_final_backup_path("")?.join(path_file_name(&ds_root.0)?);
            for relative in relative_paths {
                let src = ds_root.to_absolute(&relative);
                if src.exists() {
                    let dst = backup_ds_root.join(&relative);
                    fs::create_dir_all(dst.parent().unwrap())?;
                    fs::rename(src, dst)?;
                }
            }

            Ok(())
        })
    }

    fn insert_messages(&mut self, msgs: Vec<Message>, chat: &Chat, src_ds_root: &DatasetRoot) -> EmptyRes {
        let mut conn = self.conn.borrow_mut();
        let conn = conn.deref_mut();

        let dst_ds_root = self.dataset_root(chat.ds_uuid())?;
        let uuid = Uuid::parse_str(&chat.ds_uuid.as_ref().unwrap().value).expect("Invalid UUID!");
        let uuid_bytes = Vec::from(uuid.as_ref());

        self.copy_messages(conn, &msgs, chat.id,
                           &uuid_bytes, src_ds_root, &dst_ds_root)?;

        Ok(())
    }
}

//
// Helpers
//

const BACKUPS_DIR_NAME: &str = "_backups";
const BACKUP_NAME_PREFIX: &str = "backup_";

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
        require!(!name.is_empty(), "Filename empty: {src_absolute_path}");

        let dst_rel_path = format!("{}/{}/{}", chat_root_rel_path(chat_id), subpath.path_fragment, name);
        let dst_file = dst_ds_root.to_absolute(&dst_rel_path);
        fs::create_dir_all(dst_file.parent().unwrap()).context("Can't create dataset root path")?;

        if dst_file.exists() {
            // Assume hash collisions don't exist
            require!(subpath.use_hashing || files_are_equal(&src_file, &dst_file)?,
                     "File already exists: {}, and it doesn't match source {}",
                     path_to_str(&dst_file)?, src_absolute_path)
        } else {
            fs::copy(src_file, dst_file)?;
        }

        Ok(Some(dst_rel_path))
    } else {
        log::info!("Referenced file does not exist: {src_rel_path}");
        Ok(None)
    }
}
