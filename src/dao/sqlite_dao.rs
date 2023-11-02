use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use itertools::Either;

use lazy_static::lazy_static;
use refinery::embed_migrations;
use rusqlite::{Connection, named_params, OpenFlags, OptionalExtension};
use rusqlite_from_row::FromRow;
use uuid::Uuid;

use crate::*;
use crate::dao::sqlite_dao::mapping::*;

use super::*;

mod mapping;

pub struct SqliteDao {
    name: String,
    db_file: PathBuf,
    conn: Connection,
    cache: Option<Box<RefCell<SqliteCache>>>,
}

lazy_static! {
    static ref DEFAULT_CONN_FLAGS: OpenFlags =
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
}

embed_migrations!("resources/main/db_migrations");

impl SqliteDao {
    const FILENAME: &'static str = "data.sqlite";

    pub fn create(db_file: PathBuf) -> Result<Self> {
        require!(!db_file.exists(), "File {} already exists!", db_file.to_str().unwrap());
        Self::check_db_file_path(&db_file)?;
        let conn = Connection::open_with_flags(&db_file, *DEFAULT_CONN_FLAGS | OpenFlags::SQLITE_OPEN_CREATE)?;
        Self::load_inner(db_file, conn)
    }

    pub fn load(db_file: PathBuf) -> Result<Self> {
        require!(db_file.exists(), "File {} does not exist!", db_file.to_str().unwrap());
        Self::check_db_file_path(&db_file)?;
        let conn = Connection::open_with_flags(&db_file, *DEFAULT_CONN_FLAGS)?;
        Self::load_inner(db_file, conn)
    }

    fn check_db_file_path(db_file: &Path) -> EmptyRes {
        require!(db_file.parent().is_some_and(|p| p.exists()),
            "Parent directory for {} does not exist!", db_file.to_str().unwrap());
        require!(path_file_name(db_file)? == SqliteDao::FILENAME,
            "Incorrect file name for {}, expected {}", db_file.to_str().unwrap(), SqliteDao::FILENAME);
        Ok(())
    }

    fn load_inner(db_file: PathBuf, mut conn: Connection) -> Result<Self> {
        migrations::runner().run(&mut conn)?;
        Ok(SqliteDao {
            name: format!("{} database", path_file_name(&db_file)?),
            db_file,
            conn,
            cache: None,
        })
    }

    /// Lazily initialize cache and return the reference to it.
    fn cache(&self) -> Result<Ref<SqliteCache>> {
        if self.cache.is_none() {
            let cache = SqliteCache::new_wrapped();

            {
                let mut stmt = self.conn.prepare("SELECT ds.* FROM dataset ds")?;
                let rows = stmt
                    .query_and_then([], RawDataset::try_from_row)?
                    .flatten()
                    .map(queries::dataset::materialize)
                    .try_collect()?;
                cache.borrow_mut().datasets = rows;
            }

            let ds_uuids = cache.deref().borrow().datasets.iter().map(|ds| ds.uuid.clone().unwrap()).collect_vec();
            let users_cache = &mut cache.borrow_mut().users;
            for ds_uuid in ds_uuids {
                let mut stmt = self.conn.prepare("SELECT u.* FROM user u WHERE u.ds_uuid = ?1")?;
                let rows: Vec<(User, bool)> = stmt
                    .query_and_then([], RawUser::try_from_row)?
                    .flatten()
                    .map(queries::user::materialize)
                    .try_collect()?;
                let (mut myselves, users): (Vec<_>, Vec<_>) =
                    rows.into_iter().partition_map(|(users, is_myself)|
                        if is_myself { Either::Left(users) } else { Either::Right(users) });
                require!(myselves.len() > 0, "Myself not found!");
                require!(myselves.len() < 2, "More than one myself found!");
                users_cache.insert(ds_uuid, UserCacheForDataset {
                    myself: myselves.remove(0),
                    user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
                });
            }
        }
        Ok(self.cache.as_ref().unwrap().deref().borrow())
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
        DatasetRoot(self.db_file.parent().expect("Database file has no parent!").to_path_buf())
    }

    fn myself(&self, ds_uuid: &PbUuid) -> Result<User> {
        Ok(self.cache()?.borrow().users[ds_uuid].myself.clone())
    }

    fn users(&self, ds_uuid: &PbUuid) -> Result<Vec<User>> {
        Ok(self.cache()?.borrow().users[ds_uuid].user_by_id.values().cloned().collect_vec())
    }

    fn user_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<User>> {
        Ok(self.cache()?.borrow().users[ds_uuid].user_by_id.get(&UserId(id)).cloned())
    }

    fn chats(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let mut stmt = self.conn.prepare_cached(queries::chat::SELECT_BY_DS)?;
        let cache = self.cache()?;
        let cache = cache.deref();
        let rows = stmt
            .query_and_then(named_params! { ":ds_uuid": uuid.as_bytes() }, RawChat::try_from_row)?
            .flatten()
            .map(|raw: RawChat| queries::chat::materialize(raw, ds_uuid, cache))
            .try_collect()?;
        Ok(rows)
    }

    fn chat_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<ChatWithDetails>> {
        let uuid = Uuid::parse_str(&ds_uuid.value)?;
        let mut stmt = self.conn.prepare_cached(queries::chat::SELECT_BY_DS_AND_ID)?;
        let cache = self.cache()?;
        let cache = cache.deref();
        let row = stmt
            .query_row(named_params! { ":ds_uuid": uuid.as_bytes(), ":chat_id": id }, RawChat::try_from_row)
            .optional()?
            .map(|raw: RawChat| queries::chat::materialize(raw, ds_uuid, cache));
        transpose_option_result(row)
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        todo!()
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        todo!()
    }

    fn messages_before_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        todo!()
    }

    fn messages_after_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        todo!()
    }

    fn messages_between_impl(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<Vec<Message>> {
        todo!()
    }

    fn count_messages_between(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<usize> {
        todo!()
    }

    fn messages_around_date(&self, chat: &Chat, date_ts: Timestamp, limit: usize) -> Result<(Vec<Message>, Vec<Message>)> {
        todo!()
    }

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>> {
        todo!()
    }

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>> {
        todo!()
    }
}

//
// Queries
//

mod queries {
    use const_format::*;

    use crate::*;

    use super::*;
    use super::mapping::*;

    pub mod dataset {
        use super::*;

        pub fn materialize(raw: RawDataset) -> Result<Dataset> {
            Ok(Dataset {
                uuid: Some(PbUuid { value: Uuid::from_slice(&raw.uuid)?.to_string() }),
                alias: raw.alias,
                source_type: raw.source_type,
            })
        }
    }

    pub mod user {
        use super::*;

        pub fn materialize(raw: RawUser) -> Result<(User, bool)> {
            Ok((User {
                ds_uuid: Some(PbUuid { value: Uuid::from_slice(&raw.ds_uuid)?.to_string() }),
                id: raw.id,
                first_name_option: raw.first_name,
                last_name_option: raw.last_name,
                username_option: raw.username,
                phone_number_option: raw.phone_numbers,
            }, raw.is_myself))
        }
    }

    pub mod chat {
        use super::*;

        const SELECT: &str =
            r"SELECT
                c.*,
                (
                  SELECT STRING_AGG(u.id) FROM user u
                  INNER JOIN chat_member cm ON cm.ds_uuid = c.ds_uuid AND cm.user_id = u.id
                  WHERE u.ds_uuid = c.ds_uuid AND cm.chat_id = c.id
                ) AS member_ids
            FROM chat c";
        const DS_IS: &str = "c.ds_uuid = :ds_uuid";
        const ID_IS: &str = "c.id = :chat_id";

        pub const SELECT_BY_DS: &str = concatcp!(SELECT, " WHERE ", DS_IS);
        pub const SELECT_BY_DS_AND_ID: &str = concatcp!(SELECT_BY_DS, " AND ", ID_IS);

        pub fn materialize(raw: RawChat, ds_uuid: &PbUuid, cache: &SqliteCache) -> Result<ChatWithDetails> {
            let mut cwd = ChatWithDetails {
                chat: Chat {
                    ds_uuid: Some(ds_uuid.clone()),
                    id: raw.id,
                    name_option: raw.name,
                    tpe: raw.tpe as i32,
                    img_path_option: raw.img_path,
                    member_ids: raw.member_ids.split(',').map(|s| s.parse::<i64>()).try_collect()?,
                    msg_count: raw.msg_count as i32,
                },
                last_msg_option: todo!(),
                members: vec![],
            };
            cwd.members = resolve_users(&cache.users[ds_uuid], cwd.chat.member_ids())?;
            Ok(cwd)
        }

        fn resolve_users(cache: &UserCacheForDataset, user_ids: impl Iterator<Item=UserId>) -> Result<Vec<User>> {
            user_ids
                .map(|id|
                    cache.user_by_id.get(&id)
                        .cloned()
                        .ok_or_else(|| anyhow!("Cannot find user with ID {}", *id))
                )
                .try_collect()
        }
    }

    pub mod message {
        use super::*;

        const SELECT: &str =
            r"SELECT
                m.*,
                mc.*
            FROM message m
            LEFT JOIN message_content mc ON mc.";
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
    pub datasets: Vec<Dataset>,
    pub users: UserCache,
}

impl SqliteCache {
    fn new_wrapped() -> Box<RefCell<Self>> {
        Box::new(RefCell::new(Default::default()))
    }
}

impl std::hash::Hash for PbUuid {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.value.hash(hasher)
    }
}

impl Eq for PbUuid {}
