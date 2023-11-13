use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use rand::Rng;
use uuid::Uuid;

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::protobuf::history::*;

lazy_static! {
    pub static ref BASE_DATE: DateTime<FixedOffset> = dt("2019-01-02 11:15:21", None);

    pub static ref ZERO_UUID: Uuid = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();

    pub static ref ZERO_PB_UUID: PbUuid = PbUuid { value: ZERO_UUID.to_string() };

    pub static ref RESOURCES_DIR: String =
        concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test").replace("//", "/");

    pub static ref MESSAGE_REGULAR_NO_CONTENT: message::Typed = message::Typed::Regular(MessageRegular {
        edit_timestamp_option: None,
        is_deleted: false,
        forward_from_name_option: None,
        reply_to_message_id_option: None,
        content_option: None,
    });
}

pub fn resource(relative_path: &str) -> PathBuf {
    Path::new(RESOURCES_DIR.as_str()).join(relative_path)
}

pub fn dt(s: &str, offset: Option<&FixedOffset>) -> DateTime<FixedOffset> {
    let local = Local::now();
    let offset = offset.unwrap_or(local.offset());
    offset.from_local_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()).unwrap()
}

pub fn random_alphanumeric(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn create_named_file(path: &Path, content: &[u8]) -> EmptyRes {
    let mut file = fs::File::create(&path)?;
    file.write(content)?;
    Ok(())
}

pub fn create_random_named_file(path: &Path) -> EmptyRes {
    create_named_file(path, random_alphanumeric(256).as_bytes())
}

pub fn create_random_file(parent: &Path) -> Result<PathBuf> {
    let path = parent.join(&format!("{}.bin", random_alphanumeric(30)));
    create_random_named_file(&path)?;
    Ok(path)
}

//
// Entity creation helpers
//

pub struct DaoEntities<MsgType> {
    pub dao_holder: InMemoryDaoHolder,
    pub ds: Dataset,
    pub root: DatasetRoot,
    pub users: Vec<User>,
    pub cwd: ChatWithDetails,
    pub msgs: BTreeMap<MessageSourceId, MsgType>,
}

pub struct MergerHelper {
    pub m: DaoEntities<MasterMessage>,
    pub s: DaoEntities<SlaveMessage>,
}

impl MergerHelper {
    const MAX_USER_ID: usize = 3;

    pub fn random_user_id() -> usize {
        let mut rng = rand::thread_rng();
        rng.gen_range(1..=Self::MAX_USER_ID)
    }

    pub fn new_as_is(msgs1: Vec<Message>,
                     msgs2: Vec<Message>) -> Self {
        Self::new(msgs1, msgs2, &|_, _, _| ())
    }

    pub fn new(msgs1: Vec<Message>,
               msgs2: Vec<Message>,
               amend_message: &impl Fn(bool, &DatasetRoot, &mut Message)) -> Self {
        let m =
            create_dao_and_entities(true, "One", msgs1, Self::MAX_USER_ID, amend_message, MasterMessage);
        let s =
            create_dao_and_entities(false, "Two", msgs2, Self::MAX_USER_ID, amend_message, SlaveMessage);
        MergerHelper { m, s }
    }
}

fn create_dao_and_entities<MsgType>(
    is_master: bool,
    name_suffix: &str,
    src_msgs: Vec<Message>,
    num_users: usize,
    amend_message: &impl Fn(bool, &DatasetRoot, &mut Message),
    wrap_message: fn(Message) -> MsgType,
) -> DaoEntities<MsgType> {
    let dao_holder = create_simple_dao(is_master, name_suffix, src_msgs, num_users, amend_message);
    let (ds, root, users, cwd, msgs) =
        get_simple_dao_entities(dao_holder.dao.as_ref());
    let duplicates = msgs.iter().map(|m| m.source_id()).counts().into_iter().filter(|pair| pair.1 > 1).collect_vec();
    assert!(duplicates.is_empty(), "Duplicate messages found! {:?}", duplicates);
    let msgs = msgs.into_iter().map(|m| (m.source_id(), wrap_message(m))).collect();
    DaoEntities { dao_holder, ds, root, users, cwd, msgs }
}

fn get_simple_dao_entities(dao: &impl ChatHistoryDao)
                           -> (Dataset, DatasetRoot, Vec<User>, ChatWithDetails, Vec<Message>) {
    let ds = dao.datasets().unwrap().remove(0);
    let ds_root = dao.dataset_root(ds.uuid());
    let users = dao.users(&ds.uuid()).unwrap();
    let cwd = dao.chats(&ds.uuid()).unwrap().remove(0);
    let msgs = dao.first_messages(&cwd.chat, usize::MAX).unwrap();
    (ds, ds_root, users, cwd, msgs)
}

fn create_simple_dao(
    is_master: bool,
    name_suffix: &str,
    messages: Vec<Message>,
    num_users: usize,
    amend_message: &impl Fn(bool, &DatasetRoot, &mut Message),
) -> InMemoryDaoHolder {
    let member_ids = (1..=num_users).map(|i| i as i64).collect();
    let users = (1..=num_users).map(|i| create_user(&ZERO_PB_UUID, i as i64)).collect_vec();
    let chat = create_group_chat(&ZERO_PB_UUID, 1, "One", member_ids, messages.len());
    let cwms = vec![ChatWithMessages { chat: Some(chat), messages }];
    create_dao(name_suffix, users, cwms, |ds_root, m| amend_message(is_master, ds_root, m))
}

pub fn create_dao(
    name_suffix: &str,
    users: Vec<User> /* Last one would be self. */,
    cwms: Vec<ChatWithMessages>,
    amend_messages: impl Fn(&DatasetRoot, &mut Message),
) -> InMemoryDaoHolder {
    assert!({
                let user_ids = users.iter().map(|u| u.id).collect_vec();
                cwms.iter()
                    .flat_map(|cwm| cwm.messages.iter().map(|m| m.from_id))
                    .all(|from_id| user_ids.contains(&from_id))
            }, "All messages should have valid user IDs!");

    let ds = Dataset {
        uuid: Some(PbUuid { value: Uuid::new_v4().to_string() }),
        alias: format!("Dataset {name_suffix}"),
        source_type: "test source".to_owned(),
    };

    let mut users = users;
    users.iter_mut().for_each(|u| u.ds_uuid = ds.uuid.clone());

    let tmp_dir = TmpDir::new();
    let ds_root = DatasetRoot(tmp_dir.path.clone());

    let mut cwms = cwms;
    for cwm in cwms.iter_mut() {
        cwm.chat.iter_mut().for_each(|c| c.ds_uuid = ds.uuid.clone());
        cwm.messages.iter_mut().for_each(|m| amend_messages(&ds_root, m));
    }
    let myself = users.last().unwrap().clone();
    InMemoryDaoHolder {
        dao: Box::new(InMemoryDao::new("Test Dao".to_owned(), ds, ds_root.0, myself, users, cwms)),
        tmp_dir: tmp_dir,
    }
}

fn create_user(ds_uuid: &PbUuid, id: i64) -> User {
    User {
        ds_uuid: Some(ds_uuid.clone()),
        id,
        first_name_option: Some("User".to_owned()),
        last_name_option: Some(id.to_string()),
        username_option: Some(format!("user{id}")),
        phone_number_option: Some("xxx xx xx".replace("x", &id.to_string())),
    }
}

fn create_group_chat(ds_uuid: &PbUuid, id: i64, name_suffix: &str, member_ids: Vec<i64>, msg_count: usize) -> Chat {
    assert!(member_ids.len() >= 2);
    Chat {
        ds_uuid: Some(ds_uuid.clone()),
        id: id,
        name_option: Some(format!("Chat {}", name_suffix)),
        tpe: ChatType::PrivateGroup as i32,
        img_path_option: None,
        member_ids: member_ids,
        msg_count: msg_count as i32,
    }
}

pub fn create_regular_message(idx: usize, user_id: usize) -> Message {
    let mut rng = rand::thread_rng();
    // Any previous message
    let reply_to_message_id_option =
        if idx > 0 { Some(rng.gen_range(0..idx) as i64) } else { None };

    let typed = message::Typed::Regular(MessageRegular {
        edit_timestamp_option: Some((BASE_DATE.clone() + Duration::minutes(idx as i64) + Duration::seconds(5)).timestamp()),
        is_deleted: false,
        reply_to_message_id_option: reply_to_message_id_option,
        forward_from_name_option: Some(format!("u{user_id}")),
        content_option: Some(Content {
            sealed_value_optional: Some(
                content::SealedValueOptional::Poll(ContentPoll { question: format!("Hey, {idx}!") })
            )
        }),
    });

    let text = vec![RichText::make_plain(format!("Hello there, {idx}!"))];
    let searchable_string = make_searchable_string(&text, &typed);
    Message {
        internal_id: idx as i64 * 100,
        source_id_option: Some(idx as i64),
        timestamp: (BASE_DATE.clone() + Duration::minutes(idx as i64)).timestamp(),
        from_id: user_id as i64,
        text,
        searchable_string,
        typed: Some(typed),
    }
}

pub mod test_android {
    use rusqlite::Connection;

    use super::*;

    pub fn create_databases(name: &str, name_suffix: &str, db_filename: &str) -> Result<(PathBuf, TmpDir)> {
        let folder = resource(&format!("{}_{}", name, name_suffix));
        assert!(folder.exists());

        let databases = folder.join(loader::android::DATABASES);
        if databases.exists() { fs::remove_dir_all(databases.clone())?; }
        let databases = TmpDir::new_at(databases);

        let files: Vec<(String, PathBuf)> =
            folder.read_dir().unwrap()
                .map(|res| res.unwrap().path())
                .filter(|child| path_file_name(child).unwrap().ends_with(".sql"))
                .map(|child| {
                    (path_file_name(&child).unwrap().smart_slice(..-4).to_owned(), child.clone())
                })
                .collect_vec();

        for (table_name, file) in files.into_iter() {
            let target_db_path = databases.path.join(format!("{}.db", table_name));
            log::info!("Creating table {}", table_name);
            let conn = Connection::open(target_db_path)?;
            let sql = fs::read_to_string(&file)?;
            conn.execute_batch(&sql)?;
        }

        Ok((databases.path.join(db_filename), databases))
    }
}

//
// Helper traits/impls
//

pub trait ExtOption<T> {
    fn unwrap_ref(&self) -> &T;
}

impl<T> ExtOption<T> for Option<T> {
    fn unwrap_ref(&self) -> &T { self.as_ref().unwrap() }
}

pub struct InMemoryDaoHolder {
    pub dao: Box<InMemoryDao>,

    // We need to hold tmp_dir here to prevent early destruction.
    #[allow(unused)]
    tmp_dir: TmpDir,
}

impl Message {
    pub fn source_id(&self) -> MessageSourceId { MessageSourceId(self.source_id_option.unwrap()) }
}

#[must_use]
pub struct TmpDir {
    pub path: PathBuf,
}

impl TmpDir {
    pub fn new() -> Self {
        let dir_name = format!("chm-rust_{}", random_alphanumeric(10));
        let path = std::env::temp_dir().canonicalize().unwrap().join(dir_name);
        Self::new_at(path)
    }

    pub fn new_at(full_path: PathBuf) -> Self {
        fs::create_dir(&full_path).expect("Can't create temp directory!");
        TmpDir { path: full_path }
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect(format!("Failed to remove temporary dir '{}'", self.path.to_str().unwrap()).as_str())
    }
}
