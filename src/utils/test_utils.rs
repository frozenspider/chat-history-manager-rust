use std::cell::{RefCell, UnsafeCell};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use uuid::Uuid;

use crate::prelude::*;
use crate::dao::ChatHistoryDao;

lazy_static! {
    pub static ref BASE_DATE: DateTime<FixedOffset> = dt("2019-01-02 11:15:21", None);

    pub static ref ZERO_UUID: Uuid = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();

    pub static ref ZERO_PB_UUID: PbUuid = PbUuid { value: ZERO_UUID.to_string() };

    pub static ref RESOURCES_DIR: String =
        concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test").replace("//", "/");

    pub static ref MESSAGE_REGULAR_NO_CONTENT: message::Typed = message_regular! {
        edit_timestamp_option: None,
        is_deleted: false,
        forward_from_name_option: None,
        reply_to_message_id_option: None,
        content_option: None,
    };

    // TODO: Do we need cleanup?
    pub static ref HTTP_CLIENT: MockHttpClient = MockHttpClient::new();
}

thread_local! {
    static RNG: UnsafeCell<SmallRng> = UnsafeCell::new(SmallRng::from_entropy());
}

#[macro_export]
macro_rules! coerce_enum {
    ($expr:expr, $pat:pat => $extracted_value:expr) => {{
        if let $pat = $expr {
            $extracted_value
        } else {
            panic!("Could not coerce {} to enum variant {}", stringify!($expr), stringify!($pat));
        }
    }};
}

/// Since std::assert_matches::assert_matches is unstable...
#[macro_export]
macro_rules! assert_matches {
    ($expr:expr, $pat:pat) => {{
        let value = $expr;
        assert!(matches!(value, $pat), "Expected value to match {}! Value:\n{:#?}",
                stringify!($pat), value);
    }};
    ($expr:expr, $pat:pat, $($arg:tt)*) => {{
        let value = $expr;
        assert!(matches!(value, $pat), "Expected value to match {}! Value:\n{:#?}\nContext: {}",
                stringify!($pat), value, format_args!($($arg)*));
    }};
}

pub fn rng() -> &'static mut SmallRng {
    let ptr = RNG.with(|rng: &UnsafeCell<SmallRng>| rng.get());
    unsafe { &mut *ptr }
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
    rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn create_named_file(path: &Path, content: &[u8]) {
    let mut file = fs::File::create(&path).unwrap();
    file.write(content).unwrap();
}

pub fn create_random_named_file(path: &Path) {
    create_named_file(path, random_alphanumeric(256).as_bytes())
}

pub fn create_random_file(parent: &Path) -> PathBuf {
    let path = parent.join(&format!("{}.bin", random_alphanumeric(30)));
    create_random_named_file(&path);
    path
}

/// Returns paths to all files referenced by entities of this dataset. Some might not exist.
/// Files order matches the chats and messages order returned by DAO.
pub fn dataset_files(dao: &impl ChatHistoryDao, ds_uuid: &PbUuid) -> Vec<PathBuf> {
    let ds_root = dao.dataset_root(ds_uuid).unwrap();
    let cwds = dao.chats(ds_uuid).unwrap();
    let mut files: Vec<PathBuf> = cwds.iter()
        .map(|cwd| cwd.chat.img_path_option.as_deref())
        .flatten()
        .map(|f| ds_root.to_absolute(f)).collect();
    for cwd in cwds.iter() {
        let msgs = dao.first_messages(&cwd.chat, usize::MAX).unwrap();
        for msg in msgs.iter() {
            let more_files = msg.files(&ds_root);
            files.extend(more_files.into_iter());
        }
    }
    files
}

/// Checks that files were copied from source to destination.
pub fn assert_files(src_files: &[PathBuf], dst_files: &[PathBuf]) {
    assert_eq!(src_files.len(), dst_files.len());
    for (src, dst) in src_files.iter().zip(dst_files.iter()) {
        assert!(src.exists(), "File {} not found! Bug in test?", src.to_str().unwrap());
        assert!(dst.exists(), "File {} wasn't copied from source", dst.to_str().unwrap());
        let src_content = fs::read(src).unwrap();
        let dst_content = fs::read(dst).unwrap();
        let content_eq = src_content == dst_content;
        assert!(content_eq, "Content of {} didn't match its source {}", dst.to_str().unwrap(), src.to_str().unwrap());
    }
}

//
// Entity creation helpers
//

pub type MsgsMap<MsgType> = BTreeMap<MessageSourceId, MsgType>;

pub struct DaoEntities<MsgType> {
    pub dao_holder: InMemoryDaoHolder,
    pub ds: Dataset,
    pub ds_root: DatasetRoot,
    pub users: Vec<User>,
    pub cwd_option: Option<ChatWithDetails>,
    pub msgs: MsgsMap<MsgType>,
}

impl<MsgType> DaoEntities<MsgType> {
    pub fn cwd(&self) -> &ChatWithDetails { self.cwd_option.as_ref().unwrap() }
}

pub struct MergerHelper {
    pub m: DaoEntities<MasterMessage>,
    pub s: DaoEntities<SlaveMessage>,
}

impl MergerHelper {
    pub fn random_user_id(max: usize) -> usize {
        rng().gen_range(1..=max)
    }

    pub fn new_as_is(num_users: usize,
                     msgs1: Vec<Message>,
                     msgs2: Vec<Message>) -> Self {
        Self::new(num_users, msgs1, msgs2, &|_, _, _| {})
    }

    pub fn new(num_users: usize,
               msgs1: Vec<Message>,
               msgs2: Vec<Message>,
               amend_message: &impl Fn(bool, &DatasetRoot, &mut Message)) -> Self {
        let m_dao = create_simple_dao(true, "One", msgs1, num_users, amend_message);
        let s_dao = create_simple_dao(false, "Two", msgs2, num_users, amend_message);
        Self::new_from_daos(m_dao, s_dao)
    }

    pub fn new_from_daos(m_dao: InMemoryDaoHolder, s_dao: InMemoryDaoHolder) -> Self {
        let m = get_simple_dao_entities(m_dao, MasterMessage);
        let s = get_simple_dao_entities(s_dao, SlaveMessage);
        MergerHelper { m, s }
    }
}

pub fn get_simple_dao_entities<MsgType>(
    dao_holder: InMemoryDaoHolder,
    wrap_message: fn(Message) -> MsgType,
) -> DaoEntities<MsgType> {
    let dao = dao_holder.dao.as_ref();
    let ds = dao.datasets().unwrap().remove(0);
    let ds_root = dao.dataset_root(&ds.uuid).unwrap();
    let users = dao.users(&ds.uuid).unwrap();
    let chat = dao.chats(&ds.uuid).unwrap();
    let cwd_option = if chat.is_empty() { None } else { Some(dao.chats(&ds.uuid).unwrap().remove(0)) };
    let msgs = match cwd_option {
        Some(ref cwd) => dao.first_messages(&cwd.chat, usize::MAX).unwrap(),
        None => vec![],
    };
    let duplicates = msgs.iter().map(|m| m.source_id()).counts().into_iter().filter(|pair| pair.1 > 1).collect_vec();
    assert!(duplicates.is_empty(), "Duplicate messages found! {:?}", duplicates);
    let msgs = msgs.into_iter().map(|m| (m.source_id(), wrap_message(m))).collect();
    DaoEntities { dao_holder, ds, ds_root, users, cwd_option, msgs }
}

pub fn create_simple_dao(
    is_master: bool,
    name_suffix: &str,
    messages: Vec<Message>,
    num_users: usize,
    amend_message: &impl Fn(bool, &DatasetRoot, &mut Message),
) -> InMemoryDaoHolder {
    let users = (1..=num_users).map(|i| create_user(&ZERO_PB_UUID, i as i64)).collect_vec();
    let member_ids = users.iter().map(|u| u.id).collect_vec();
    let chat = create_group_chat(&ZERO_PB_UUID, 1, "One", member_ids, messages.len());
    let cwms = vec![ChatWithMessages { chat, messages }];
    create_dao(name_suffix, users, cwms, |ds_root, m| amend_message(is_master, ds_root, m))
}

pub fn create_dao(
    name_suffix: &str,
    users: Vec<User> /* First one would be self. */,
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
        uuid: PbUuid::random(),
        alias: format!("Dataset {name_suffix}"),
    };

    let mut users = users;
    users.iter_mut().for_each(|u| u.ds_uuid = ds.uuid.clone());

    let tmp_dir = TmpDir::new();
    let ds_root = DatasetRoot(tmp_dir.path.clone());

    let mut cwms = cwms;
    for cwm in cwms.iter_mut() {
        cwm.chat.ds_uuid = ds.uuid.clone();
        let img = create_random_file(&ds_root.0);
        cwm.chat.img_path_option = Some(ds_root.to_relative(&img).unwrap());
        for m in cwm.messages.iter_mut() {
            amend_messages(&ds_root, m);
        }
    }
    let myself_id = users.first().unwrap().id();
    InMemoryDaoHolder {
        dao: Box::new(InMemoryDao::new_single(
            format!("Test Dao {name_suffix}"),
            ds,
            ds_root.0,
            myself_id,
            users,
            cwms,
        )),
        tmp_dir: tmp_dir,
    }
}

pub fn create_user(ds_uuid: &PbUuid, id: i64) -> User {
    User {
        ds_uuid: ds_uuid.clone(),
        id,
        first_name_option: Some("User".to_owned()),
        last_name_option: Some(id.to_string()),
        username_option: Some(format!("user{id}")),
        phone_number_option: Some("xxx xx xx".replace("x", &id.to_string())),
    }
}

pub fn create_personal_chat(ds_uuid: &PbUuid, idx: i32, user: &User, member_ids: Vec<i64>, msg_count: usize) -> Chat {
    assert!(member_ids.len() == 2);
    Chat {
        ds_uuid: ds_uuid.clone(),
        id: idx as i64,
        name_option: user.pretty_name_option(),
        source_type: SourceType::Telegram as i32,
        tpe: ChatType::Personal as i32,
        img_path_option: None,
        member_ids,
        msg_count: msg_count as i32,
        main_chat_id: None,
    }
}

pub fn create_group_chat(ds_uuid: &PbUuid, id: i64, name_suffix: &str, member_ids: Vec<i64>, msg_count: usize) -> Chat {
    assert!(member_ids.len() >= 2);
    Chat {
        ds_uuid: ds_uuid.clone(),
        id: id,
        name_option: Some(format!("Chat {}", name_suffix)),
        source_type: SourceType::Telegram as i32,
        tpe: ChatType::PrivateGroup as i32,
        img_path_option: None,
        member_ids: member_ids,
        msg_count: msg_count as i32,
        main_chat_id: None,
    }
}

pub fn create_regular_message(idx: usize, user_id: usize) -> Message {
    let rng = rng();
    // Any previous message
    let reply_to_message_id_option =
        if idx > 0 { Some(rng.gen_range(0..idx) as i64) } else { None };

    let typed = message_regular! {
        edit_timestamp_option: Some((BASE_DATE.clone() + Duration::minutes(idx as i64) + Duration::seconds(5)).timestamp()),
        is_deleted: false,
        reply_to_message_id_option: reply_to_message_id_option,
        forward_from_name_option: Some(format!("u{user_id}")),
        content_option: Some(Content {
            sealed_value_optional: Some(
                content::SealedValueOptional::Poll(ContentPoll { question: format!("Hey, {idx}!") })
            )
        }),
    };

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

    pub fn create_databases(name: &str,
                            name_suffix: &str,
                            target_db_ext_suffix: &str,
                            db_filename: &str) -> (PathBuf, TmpDir) {
        let folder = resource(&format!("{}_{}", name, name_suffix));
        assert!(folder.exists());

        let databases = folder.join(loader::android::DATABASES);
        if databases.exists() { fs::remove_dir_all(databases.clone()).unwrap(); }
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
            let target_db_path =
                databases.path.join(format!("{table_name}{target_db_ext_suffix}"));
            log::info!("Creating table {}", table_name);
            let conn = Connection::open(target_db_path).unwrap();
            let sql = fs::read_to_string(&file).unwrap();
            conn.execute_batch(&sql).unwrap();
        }

        (databases.path.join(db_filename), databases)
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

/// Since InMemoryDao is usually used to store just one dataset, test helpers are in order.
impl InMemoryDao {
    pub fn ds_uuid(&self) -> PbUuid {
        self.dataset().uuid.clone()
    }

    pub fn dataset(&self) -> Dataset {
        self.datasets().unwrap().first().unwrap().clone()
    }

    pub fn myself_single_ds(&self) -> User {
        self.myself(&self.ds_uuid()).unwrap()
    }

    pub fn users_single_ds(&self) -> Vec<User> {
        self.users(&self.ds_uuid()).unwrap()
    }

    pub fn cwms_single_ds(&self) -> Vec<ChatWithMessages> {
        self.cwms[&self.ds_uuid()].clone()
    }
}

pub struct InMemoryDaoHolder {
    pub dao: Box<InMemoryDao>,

    // We need to hold tmp_dir here to prevent early destruction.
    #[allow(unused)]
    pub tmp_dir: TmpDir,
}

pub const fn src_id(id: i64) -> MessageSourceId { MessageSourceId(id) }

impl Message {
    pub fn source_id(&self) -> MessageSourceId { src_id(self.source_id_option.unwrap()) }
}

pub trait MsgVec {
    fn cloned<const N: usize>(&self, src_ids: [MessageSourceId; N]) -> Self;
    fn changed(&self, condition: impl Fn(MessageSourceId) -> bool) -> Self;
}

impl MsgVec for Vec<Message> {
    fn cloned<const N: usize>(&self, src_ids: [MessageSourceId; N]) -> Self {
        self.iter().filter(|u| src_ids.contains(&u.source_id())).cloned().collect_vec()
    }

    fn changed(&self, condition: impl Fn(MessageSourceId) -> bool) -> Self {
        self.iter().cloned().map(|m| match m {
            Message { typed: Some(ref typed @ message::Typed::Regular(_)), .. } if condition(m.source_id()) => {
                let text = vec![RichText::make_plain(format!("Different message {}", *m.source_id()))];
                let searchable_string = make_searchable_string(&text, typed);
                Message { text, searchable_string, ..m }
            }
            m => m
        }).collect_vec()
    }
}


impl<'a, T> PracticalEq for PracticalEqTuple<'a, Vec<T>> where for<'b> PracticalEqTuple<'a, T>: PracticalEq {
    fn practically_equals(&self, other: &Self) -> Result<bool> {
        if self.v.len() != other.v.len() {
            return Ok(false);
        }
        for (v1, v2) in self.v.iter().zip(other.v.iter()) {
            if !self.with(v1).practically_equals(&other.with(v2))? {
                return Ok(false);
            }
        }
        Ok(true)
    }
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

pub struct MockHttpClient {
    pub calls: Arc<Mutex<RefCell<Vec<String>>>>,
}

impl MockHttpClient {
    pub fn new() -> Self {
        MockHttpClient { calls: Arc::new(Mutex::new(RefCell::new(vec![]))) }
    }

    pub fn calls_copy(&self) -> Vec<String> {
        let lock = self.calls.lock().unwrap();
        let cell = &*lock;
        let vec: &Vec<String> = &(*cell.borrow());
        vec.clone()
    }
}

impl HttpClient for MockHttpClient {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        log::info!("Mocking request to {}", url);
        let lock = self.calls.lock().unwrap();
        let cell = &*lock;
        cell.borrow_mut().push(url.to_owned());
        Ok(Vec::from(url.as_bytes()))
    }
}
