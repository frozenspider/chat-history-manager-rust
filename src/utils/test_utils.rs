use std::path::{Path, PathBuf};

use chrono::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use rand::Rng;
use uuid::Uuid;
use crate::dao::ChatHistoryDao;

use crate::entity_utils::*;
use crate::InMemoryDao;
use crate::protobuf::history::*;

lazy_static! {
    pub static ref BASE_DATE: DateTime<FixedOffset> = dt("2019-01-02 11:15:21", None);

    pub static ref ZERO_UUID: Uuid = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();

    pub static ref ZERO_PB_UUID: PbUuid = PbUuid { value: ZERO_UUID.to_string() };

    pub static ref RESOURCES_DIR: String =
        concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test").replace("//", "/");
}

pub fn resource(relative_path: &str) -> PathBuf {
    Path::new(RESOURCES_DIR.as_str()).join(relative_path)
}

pub fn dt(s: &str, offset: Option<&FixedOffset>) -> DateTime<FixedOffset> {
    let local = Local::now();
    let offset = offset.unwrap_or(local.offset());
    offset.from_local_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()).unwrap()
}

//
// Entity creation helpers
//

type AmendMessageFn = fn(bool, &DatasetRoot, &mut Message);

pub struct DaoEntities<MsgType> {
    pub dao: Box<InMemoryDao>,
    pub ds: Dataset,
    pub root: DatasetRoot,
    pub users: Vec<User>,
    pub cwd: ChatWithDetails,
    pub msgs: Vec<MsgType>,
}

fn create_dao_and_entities<MsgType>(
    is_master: bool,
    name_suffix: &str,
    src_msgs: Vec<Message>,
    num_users: usize,
    amend_message: AmendMessageFn,
    wrap_message: fn(Message) -> MsgType,
) -> DaoEntities<MsgType> {
    let dao = create_simple_dao(is_master, name_suffix, src_msgs, num_users, amend_message);
    let (ds, root, users, cwd, msgs) =
        get_simple_dao_entities(dao.as_ref());
    DaoEntities { dao, ds, root, users, cwd, msgs: msgs.into_iter().map(wrap_message).collect() }
}

fn get_simple_dao_entities(dao: &impl ChatHistoryDao)
                           -> (Dataset, DatasetRoot, Vec<User>, ChatWithDetails, Vec<Message>) {
    let ds = dao.datasets().remove(0);
    let ds_root = dao.dataset_root(ds.uuid());
    let users = dao.users(&ds.uuid());
    let cwd = dao.chats(&ds.uuid()).remove(0);
    let msgs = dao.first_messages(&cwd.chat, usize::MAX);
    (ds, ds_root, users, cwd, msgs)
}

fn create_simple_dao(
    is_master: bool,
    name_suffix: &str,
    messages: Vec<Message>,
    num_users: usize,
    amend_message: AmendMessageFn,
) -> Box<InMemoryDao> {
    let member_ids = (1..num_users).map(|i| i as i64).collect();
    let users = (1..num_users).map(|i| create_user(&ZERO_PB_UUID, i as i64)).collect_vec();
    let chat = create_group_chat(&ZERO_PB_UUID, 1, "One", member_ids, messages.len());
    let cwms = vec![ChatWithMessages { chat: Some(chat), messages }];
    create_dao(name_suffix, users, cwms, |ds_root, m| amend_message(is_master, ds_root, m))
}

pub fn create_dao(
    name_suffix: &str,
    users: Vec<User> /* Last one would be self. */,
    cwms: Vec<ChatWithMessages>,
    amend_messages: impl Fn(&DatasetRoot, &mut Message),
) -> Box<InMemoryDao> {
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

    let ds_root = DatasetRoot(std::env::temp_dir().join("chm-rust"));

    let mut cwms = cwms;
    for cwm in cwms.iter_mut() {
        cwm.chat.iter_mut().for_each(|c| c.ds_uuid = ds.uuid.clone());
        cwm.messages.iter_mut().for_each(|m| amend_messages(&ds_root, m));
    }
    let myself = users.last().unwrap().clone();
    Box::new(InMemoryDao::new("Test Dao".to_owned(), ds, ds_root.0, myself, users, cwms))
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

pub fn create_regular_message(idx: i64, user_id: i64) -> Message {
    let mut rng = rand::thread_rng();
    // Any previous message
    let reply_to_message_id_option =
        if idx > 0 { Some(rng.gen_range(0..idx)) } else { None };

    let typed = message::Typed::Regular(MessageRegular {
        edit_timestamp_option: Some((BASE_DATE.clone() + Duration::minutes(idx) + Duration::seconds(5)).timestamp()),
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
        internal_id: idx * 100,
        source_id_option: Some(idx),
        timestamp: (BASE_DATE.clone() + Duration::minutes(idx)).timestamp(),
        from_id: user_id,
        text,
        searchable_string,
        typed: Some(typed),
    }
}
