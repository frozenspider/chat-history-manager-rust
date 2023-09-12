use std::collections::Bound;
use std::ops::RangeBounds;

use chrono::Duration;
use rand::Rng;

use crate::test_utils::*;

use super::*;

//
// Tests
//

#[test]
fn basics() {
    let dao = create_dao();
    assert_eq!(dao.name(), &dao.name);
    assert_eq!(dao.storage_path(), &dao.ds_root);
    assert_eq!(dao.datasets().iter().collect_vec(), vec![&dao.dataset]);
    let ds_uuid = dao.datasets().remove(0).uuid.unwrap();

    let users = dao.users(&ds_uuid);
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].id, 2);
    assert_eq!(users[1].id, 1);
    assert_eq!(dao.chats(&ds_uuid).len(), 1);

    let cwd = dao.chats(&ds_uuid).remove(0);
    assert_eq!(cwd.last_msg_option.as_ref(), dao.cwms[0].messages.last());
    assert_eq!(cwd.members, users);
}

#[test]
fn messages_first_last_scroll() {
    let dao = create_dao();
    let ds_uuid = dao.datasets().remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid).remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    assert_eq!(dao.first_messages(&chat, 1), split(msgs, ..=0));
    assert_eq!(dao.first_messages(&chat, 2), split(msgs, ..=1));
    assert_eq!(dao.first_messages(&chat, 1000), split(msgs, ..));
    assert_eq!(dao.first_messages(&chat, len), split(msgs, ..));

    assert_eq!(dao.last_messages(&chat, 1), split(msgs, -1..));
    assert_eq!(dao.last_messages(&chat, 2), split(msgs, -2..));
    assert_eq!(dao.last_messages(&chat, 1000), split(msgs, ..));
    assert_eq!(dao.last_messages(&chat, len), split(msgs, ..));

    assert_eq!(dao.scroll_messages(&chat, 0, 0), vec![]);
    assert_eq!(dao.scroll_messages(&chat, 1000, 0), vec![]);
    assert_eq!(dao.scroll_messages(&chat, 1000, 1000), vec![]);

    assert_eq!(dao.scroll_messages(&chat, 0, 1), split(msgs, ..=0));
    assert_eq!(dao.scroll_messages(&chat, len - 1, 1), split(msgs, -1..));
    assert_eq!(dao.scroll_messages(&chat, len - 2, 2), split(msgs, -2..));
    assert_eq!(dao.scroll_messages(&chat, 0, 1000), split(msgs, ..));
    assert_eq!(dao.scroll_messages(&chat, 0, len), split(msgs, ..));
    assert_eq!(dao.scroll_messages(&chat, 1, len - 2), split(msgs, 1..-1));
}

#[test]
fn messages_befoer_after_between() -> Res<()> {
    let dao = create_dao();
    let ds_uuid = dao.datasets().remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid).remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    assert_eq!(dao.messages_after(&chat, &msgs[0], 1)?, split(msgs, ..=0));
    assert_eq!(dao.messages_after(&chat, &msgs[0], 2)?, split(msgs, ..=1));
    assert_eq!(dao.messages_after(&chat, &msgs[1], 1)?, split(msgs, 1..=1));
    assert_eq!(dao.messages_after(&chat, &msgs[0], 1000)?, split(msgs, ..));
    assert_eq!(dao.messages_after(&chat, &msgs[0], len)?, split(msgs, ..));
    assert_eq!(dao.messages_after(&chat, &msgs[1], 1000)?, split(msgs, 1..));
    assert_eq!(dao.messages_after(&chat, &msgs[1], len - 2)?, split(msgs, 1..-1));
    assert_eq!(dao.messages_after(&chat, &msgs[len - 1], 1000)?, split(msgs, -1..));

    assert_eq!(dao.messages_before(&chat, &msgs[len - 1], 1)?, split(msgs, -1..));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 1], 2)?, split(msgs, -2..));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 2], 1)?, split(msgs, -2..-1));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 1], 1000)?, split(msgs, ..));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 1], len)?, split(msgs, ..));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 2], 1000)?, split(msgs, ..-1));
    assert_eq!(dao.messages_before(&chat, &msgs[len - 2], len - 2)?, split(msgs, 1..-1));
    assert_eq!(dao.messages_before(&chat, &msgs[0], 1000)?, split(msgs, ..=0));

    assert_eq!(dao.messages_between(&chat, &msgs[0], &msgs[0])?, split(msgs, ..=0));
    assert_eq!(dao.messages_between(&chat, &msgs[0], &msgs[1])?, split(msgs, ..=1));
    assert_eq!(dao.messages_between(&chat, &msgs[0], &msgs[len - 1])?, split(msgs, ..));
    assert_eq!(dao.messages_between(&chat, &msgs[1], &msgs[len - 2])?, split(msgs, 1..-1));
    assert_eq!(dao.messages_between(&chat, &msgs[len - 1], &msgs[len - 1])?, split(msgs, -1..));
    assert_eq!(dao.messages_between(&chat, &msgs[len - 2], &msgs[len - 1])?, split(msgs, -2..));

    assert_eq!(dao.count_messages_between(&chat, &msgs[0], &msgs[0]), 0);
    assert_eq!(dao.count_messages_between(&chat, &msgs[0], &msgs[1]), 0);
    assert_eq!(dao.count_messages_between(&chat, &msgs[0], &msgs[2]), 1);
    assert_eq!(dao.count_messages_between(&chat, &msgs[0], &msgs[len - 1]), len - 2);
    assert_eq!(dao.count_messages_between(&chat, &msgs[len - 1], &msgs[len - 1]), 0);
    assert_eq!(dao.count_messages_between(&chat, &msgs[len - 2], &msgs[len - 1]), 0);
    assert_eq!(dao.count_messages_between(&chat, &msgs[len - 3], &msgs[len - 1]), 1);

    Ok(())
}

#[test]
fn messages_around() -> Res<()> {
    let dao = create_dao();
    let ds_uuid = dao.datasets().remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid).remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    let none_vec = vec![];
    let none = none_vec.as_slice();

    const START: Timestamp = 0;
    const END: Timestamp = Timestamp::MAX;

    fn assert_split(actual: (Vec<Message>, Vec<Message>), left: &[Message], right: &[Message]) {
        assert_eq!(actual.0, left);
        assert_eq!(actual.1, right);
    }

    assert_split(dao.messages_around_date(&chat, START, 1), none, split(msgs, ..=0));
    assert_split(dao.messages_around_date(&chat, START, 1000), none, split(msgs, ..));

    assert_split(dao.messages_around_date(&chat, END, 1), split(msgs, -1..), none);
    assert_split(dao.messages_around_date(&chat, END, 1000), split(msgs, ..), none);


    assert_split(dao.messages_around_date(&chat, msgs[0].timestamp, 1), none, split(msgs, ..=0));
    assert_split(dao.messages_around_date(&chat, msgs[1].timestamp, 1), split(msgs, ..=0), split(msgs, 1..=1));
    assert_split(dao.messages_around_date(&chat, msgs[2].timestamp, 2), split(msgs, ..=1), split(msgs, 2..=3));
    assert_split(dao.messages_around_date(&chat, msgs[2].timestamp, 4), split(msgs, ..=1), split(msgs, 2..=5));

    assert_split(dao.messages_around_date(&chat, msgs[len - 1].timestamp, 1), split(msgs, -2..=-2), split(msgs, -1..));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp, 1), split(msgs, -3..=-3), split(msgs, -2..=-2));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp, 2), split(msgs, -4..=-3), split(msgs, -2..));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp, 4), split(msgs, -6..=-3), split(msgs, -2..));

    // Timestamp between N-1 and N
    let n = len / 2;
    let mid_ts = (msgs[n - 1].timestamp + msgs[n].timestamp) / 2;
    let n = n as i32;

    assert_split(dao.messages_around_date(&chat, mid_ts, 1),
                 split(msgs, (n - 1)..n), split(msgs, n..=n));

    Ok(())
}

//
// Helpers
//

fn split<T, R: RangeBounds<i32>>(vec: &Vec<T>, range: R) -> &[T] {
    let lower_inc: usize = match range.start_bound() {
        Bound::Included(&idx) if idx < 0 => vec.len() - (-idx as usize),
        Bound::Included(&idx) => idx as usize,
        Bound::Excluded(&idx) if idx < 0 => vec.len() - (-idx as usize) + 1,
        Bound::Excluded(&idx) => (idx + 1) as usize,
        Bound::Unbounded => 0
    };
    let upper_inc: usize = match range.end_bound() {
        Bound::Included(&idx) if idx < 0 => vec.len() - (-idx as usize),
        Bound::Included(&idx) => idx as usize,
        Bound::Excluded(&idx) if idx < 0 => vec.len() - (-idx as usize) - 1,
        Bound::Excluded(&idx) => (idx - 1) as usize,
        Bound::Unbounded => vec.len() - 1
    };
    &vec[lower_inc..=upper_inc]
}

fn create_regular_message(idx: i64, user_id: i64) -> Message {
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

fn create_dao() -> InMemoryDao {
    let ds = Dataset {
        uuid: Some(PbUuid { value: "00000000-0000-0000-0000-000000000000".to_owned() }),
        alias: "Dataset One".to_owned(),
        source_type: "test source".to_owned(),
    };
    let ds_root: PathBuf = std::env::temp_dir().join("chm-rust");
    let users = vec![
        User {
            ds_uuid: ds.uuid.clone(),
            id: 1,
            first_name_option: Some("Wwwwww Www".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        },
        User {
            ds_uuid: ds.uuid.clone(),
            id: 2,
            first_name_option: Some("Aaaaa".to_owned()),
            last_name_option: Some("Aaaaaaaaaaa".to_owned()),
            username_option: Some("myself".to_owned()),
            phone_number_option: Some("+998 91 1234567".to_owned()),
        },
    ];
    let cwms = vec![{
        let messages =
            (0..10).map(|i| create_regular_message(i, (i % 2) + 1)).collect_vec();
        ChatWithMessages {
            chat: Some(Chat {
                ds_uuid: ds.uuid.clone(),
                id: 1,
                name_option: Some("Chat One".to_owned()),
                tpe: ChatType::PrivateGroup.into(),
                img_path_option: None,
                member_ids: users.iter().map(|u| u.id).collect_vec(),
                msg_count: messages.len() as i32,
            }),
            messages,
        }
    }];
    InMemoryDao::new("Test Dao".to_owned(), ds, ds_root, users.last().unwrap().clone(), users, cwms)
}
