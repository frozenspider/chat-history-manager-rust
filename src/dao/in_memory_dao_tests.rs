use crate::*;

use super::*;

#[test]
fn basics() -> EmptyRes {
    let dao_holder = create_specific_dao();
    let dao = dao_holder.dao;
    assert_eq!(dao.name(), &dao.name);
    assert_eq!(dao.storage_path(), &dao.ds_root);
    assert_eq!(dao.datasets()?.iter().collect_vec(), vec![&dao.dataset]);
    let ds_uuid = dao.datasets()?.remove(0).uuid.unwrap();

    let users = dao.users(&ds_uuid)?;
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].id, 2);
    assert_eq!(users[1].id, 1);
    assert_eq!(dao.chats(&ds_uuid)?.len(), 1);

    let cwd = dao.chats(&ds_uuid)?.remove(0);
    assert_eq!(cwd.last_msg_option.as_ref(), dao.cwms[0].messages.last());
    assert_eq!(cwd.members, users);
    Ok(())
}

#[test]
fn messages_first_last_scroll() -> EmptyRes {
    let dao_holder = create_specific_dao();
    let dao = dao_holder.dao;
    let ds_uuid = dao.datasets()?.remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid)?.remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    assert_eq!(dao.first_messages(&chat, 1)?, msgs.smart_slice(..=0));
    assert_eq!(dao.first_messages(&chat, 2)?, msgs.smart_slice(..=1));
    assert_eq!(dao.first_messages(&chat, 1000)?, msgs.smart_slice(..));
    assert_eq!(dao.first_messages(&chat, len)?, msgs.smart_slice(..));

    assert_eq!(dao.last_messages(&chat, 1)?, msgs.smart_slice(-1..));
    assert_eq!(dao.last_messages(&chat, 2)?, msgs.smart_slice(-2..));
    assert_eq!(dao.last_messages(&chat, 1000)?, msgs.smart_slice(..));
    assert_eq!(dao.last_messages(&chat, len)?, msgs.smart_slice(..));

    assert_eq!(dao.scroll_messages(&chat, 0, 0)?, vec![]);
    assert_eq!(dao.scroll_messages(&chat, 1000, 0)?, vec![]);
    assert_eq!(dao.scroll_messages(&chat, 1000, 1000)?, vec![]);

    assert_eq!(dao.scroll_messages(&chat, 0, 1)?, msgs.smart_slice(..=0));
    assert_eq!(dao.scroll_messages(&chat, len - 1, 1)?, msgs.smart_slice(-1..));
    assert_eq!(dao.scroll_messages(&chat, len - 2, 2)?, msgs.smart_slice(-2..));
    assert_eq!(dao.scroll_messages(&chat, 0, 1000)?, msgs.smart_slice(..));
    assert_eq!(dao.scroll_messages(&chat, 0, len)?, msgs.smart_slice(..));
    assert_eq!(dao.scroll_messages(&chat, 1, len - 2)?, msgs.smart_slice(1..-1));

    Ok(())
}

#[test]
fn messages_befoer_after_slice() -> EmptyRes {
    let dao_holder = create_specific_dao();
    let dao = dao_holder.dao;
    let ds_uuid = dao.datasets()?.remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid)?.remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    assert_eq!(dao.messages_after(&chat, msgs[0].internal_id(), 1)?, msgs.smart_slice(1..=1));
    assert_eq!(dao.messages_after(&chat, msgs[0].internal_id(), 2)?, msgs.smart_slice(1..=2));
    assert_eq!(dao.messages_after(&chat, msgs[1].internal_id(), 1)?, msgs.smart_slice(2..=2));
    assert_eq!(dao.messages_after(&chat, msgs[0].internal_id(), 1000)?, msgs.smart_slice(1..));
    assert_eq!(dao.messages_after(&chat, msgs[0].internal_id(), len - 1)?, msgs.smart_slice(1..));
    assert_eq!(dao.messages_after(&chat, msgs[1].internal_id(), 1000)?, msgs.smart_slice(2..));
    assert_eq!(dao.messages_after(&chat, msgs[1].internal_id(), len - 3)?, msgs.smart_slice(2..-1));
    assert_eq!(dao.messages_after(&chat, msgs[len - 1].internal_id(), 1000)?, vec![]);

    assert_eq!(dao.messages_before(&chat, msgs[len - 1].internal_id(), 1)?, msgs.smart_slice(-2..-1));
    assert_eq!(dao.messages_before(&chat, msgs[len - 1].internal_id(), 2)?, msgs.smart_slice(-3..-1));
    assert_eq!(dao.messages_before(&chat, msgs[len - 2].internal_id(), 1)?, msgs.smart_slice(-3..-2));
    assert_eq!(dao.messages_before(&chat, msgs[len - 1].internal_id(), 1000)?, msgs.smart_slice(..-1));
    assert_eq!(dao.messages_before(&chat, msgs[len - 1].internal_id(), len - 1)?, msgs.smart_slice(..-1));
    assert_eq!(dao.messages_before(&chat, msgs[len - 2].internal_id(), 1000)?, msgs.smart_slice(..-2));
    assert_eq!(dao.messages_before(&chat, msgs[len - 2].internal_id(), len - 3)?, msgs.smart_slice(1..-2));
    assert_eq!(dao.messages_before(&chat, msgs[0].internal_id(), 1000)?, vec![]);

    assert_eq!(dao.messages_slice(&chat, msgs[0].internal_id(), msgs[0].internal_id())?, &msgs[0..=0]);
    assert_eq!(dao.messages_slice(&chat, msgs[0].internal_id(), msgs[1].internal_id())?, &msgs[0..=1]);
    assert_eq!(dao.messages_slice(&chat, msgs[0].internal_id(), msgs[len - 1].internal_id())?, *msgs);
    assert_eq!(dao.messages_slice(&chat, msgs[1].internal_id(), msgs[len - 2].internal_id())?, msgs.smart_slice(1..-1));
    assert_eq!(dao.messages_slice(&chat, msgs[len - 1].internal_id(), msgs[len - 1].internal_id())?, msgs.smart_slice(-1..));
    assert_eq!(dao.messages_slice(&chat, msgs[len - 2].internal_id(), msgs[len - 1].internal_id())?, msgs.smart_slice(-2..));

    assert_eq!(dao.messages_slice_len(&chat, msgs[0].internal_id(), msgs[0].internal_id())?, 1);
    assert_eq!(dao.messages_slice_len(&chat, msgs[0].internal_id(), msgs[1].internal_id())?, 2);
    assert_eq!(dao.messages_slice_len(&chat, msgs[0].internal_id(), msgs[2].internal_id())?, 3);
    assert_eq!(dao.messages_slice_len(&chat, msgs[0].internal_id(), msgs[len - 1].internal_id())?, len);
    assert_eq!(dao.messages_slice_len(&chat, msgs[len - 1].internal_id(), msgs[len - 1].internal_id())?, 1);
    assert_eq!(dao.messages_slice_len(&chat, msgs[len - 2].internal_id(), msgs[len - 1].internal_id())?, 2);
    assert_eq!(dao.messages_slice_len(&chat, msgs[len - 3].internal_id(), msgs[len - 1].internal_id())?, 3);

    Ok(())
}

#[test]
fn messages_around() -> EmptyRes {
    let dao_holder = create_specific_dao();
    let dao = dao_holder.dao;
    let ds_uuid = dao.datasets()?.remove(0).uuid.unwrap();
    let chat = dao.chats(&ds_uuid)?.remove(0).chat;
    let msgs = &dao.cwms[0].messages;
    let len = msgs.len();

    let none_vec = vec![];
    let none = none_vec.as_slice();

    const START: Timestamp = Timestamp(0);
    const END: Timestamp = Timestamp::MAX;

    fn assert_split(actual: (Vec<Message>, Vec<Message>), left: &[Message], right: &[Message]) {
        assert_eq!(actual.0, left);
        assert_eq!(actual.1, right);
    }

    assert_split(dao.messages_around_date(&chat, START, 1)?, none, msgs.smart_slice(..=0));
    assert_split(dao.messages_around_date(&chat, START, 1000)?, none, msgs.smart_slice(..));

    assert_split(dao.messages_around_date(&chat, END, 1)?, msgs.smart_slice(-1..), none);
    assert_split(dao.messages_around_date(&chat, END, 1000)?, msgs.smart_slice(..), none);


    assert_split(dao.messages_around_date(&chat, msgs[0].timestamp(), 1)?, none, msgs.smart_slice(..=0));
    assert_split(dao.messages_around_date(&chat, msgs[1].timestamp(), 1)?, msgs.smart_slice(..=0), msgs.smart_slice(1..=1));
    assert_split(dao.messages_around_date(&chat, msgs[2].timestamp(), 2)?, msgs.smart_slice(..=1), msgs.smart_slice(2..=3));
    assert_split(dao.messages_around_date(&chat, msgs[2].timestamp(), 4)?, msgs.smart_slice(..=1), msgs.smart_slice(2..=5));

    assert_split(dao.messages_around_date(&chat, msgs[len - 1].timestamp(), 1)?, msgs.smart_slice(-2..=-2), msgs.smart_slice(-1..));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp(), 1)?, msgs.smart_slice(-3..=-3), msgs.smart_slice(-2..=-2));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp(), 2)?, msgs.smart_slice(-4..=-3), msgs.smart_slice(-2..));
    assert_split(dao.messages_around_date(&chat, msgs[len - 2].timestamp(), 4)?, msgs.smart_slice(-6..=-3), msgs.smart_slice(-2..));

    // Timestamp between N-1 and N
    let n = len / 2;
    let mid_ts = Timestamp((msgs[n - 1].timestamp + msgs[n].timestamp) / 2);
    let n = n as i32;

    assert_split(dao.messages_around_date(&chat, mid_ts, 1)?,
                 msgs.smart_slice((n - 1)..n), msgs.smart_slice(n..=n));

    Ok(())
}

//
// Helpers
//

pub fn create_specific_dao() -> InMemoryDaoHolder {
    let users = vec![
        User {
            ds_uuid: Some(ZERO_PB_UUID.clone()),
            id: 1,
            first_name_option: Some("Wwwwww Www".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        },
        User {
            ds_uuid: Some(ZERO_PB_UUID.clone()),
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
                ds_uuid: Some(ZERO_PB_UUID.clone()),
                id: 1,
                name_option: Some("Chat One".to_owned()),
                source_type: SourceType::Telegram as i32,
                tpe: ChatType::PrivateGroup as i32,
                img_path_option: None,
                member_ids: users.iter().map(|u| u.id).collect_vec(),
                msg_count: messages.len() as i32,
            }),
            messages,
        }
    }];

    create_dao("One", users, cwms, |_, _| ())
}
