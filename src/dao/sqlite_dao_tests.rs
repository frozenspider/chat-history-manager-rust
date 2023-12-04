#![allow(unused_imports)]

use std::cmp;
use std::fs::File;

use itertools::Itertools;
use pretty_assertions::{assert_eq, assert_ne};
use regex::Regex;

use crate::NoChooser;
use crate::dao::ChatHistoryDao;
use crate::entity_utils::*;
use crate::loader::Loader;
use crate::protobuf::history::*;
use crate::protobuf::history::message::*;

use super::*;

const TELEGRAM_DIR: &str = "telegram_2020-01";

thread_local! {
    static LOADER: Loader = Loader::new::<MockHttpClient>(&HTTP_CLIENT, Box::new(NoChooser), None, None);
}

type Tup<'a, T> = PracticalEqTuple<'a, T>;

#[test]
fn relevant_files_are_copied() -> EmptyRes {
    let daos = init();
    let src_files = dataset_files(daos.src_dao.as_ref(), &daos.ds_uuid);

    // Sanity check: dataset_files() does the right thing.
    {
        let src = fs::read_to_string(daos.src_dir.join("result.json"))?;
        let path_regex = Regex::new(r#""(chats/[a-zA-Z0-9()\[\]./\\_ -]+)""#).unwrap();
        let src_files_2 = path_regex.captures_iter(&src)
            .map(|c| c.get(1).unwrap().as_str())
            .map(|p| daos.src_ds_root.to_absolute(p))
            .sorted().collect_vec();
        assert_eq!(src_files.iter().sorted().collect_vec(), src_files_2.iter().sorted().collect_vec());
    }

    let dst_files = dataset_files(&daos.dst_dao, &daos.ds_uuid);
    assert_files(&src_files, &dst_files);

    let paths_not_to_copy = vec![
        "dont_copy_me.txt",
        "chats/chat_01/dont_copy_me_either.txt",
    ];

    for path in paths_not_to_copy {
        let src_file = daos.src_ds_root.to_absolute(path);
        assert!(src_file.exists(), "File {path} (source) isn't found! Bug in test?");
        assert!(!src_files.contains(&src_file));
        assert!(!dst_files.iter()
            .map(|f| path_file_name(f).unwrap())
            .contains(&path_file_name(&src_file).unwrap()));
    }
    Ok(())
}

/// Messages and chats are equal
#[test]
fn fetching() -> EmptyRes {
    const NUM_MSGS_TO_TAKE: usize = 10;
    let daos = init();

    let src_chats = daos.src_dao.chats(&daos.ds_uuid)?;
    let dst_chats = daos.dst_dao.chats(&daos.ds_uuid)?;
    assert_eq!(src_chats.len(), dst_chats.len());

    for (src_cwd, dst_cwd) in src_chats.iter().zip(dst_chats.iter()) {
        assert_eq!(daos.src_dao.chat_option(&daos.ds_uuid, src_cwd.chat.id)?, Some(src_cwd.clone()));
        assert_eq!(daos.dst_dao.chat_option(&daos.ds_uuid, dst_cwd.chat.id)?, Some(dst_cwd.clone()));

        let practically_eq = |src_msgs: &Vec<Message>, dst_msgs: &Vec<Message>| {
            Tup::new(src_msgs, &daos.src_ds_root, &src_cwd)
                .practically_equals(&Tup::new(dst_msgs, &daos.dst_ds_root, &dst_cwd))
        };

        assert_eq!(src_cwd.chat, dst_cwd.chat);

        let all_src_msgs = daos.src_dao.last_messages(&src_cwd.chat, src_cwd.chat.msg_count as usize)?;
        let all_dst_msgs = daos.dst_dao.last_messages(&dst_cwd.chat, dst_cwd.chat.msg_count as usize)?;
        assert_eq!(all_dst_msgs.len(), dst_cwd.chat.msg_count as usize);
        assert!(practically_eq(&all_src_msgs, &all_dst_msgs)?);

        let fetch = |f: &dyn Fn(&dyn ChatHistoryDao, &ChatWithDetails, &[Message]) -> Result<Vec<Message>>| {
            let src_msgs = f(daos.src_dao.as_ref(), &src_cwd, &all_src_msgs)?;
            let dst_msgs = f(&daos.dst_dao, &dst_cwd, &all_dst_msgs)?;
            Ok::<_, anyhow::Error>((src_msgs, dst_msgs))
        };

        // An unfortunate shortcoming of Rust not supporting generics for closures
        let count = |f: &dyn Fn(&dyn ChatHistoryDao, &ChatWithDetails, &[Message]) -> Result<usize>| {
            let src_msgs = f(daos.src_dao.as_ref(), &src_cwd, &all_src_msgs)?;
            let dst_msgs = f(&daos.dst_dao, &dst_cwd, &all_dst_msgs)?;
            Ok::<_, anyhow::Error>((src_msgs, dst_msgs))
        };

        // first_messages

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, _| dao.first_messages(&cwd.chat, NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(0..(NUM_MSGS_TO_TAKE as i32)));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (_, dst_msgs) =
            fetch(&|dao, cwd, _| dao.first_messages(&cwd.chat, cwd.chat.msg_count as usize))?;
        assert_eq!(dst_msgs, all_dst_msgs);

        // last_messages

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, _| dao.last_messages(&cwd.chat, NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(-(NUM_MSGS_TO_TAKE as i32)..));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (_, dst_msgs) =
            fetch(&|dao, cwd, _| dao.last_messages(&cwd.chat, cwd.chat.msg_count as usize))?;
        assert_eq!(dst_msgs, all_dst_msgs);

        // scroll_messages

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, _| dao.scroll_messages(&cwd.chat, 0, cwd.chat.msg_count as usize))?;
        assert_eq!(dst_msgs, all_dst_msgs);
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, _| dao.scroll_messages(&cwd.chat, 1, cwd.chat.msg_count as usize - 1))?;
        assert_eq!(&dst_msgs, &all_dst_msgs[1..]);
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        // messages_before

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_before(
                &cwd.chat, all.last().unwrap().internal_id(), NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(-(NUM_MSGS_TO_TAKE as i32 + 1)..-1));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_before(
                &cwd.chat, all.smart_slice(..-1).last().unwrap().internal_id(), NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(-(NUM_MSGS_TO_TAKE as i32 + 2)..-2));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        // messages_after

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_after(
                &cwd.chat, all[0].internal_id(), NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(1..(NUM_MSGS_TO_TAKE as i32 + 1)));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_after(
                &cwd.chat, all[1].internal_id(), NUM_MSGS_TO_TAKE))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(2..(NUM_MSGS_TO_TAKE as i32 + 2)));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        // messages_between

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_slice(
                &cwd.chat, all[0].internal_id(), all.last().unwrap().internal_id()))?;
        assert_eq!(&dst_msgs, &all_dst_msgs);
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        let (src_msgs, dst_msgs) =
            fetch(&|dao, cwd, all| dao.messages_slice(
                &cwd.chat, all[1].internal_id(), all.smart_slice(..-1).last().unwrap().internal_id()))?;
        assert_eq!(&dst_msgs, all_dst_msgs.smart_slice(1..-1));
        assert!(practically_eq(&src_msgs, &dst_msgs)?);

        // count_messages_between

        let (src_msgs_count, dst_msgs_count) =
            count(&|dao, cwd, all| dao.messages_slice_len(
                &cwd.chat, all[0].internal_id(), all.last().unwrap().internal_id()))?;
        assert_eq!(dst_msgs_count, cmp::max(all_dst_msgs.len() as i32, 0) as usize);
        assert_eq!(src_msgs_count, dst_msgs_count);

        let (src_msgs_count, dst_msgs_count) =
            count(&|dao, cwd, all| dao.messages_slice_len(
                &cwd.chat, all[1].internal_id(), all.last().unwrap().internal_id()))?;
        assert_eq!(dst_msgs_count, cmp::max(all_dst_msgs.len() as i32 - 1, 0) as usize);
        assert_eq!(src_msgs_count, dst_msgs_count);

        let (src_msgs_count, dst_msgs_count) =
            count(&|dao, cwd, all| dao.messages_slice_len(
                &cwd.chat, all[0].internal_id(), all.smart_slice(..-1).last().unwrap().internal_id()))?;
        assert_eq!(dst_msgs_count, cmp::max(all_dst_msgs.len() as i32 - 1, 0) as usize);
        assert_eq!(src_msgs_count, dst_msgs_count);
    }

    Ok(())
}

#[test]
fn fetching_corner_cases() -> EmptyRes {
    let dao_holder = create_simple_dao(
        false,
        "test",
        (3..=7).map(|idx| create_regular_message(idx, 1)).collect_vec(),
        2,
        &|_, _, _| {});
    let daos = init_from(dao_holder.dao,
                         dao_holder.tmp_dir.path.clone(),
                         Some(dao_holder.tmp_dir));

    let mut dao_vec: Vec<(&dyn ChatHistoryDao, &str)> = vec![];
    dao_vec.push((daos.src_dao.as_ref(), "in-memory"));
    dao_vec.push((&daos.dst_dao, "sqlite"));
    for (dao, clue) in dao_vec {
        for ChatWithDetails { chat, .. } in dao.chats(&daos.ds_uuid)? {
            let msgs = dao.first_messages(&chat, usize::MAX)?;
            let m = |i| msgs.iter().find(|m| m.source_id() == src_id(i)).unwrap();

            assert_eq!(&dao.messages_before(&chat, m(3).internal_id(), 10)?, &[], "{clue}");
            assert_eq!(&dao.messages_before(&chat, m(4).internal_id(), 10)?, &[m(3).clone()], "{clue}");

            assert_eq!(&dao.messages_after(&chat, m(7).internal_id(), 10)?, &[], "{clue}");
            assert_eq!(&dao.messages_after(&chat, m(6).internal_id(), 10)?, &[m(7).clone()], "{clue}");

            assert_eq!(&dao.messages_slice(&chat, m(3).internal_id(), m(3).internal_id())?, &[m(3).clone()], "{clue}");
            assert_eq!(&dao.messages_slice(&chat, m(3).internal_id(), m(4).internal_id())?, &[m(3).clone(), m(4).clone()], "{clue}");
            assert_eq!(&dao.messages_slice(&chat, m(3).internal_id(), m(5).internal_id())?, &[m(3).clone(), m(4).clone(), m(5).clone()], "{clue}");

            assert_eq!(dao.messages_slice_len(&chat, m(3).internal_id(), m(3).internal_id())?, 1, "{clue}");
            assert_eq!(dao.messages_slice_len(&chat, m(3).internal_id(), m(4).internal_id())?, 2, "{clue}");
            assert_eq!(dao.messages_slice_len(&chat, m(3).internal_id(), m(5).internal_id())?, 3, "{clue}");

            assert_eq!(dao.messages_slice_len(&chat, m(7).internal_id(), m(7).internal_id())?, 1, "{clue}");
            assert_eq!(dao.messages_slice_len(&chat, m(6).internal_id(), m(7).internal_id())?, 2, "{clue}");
            assert_eq!(dao.messages_slice_len(&chat, m(5).internal_id(), m(7).internal_id())?, 3, "{clue}");
        }
    }
    Ok(())
}

#[test]
fn inserts() -> EmptyRes {
    let dao_holder = create_simple_dao(
        false,
        "test",
        (1..=10).map(|idx| create_regular_message(idx, 1)).collect_vec(),
        2,
        &|_, _, _| {});
    let src_dao = dao_holder.dao.as_ref();
    let ds_uuid = &src_dao.ds_uuid;
    let src_ds_root = src_dao.dataset_root(ds_uuid)?;

    let (mut dst_dao, _dst_dao_tmpdir) = create_sqlite_dao();
    let dst_ds_root = dst_dao.dataset_root(ds_uuid)?;
    assert_eq!(dst_dao.datasets()?, vec![]);

    // Inserting dataset and users
    dst_dao.insert_dataset(src_dao.in_mem_dataset())?;
    for u in src_dao.in_mem_users() {
        let is_myself = u.id == src_dao.in_mem_myself().id;
        dst_dao.insert_user(u, is_myself)?;
    }
    assert_eq!(dst_dao.datasets()?, src_dao.datasets()?);
    assert_eq!(dst_dao.users(ds_uuid)?, src_dao.users(ds_uuid)?);
    assert_eq!(dst_dao.myself(ds_uuid)?, src_dao.myself(ds_uuid)?);
    assert_eq!(dst_dao.chats(ds_uuid)?, vec![]);

    // Inserting chat
    for c in src_dao.chats(ds_uuid)? {
        dst_dao.insert_chat(c.chat, &src_ds_root)?;
    }
    assert_eq!(dst_dao.chats(ds_uuid)?.len(), src_dao.chats(ds_uuid)?.len());
    for (dst_cwd, src_cwd) in dst_dao.chats(ds_uuid)?.iter().zip(src_dao.chats(ds_uuid)?.iter()) {
        assert_eq!(dst_cwd.members[0], dst_dao.myself(ds_uuid)?);
        assert_eq!(dst_cwd.members, src_cwd.members);
        assert_eq!(dst_cwd.last_msg_option, None);

        let dst_pet = PracticalEqTuple::new(&dst_cwd.chat, &dst_ds_root, &dst_cwd);
        let src_pet = PracticalEqTuple::new(&src_cwd.chat, &src_ds_root, &src_cwd);
        assert!(dst_pet.practically_equals(&src_pet)?);

        // Inserting messages
        assert_eq!(dst_dao.first_messages(&dst_cwd.chat, usize::MAX)?, vec![]);
        assert_eq!(dst_dao.last_messages(&dst_cwd.chat, usize::MAX)?, vec![]);

        let src_msgs = src_dao.first_messages(&src_cwd.chat, usize::MAX)?;
        dst_dao.insert_messages(src_msgs.clone(), &dst_cwd.chat, &src_ds_root)?;

        assert_eq!(dst_dao.first_messages(&dst_cwd.chat, usize::MAX)?.len(), src_msgs.len());
        assert_eq!(dst_dao.last_messages(&dst_cwd.chat, usize::MAX)?.len(), src_msgs.len());

        for (dst_msg, src_msg) in dst_dao.first_messages(&dst_cwd.chat, usize::MAX)?.iter().zip(src_msgs.iter()) {
            let dst_pet = PracticalEqTuple::new(dst_msg, &dst_ds_root, &dst_cwd);
            let src_pet = PracticalEqTuple::new(src_msg, &src_ds_root, &src_cwd);
            assert!(dst_pet.practically_equals(&src_pet)?);
        }
    }

    Ok(())
}

#[test]
fn update_dataset() -> EmptyRes {
    let (mut dao, _tmp_dir) = create_sqlite_dao();

    let ds = dao.insert_dataset(Dataset { uuid: Some(ZERO_PB_UUID.clone()), alias: "My Dataset".to_owned() })?;
    dao.insert_user(create_user(ds.uuid(), 1), true)?;

    let ds = dao.as_mutable()?.update_dataset(Dataset { uuid: ds.uuid.clone(), alias: "Renamed Dataset".to_owned() })?;
    assert_eq!(dao.datasets()?.remove(0), ds);

    Ok(())
}

#[test]
fn delete_dataset() -> EmptyRes {
    let daos = init();
    let mut dao = daos.dst_dao;

    let dst_files = dataset_files(&dao, &daos.ds_uuid);
    for f in dst_files.iter() {
        assert!(f.exists());
    }
    let other_ds = dao.insert_dataset(Dataset { uuid: Some(ZERO_PB_UUID.clone()), alias: "My Dataset".to_owned() })?;
    let other_user = dao.insert_user(create_user(other_ds.uuid(), 1), true)?;
    assert_eq!(dao.datasets()?.len(), 2);

    dao.delete_dataset(daos.ds_uuid.clone())?;

    // Files must be moved to backup dir
    let specific_backup_paths: Vec<_> =
        dao.backup_path().read_dir()?.into_iter().map(|e| e.map(|e| e.path())).try_collect()?;
    assert_eq!(specific_backup_paths.len(), 1);
    let specific_backup_path = &specific_backup_paths[0];
    assert!(path_file_name(specific_backup_path)?.starts_with(BACKUP_NAME_PREFIX));
    assert!(specific_backup_path.is_dir());
    let storage_path_str = path_to_str(dao.storage_path())?;
    for f in dst_files.iter() {
        assert!(!f.exists());
        let moved_f = Path::new(&path_to_str(&f)?
            .replace(storage_path_str, path_to_str(specific_backup_path)?)).to_path_buf();
        assert!(moved_f.exists());
    }

    // Other dataset remain unaffected
    assert_eq!(dao.datasets()?.len(), 1);
    assert_eq!(dao.users(other_ds.uuid())?, vec![other_user]);

    Ok(())
}


#[test]
fn update_user() -> EmptyRes {
    use message_service::SealedValueOptional::*;

    let (mut dao, _tmp_dir) = create_sqlite_dao();

    let ds = dao.insert_dataset(Dataset { uuid: Some(ZERO_PB_UUID.clone()), alias: "My Dataset".to_owned() })?;

    let users: Vec<User> = (1..=3)
        .map(|i| dao.insert_user(create_user(&ZERO_PB_UUID, i as i64), i == 1))
        .try_collect()?;

    fn make_hello_message(internal_id: i64, from_id: i64) -> Message {
        Message::new(
            internal_id,
            Some(internal_id),
            dt("2023-12-03 12:00:00", None).timestamp() + internal_id,
            from_id,
            vec![RichText::make_plain(format!("Hello there from u#{from_id}!"))],
            MESSAGE_REGULAR_NO_CONTENT.clone(),
        )
    }

    let no_ds_tmp_dir = TmpDir::new();
    let no_ds_root = DatasetRoot(no_ds_tmp_dir.path.clone());

    // Group chat, with messages containing members

    let mut group_chat = create_group_chat(&ZERO_PB_UUID, 1, "Group",
                                           vec![1, 2, 3], 123456789);
    let group_chat_msgs = vec![
        Message::new(
            1, Some(1), dt("2023-12-03 12:00:00", None).timestamp(), 1,
            vec![],
            Typed::Service(MessageService {
                sealed_value_optional: Some(GroupCreate(MessageServiceGroupCreate {
                    title: group_chat.name_option.clone().unwrap(),
                    members: users.iter().map(|u| u.pretty_name()).collect_vec(),
                }))
            }),
        ),
        make_hello_message(2, 1),
        make_hello_message(3, 2),
        make_hello_message(4, 3),
    ];
    group_chat.msg_count = group_chat_msgs.len() as i32;
    let group_chat = dao.insert_chat(group_chat, &no_ds_root)?;
    dao.insert_messages(group_chat_msgs.clone(), &group_chat, &no_ds_root)?;

    // Personal chats

    let personal_chat_u2_msgs = vec![
        make_hello_message(1, 2),
    ];
    let personal_chat_u2 = create_personal_chat(&ZERO_PB_UUID, 2, &users[1], vec![1, 2], personal_chat_u2_msgs.len());
    let personal_chat_u2 = dao.insert_chat(personal_chat_u2, &no_ds_root)?;
    dao.insert_messages(personal_chat_u2_msgs.clone(), &personal_chat_u2, &no_ds_root)?;

    let personal_chat_u3 = create_personal_chat(&ZERO_PB_UUID, 3, &users[2], vec![1, 3], 0);
    let personal_chat_u3 = dao.insert_chat(personal_chat_u3, &no_ds_root)?;

    // Updating users

    let mut changed_users = users.clone();

    changed_users[0].first_name_option = Some("MYSELF FN".to_owned());
    changed_users[0].last_name_option = None;
    changed_users[0].phone_number_option = Some("+123".to_owned());
    changed_users[0].username_option = None;

    assert_eq!(dao.update_user(changed_users[0].clone())?, changed_users[0]);

    // Renaming myself should not affect private chat names
    assert_eq!(dao.chat_option(ds.uuid(), personal_chat_u2.id)?.map(|cwd| cwd.chat), Some(personal_chat_u2.clone()));
    assert_eq!(dao.chat_option(ds.uuid(), personal_chat_u3.id)?.map(|cwd| cwd.chat), Some(personal_chat_u3.clone()));
    assert_eq!(dao.chat_option(ds.uuid(), group_chat.id)?.map(|cwd| cwd.chat), Some(group_chat.clone()));

    changed_users[1].first_name_option = Some("U1 FN".to_owned());
    changed_users[1].last_name_option = Some("U1 LN".to_owned());
    changed_users[1].phone_number_option = None;
    changed_users[1].username_option = Some("U1 UN".to_owned());

    changed_users[2].first_name_option = None;
    changed_users[2].last_name_option = None;
    changed_users[2].phone_number_option = None;
    changed_users[2].username_option = None;

    assert_eq!(dao.update_user(changed_users[1].clone())?, changed_users[1]);
    assert_eq!(dao.update_user(changed_users[2].clone())?, changed_users[2]);

    assert_eq!(dao.users(ds.uuid())?, changed_users);
    assert_eq!(dao.myself(ds.uuid())?, changed_users[0]);

    // Personal chat names should be renamed accordingly

    assert_eq!(dao.chat_option(ds.uuid(), personal_chat_u2.id)?.unwrap().chat,
               Chat {
                   name_option: Some("U1 FN U1 LN".to_owned()),
                   ..personal_chat_u2.clone()
               });

    assert_eq!(dao.chat_option(ds.uuid(), personal_chat_u3.id)?.unwrap().chat,
               Chat {
                   name_option: None,
                   ..personal_chat_u3.clone()
               });

    assert_eq!(dao.chat_option(ds.uuid(), group_chat.id)?.unwrap().chat,
               group_chat);

    // String members should also be renamed

    if let Some(Typed::Service(MessageService {
                                   sealed_value_optional: Some(GroupCreate(MessageServiceGroupCreate { members, .. }))
                               })) = dao.first_messages(&group_chat, 1)?.remove(0).typed {
        assert_eq!(members.as_ref(), vec!["MYSELF FN", "U1 FN U1 LN", UNNAMED]);
    }

    Ok(())
}

#[test]
fn delete_chat() -> EmptyRes {
    let daos = init();
    let mut dao = daos.dst_dao;

    let dst_files = dataset_files(&dao, &daos.ds_uuid);
    for f in dst_files.iter() {
        assert!(f.exists());
    }
    assert_eq!(dao.datasets()?.len(), 1);
    let dst_ds = dao.datasets()?.remove(0);
    assert_eq!(dao.users(&daos.ds_uuid)?.len(), 9);

    assert_eq!(dao.chats(dst_ds.uuid())?.len(), 4);
    let cwd = dao.chats(dst_ds.uuid())?.into_iter()
        .find(|cwd| cwd.chat.tpe == ChatType::PrivateGroup as i32).unwrap();
    let files = dao.first_messages(&cwd.chat, usize::MAX)?.iter()
        .flat_map(|m| m.files(&daos.dst_ds_root)).collect_vec();
    assert!(files.len() > 0);

    dao.delete_chat(cwd.chat)?;
    assert_eq!(dao.chats(dst_ds.uuid())?.len(), 3);

    // Files must be moved to backup dir
    let specific_backup_paths: Vec<_> =
        dao.backup_path().read_dir()?.into_iter().map(|e| e.map(|e| e.path())).try_collect()?;
    assert_eq!(specific_backup_paths.len(), 1);
    let specific_backup_path = &specific_backup_paths[0];
    assert!(path_file_name(specific_backup_path)?.starts_with(BACKUP_NAME_PREFIX));
    assert!(specific_backup_path.is_dir());
    let storage_path_str = path_to_str(dao.storage_path())?;
    for f in files.iter() {
        assert!(!f.exists());
        let moved_f = Path::new(&path_to_str(&f)?
            .replace(storage_path_str, path_to_str(specific_backup_path)?)).to_path_buf();
        assert!(moved_f.exists());
    }


    // Other chats must remain unaffected
    for ChatWithDetails { chat, .. } in dao.chats(&daos.ds_uuid)? {
        assert_eq!(chat.tpe, ChatType::Personal as i32);
        assert!(chat.msg_count > 0);
        assert_eq!(chat.msg_count as usize, dao.first_messages(&chat, usize::MAX)?.len());
        for f in dao.first_messages(&chat, usize::MAX)?.iter()
            .flat_map(|m| m.files(&daos.dst_ds_root)) {
            assert!(f.exists());
        }
    }

    // 3 users were participating in other chats, so they remain. Other should be removed.
    let members = dao.chats(&daos.ds_uuid)?.into_iter()
        .flat_map(|cwd| cwd.members)
        .sorted_by_key(|u| u.id)
        .dedup()
        .collect_vec();
    assert_eq!(members.len(), 4);
    assert_eq!(dao.users(&daos.ds_uuid)?.into_iter().sorted_by_key(|u| u.id).collect_vec(), members);

    Ok(())
}

#[ignore]
#[test]
fn absorb_user() -> EmptyRes {
    Ok(())
}

/*
test("merge (absorb) user") {
  def fetchPersonalChat(u: User): Chat = {
    h2dao.chats(dsUuid).map(_.chat).find(c => c.tpe == ChatType.Personal && c.memberIds.contains(u.id)) getOrElse {
      fail(s"Chat for user $u not found!")
    }
  }

  val usersBefore = h2dao.users(dsUuid)
  val chatsBefore = h2dao.chats(dsUuid)

  val baseUser     = usersBefore.find(_.id == 777777777L).get
  val absorbedUser = usersBefore.find(_.id == 32507588L).get

  val baseUserPc     = fetchPersonalChat(baseUser)
  val absorbedUserPc = fetchPersonalChat(absorbedUser)

  val baseUserPcMsgs     = h2dao.firstMessages(baseUserPc, 99999)
  val absorbedUserPcMsgs = h2dao.firstMessages(absorbedUserPc, 99999)

  val newPhoneNumber = "+123 456 789"
  h2dao.mergeUsers(baseUser, absorbedUser.copy(phoneNumberOption = Some(newPhoneNumber)))

  val chatsAfter = h2dao.chats(dsUuid)
  val usersAfter = h2dao.users(dsUuid)

  // Verify users
  assert(usersAfter.size === usersBefore.size - 1)
  val expectedUser = baseUser.copy(phoneNumberOption = Some(baseUser.phoneNumberOption.get + "," + newPhoneNumber))
  assert(usersAfter.find(_.id == baseUser.id) === Some(expectedUser))
  assert(!usersAfter.exists(_.id == absorbedUser.id))

  // Verify chats
  assert(chatsAfter.size === chatsBefore.size - 1)
  val expectedChat = baseUserPc.copy(
    nameOption = expectedUser.firstNameOption,
    msgCount   = baseUserPcMsgs.size + absorbedUserPcMsgs.size
  )
  assert(chatsAfter.find(_.chat.id == baseUserPc.id).map(_.chat) === Some(expectedChat))
  assert(!chatsAfter.exists(_.chat.id == absorbedUserPc.id))

  // Verify messages
  val expectedMessages =
    (baseUserPcMsgs ++ absorbedUserPcMsgs.map { m =>
      m.copy(
        sourceIdOption = None,
        fromId         = baseUser.id,
      )
    }).sortBy(_.timestamp)
  assert(h2dao.firstMessages(chatsAfter.find(_.chat.id == baseUserPc.id).get.chat, 99999) === expectedMessages)
}*/

#[ignore]
#[test]
fn shift_dataset_time() -> EmptyRes {
    Ok(())
}

/*
test("shift dataset time") {
  val chat = h2dao.chats(dsUuid).head.chat
  def getMsg() = {
    h2dao.firstMessages(chat, 1).head
  }
  val msg0 = getMsg()

  {
    // +8
    h2dao.shiftDatasetTime(dsUuid, 8)
    val msg1 = getMsg()
    assert(msg1.internalId == msg0.internalId)
    assert(msg1.time == msg0.time.plusHours(8))
  }

  {
    // +8 -5
    h2dao.shiftDatasetTime(dsUuid, -5)
    val msg1 = getMsg()
    assert(msg1.internalId == msg0.internalId)
    assert(msg1.time == msg0.time.plusHours(3))
  }
}
*/

#[test]
fn backups() -> EmptyRes {
    let dao_holder = create_simple_dao(
        false,
        "test",
        (1..=10).map(|idx| create_regular_message(idx, 1)).collect_vec(),
        2,
        &|_, _, _| {});
    let src_dao = dao_holder.dao.as_ref();
    let ds_uuid = &src_dao.ds_uuid;
    let src_ds_root = src_dao.dataset_root(ds_uuid)?;

    let (mut dst_dao, dst_dao_tmpdir) = create_sqlite_dao();
    assert_eq!(dst_dao.datasets()?, vec![]);

    let backups_dir = dst_dao_tmpdir.path.join(BACKUPS_DIR_NAME);
    assert_eq!(backups_dir.exists(), false);

    let list_backups = || list_all_files(&backups_dir, true).unwrap().into_iter().sorted().collect_vec();

    // First backup
    dst_dao.backup()?.join().unwrap();
    assert_eq!(backups_dir.exists(), true);
    let backups_1 = list_backups();
    assert_eq!(backups_1.len(), 1);

    // Inserting everything from src_dao
    dst_dao.insert_dataset(src_dao.in_mem_dataset())?;
    for u in src_dao.in_mem_users() {
        let is_myself = u.id == src_dao.in_mem_myself().id;
        dst_dao.insert_user(u, is_myself)?;
    }
    for src_cwd in src_dao.chats(ds_uuid)? {
        let src_chat = src_cwd.chat;
        let dst_chat = dst_dao.insert_chat(src_chat.clone(), &src_ds_root)?;
        dst_dao.insert_messages(src_dao.first_messages(&src_chat, usize::MAX)?, &dst_chat, &src_ds_root)?;
    }

    // Second backup
    dst_dao.backup()?.join().unwrap();
    let backups_2 = list_backups();
    assert_eq!(backups_2.len(), 2);
    assert_eq!(backups_2[0], backups_1[0]);
    assert!(backups_2[0].metadata()?.len() < backups_2[1].metadata()?.len());

    // Third backup
    dst_dao.backup()?.join().unwrap();
    let backups_3 = list_backups();
    assert_eq!(backups_3.len(), 3);
    assert_eq!(backups_3[0], backups_2[0]);
    assert_eq!(backups_3[1], backups_2[1]);
    assert!(backups_3[0].metadata()?.len() < backups_3[1].metadata()?.len());
    assert_eq!(backups_3[1].metadata()?.len(), backups_3[2].metadata()?.len());

    // Fourth backup, first one is deleted
    dst_dao.backup()?.join().unwrap();
    let backups_4 = list_backups();
    assert_eq!(backups_4.len(), 3);
    assert_eq!(backups_4[0], backups_3[1]);
    assert_eq!(backups_4[1], backups_3[2]);
    assert!(!backups_4.contains(&backups_3[0]));
    assert_eq!(backups_4[0].metadata()?.len(), backups_4[1].metadata()?.len());
    assert_eq!(backups_4[1].metadata()?.len(), backups_4[2].metadata()?.len());

    // Let's test that backup actually contains the same info
    let last_backup = backups_4.last().unwrap().clone();
    let mut last_backup = File::open(&last_backup)?;
    let mut zip = zip::ZipArchive::new(&mut last_backup)?;
    assert_eq!(zip.len(), 1);

    let mut zip_file = zip.by_index(0)?;
    assert_eq!(zip_file.name(), SqliteDao::FILENAME);

    let unzip_path = backups_dir.join(zip_file.name());
    assert!(!unzip_path.exists());
    std::io::copy(&mut zip_file, &mut File::create(&unzip_path)?)?;
    let dst_dataset_root = dst_dao.dataset_root(ds_uuid)?;
    if dst_dataset_root.0.exists() {
        fs_extra::dir::copy(&dst_dataset_root.0,
                            backups_dir.join(path_file_name(&dst_dataset_root.0)?),
                            &fs_extra::dir::CopyOptions::new().copy_inside(true))?;
    }

    let loaded_dao = SqliteDao::load(&unzip_path)?;

    ensure_datasets_are_equal(&dst_dao, &loaded_dao, ds_uuid, ds_uuid)?;

    Ok(())
}

//
// Helpers
//

struct TestDaos {
    src_dao: Box<InMemoryDao>,
    src_dir: PathBuf,
    dst_dao: SqliteDao,
    // Temp dirs are held to prevent destruction
    #[allow(unused)]
    src_dao_tmpdir: Option<TmpDir>,
    #[allow(unused)]
    dst_dao_tmpdir: TmpDir,
    ds_uuid: PbUuid,
    src_ds_root: DatasetRoot,
    dst_ds_root: DatasetRoot,
}

fn init() -> TestDaos {
    let src_dir = resource(TELEGRAM_DIR);
    let src_dao = LOADER.with(|loader| loader.parse(&src_dir).unwrap());
    init_from(src_dao, src_dir, None)
}

fn init_from(src_dao: Box<InMemoryDao>, src_dir: PathBuf, src_dao_tmpdir: Option<TmpDir>) -> TestDaos {
    let (dst_dao, dst_dao_tmpdir) = create_sqlite_dao();
    dst_dao.copy_all_from(src_dao.as_ref()).unwrap();
    let ds_uuid = src_dao.datasets().unwrap()[0].uuid().clone();
    let src_ds_root = src_dao.dataset_root(&ds_uuid).unwrap();
    let dst_ds_root = dst_dao.dataset_root(&ds_uuid).unwrap();
    TestDaos { src_dao, src_dir, src_dao_tmpdir, dst_dao, dst_dao_tmpdir, ds_uuid, src_ds_root, dst_ds_root }
}

fn create_sqlite_dao() -> (SqliteDao, TmpDir) {
    let tmp_dir = TmpDir::new();
    log::info!("Using temp dir {} for Sqlite DAO", path_to_str(&tmp_dir.path).unwrap());
    let dao = SqliteDao::create(&tmp_dir.path.join(SqliteDao::FILENAME)).unwrap();
    (dao, tmp_dir)
}
