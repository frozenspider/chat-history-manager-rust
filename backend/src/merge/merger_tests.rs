#![allow(unused_imports)]

use std::fs;
use pretty_assertions::{assert_eq, assert_ne};
use uuid::Uuid;

use crate::prelude::*;
use crate::dao::{ChatHistoryDao, UserCacheForDataset, WithCache};

use super::*;

#[test]
fn merge_users() -> EmptyRes {
    let users = (1..=6).map(|id| create_user(&ZERO_PB_UUID, id)).collect_vec();
    let users_a = users.iter().filter(|u| [1_i64, 2, 3, 6].contains(&u.id)).cloned().collect_vec();
    let users_b = change_users(&users[..5], |id| [2_i64, 3, 4].contains(&id));
    let cwm_a = ChatWithMessages {
        chat: create_group_chat(&ZERO_PB_UUID, 1, "A",
                                users_a.iter().map(|u| u.id).collect_vec(), 0),
        messages: vec![],
    };
    let cwm_b = ChatWithMessages {
        chat: create_group_chat(&ZERO_PB_UUID, 1, "B",
                                users_b.iter().map(|u| u.id)
                                    .filter(|id| *id != 5 /* User 5 won't be added */).collect_vec(), 0),
        messages: vec![],
    };

    let helper = MergerHelper::new_from_daos(
        create_dao("One", users_a.clone(), vec![cwm_a], |_, _| {}),
        create_dao("Two", users_b.clone(), vec![cwm_b], |_, _| {}),
    );

    let (new_dao, new_ds, _tmpdir) = merge(
        &helper,
        vec![
            UserMergeDecision::MatchOrDontReplace(UserId(1)),
            UserMergeDecision::MatchOrDontReplace(UserId(2)),
            UserMergeDecision::Replace(UserId(3)),
            UserMergeDecision::Add(UserId(4)),
            UserMergeDecision::DontAdd(UserId(5)),
            UserMergeDecision::Retain(UserId(6)),
        ],
        vec![ChatMergeDecision::Merge {
            chat_id: ChatId(1),
            message_merges: vec![],
        }],
    );

    let by_id = |u: &[User], id: i64| -> User {
        User {
            ds_uuid: new_ds.uuid.clone(),
            ..u.iter().find(|u| u.id == id).unwrap().clone()
        }
    };
    let new_users = new_dao.users(&new_ds.uuid)?;
    assert_eq!(new_users, [
        by_id(&users_a, 1),
        by_id(&users_a, 2),
        by_id(&users_b, 3),
        by_id(&users_b, 4),
        // User 5 discarded
        by_id(&users_a, 6),
    ]);

    Ok(())
}

#[test]
fn merge_users_updating_chat_name() -> EmptyRes {
    let users = (1..=6).map(|id| create_user(&ZERO_PB_UUID, id)).collect_vec();
    let users_a = users.clone();
    let users_b = change_users(&users_a, |_id| true);
    let cwms = vec![
        ChatWithMessages {
            chat: create_group_chat(&ZERO_PB_UUID, 1, "Group", vec![1, 2, 3], 0),
            messages: vec![],
        },
        ChatWithMessages {
            chat: create_personal_chat(&ZERO_PB_UUID, 2, &users_a[1], vec![1, 2], 0),
            messages: vec![],
        },
        ChatWithMessages {
            chat: {
                let mut chat = create_personal_chat(&ZERO_PB_UUID, 3, &users_a[2], vec![1, 3], 0);
                chat.name_option = None;
                chat
            },
            messages: vec![],
        },
    ];
    let helper = MergerHelper::new_from_daos(
        create_dao("One", users_a.clone(), cwms.clone(), |_, _| {}),
        create_dao("Two", users_b.clone(), cwms.clone(), |_, _| {}),
    );

    let (new_dao, new_ds, _tmpdir) = merge(
        &helper,
        users.iter().map(|u| UserMergeDecision::Replace(u.id())).collect_vec(),
        cwms.iter().map(|cwm| ChatMergeDecision::Merge {
            chat_id: cwm.chat.id(),
            message_merges: vec![],
        }).collect_vec(),
    );

    let new_users = new_dao.users(&new_ds.uuid)?;
    assert_eq!(new_users, users_b.clone().into_iter().map(|mut u| {
        u.ds_uuid = new_ds.uuid.clone();
        u
    }).collect_vec());

    let new_chats = new_dao.chats(&new_ds.uuid)?.into_iter().sorted_by_key(|cwd| cwd.chat.id).collect_vec();
    assert_eq!(new_chats.len(), 3);

    assert_eq!(new_chats[0].chat.tpe, ChatType::PrivateGroup as i32);
    assert_eq!(new_chats[0].chat.name_option.as_deref(), Some("Chat Group"));
    assert_eq!(new_chats[1].chat.tpe, ChatType::Personal as i32);
    assert_eq!(new_chats[1].chat.name_option.as_deref(), Some("ChangedUserFN-2 ChangedUserLN-2"));
    assert_eq!(new_chats[2].chat.tpe, ChatType::Personal as i32);
    assert_eq!(new_chats[2].chat.name_option.as_deref(), Some("ChangedUserFN-3 ChangedUserLN-3"));

    Ok(())
}

#[test]
fn merge_multiple_datasets() -> EmptyRes {
    let msgs = vec![create_regular_message(1, 1)];
    let mut helper = MergerHelper::new_as_is(2, msgs.clone(), msgs);

    let other_ds = Dataset {
        uuid: PbUuid { value: Uuid::parse_str("12345678-1234-1234-1234-123456789ABC").unwrap().to_string() },
        alias: "Another dataset".to_owned(),
    };
    let other_ds_users = vec![create_user(&other_ds.uuid, 123), create_user(&other_ds.uuid, 456)];
    let other_tmp_dir = TmpDir::new();
    let other_ds_root = DatasetRoot(other_tmp_dir.path.clone());

    {
        let cache = helper.m.dao_holder.dao.get_cache_mut_unchecked();
        let mut cache = cache.inner.borrow_mut();

        cache.datasets.push(other_ds.clone());
        cache.users.insert(other_ds.uuid.clone(), UserCacheForDataset {
            myself_id: other_ds_users[0].id(),
            user_by_id: other_ds_users.iter().cloned().map(|u| (u.id(), u)).collect(),
        });
    }

    let other_chat = create_personal_chat(&other_ds.uuid, 1, &other_ds_users[0],
                                          other_ds_users.iter().map(|u| u.id).collect_vec(), 3);
    let other_chat_msgs = (1..=other_chat.msg_count)
        .map(|i| create_regular_message(i as usize, other_ds_users[0].id as usize))
        .collect_vec();

    helper.m.dao_holder.dao.ds_roots.insert(other_ds.uuid.clone(), other_ds_root.clone());
    helper.m.dao_holder.dao.cwms.insert(other_ds.uuid.clone(), vec![ChatWithMessages {
        chat: other_chat.clone(),
        messages: other_chat_msgs.clone(),
    }]);

    let (new_dao, new_ds, _tmpdir) = merge(
        &helper,
        dont_replace_both_users(),
        vec![ChatMergeDecision::Merge {
            chat_id: ChatId(1),
            message_merges: vec![
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }],
    );
    assert_eq!(new_dao.datasets()?.iter().sorted_by_key(|ds| &ds.uuid.value).collect_vec(),
               vec![new_ds.clone(), other_ds.clone()].iter().sorted_by_key(|ds| &ds.uuid.value).collect_vec());
    assert_eq!(new_dao.users(&other_ds.uuid)?,
               other_ds_users);
    assert_eq!(new_dao.chats(&other_ds.uuid)?.into_iter().map(|cwd| cwd.chat).collect_vec(),
               vec![other_chat.clone()]);

    let new_other_ds_root = new_dao.dataset_root(&other_ds.uuid)?;
    for (old_cwd, new_cwd) in helper.m.dao_holder.dao.chats(&other_ds.uuid)?.iter()
        .zip(new_dao.chats(&other_ds.uuid)?.iter())
    {
        assert!(PracticalEqTuple::new(&old_cwd.chat, &other_ds_root, old_cwd)
            .practically_equals(&PracticalEqTuple::new(&new_cwd.chat, &new_other_ds_root, new_cwd))?);

        let new_msgs = new_dao.first_messages(&other_chat, usize::MAX)?;
        assert_eq!(new_msgs.len(), other_chat_msgs.len());
        for (old_m, new_m) in other_chat_msgs.iter().zip(new_msgs.iter()) {
            assert_practically_equals(old_m, &other_ds_root, old_cwd,
                                      new_m, &new_other_ds_root, new_cwd);
        }
    }

    Ok(())
}

#[test]
fn merge_chats_match_single_message() -> EmptyRes {
    let msgs_a = vec![create_regular_message(1, 1)];
    let msgs_b = vec![create_regular_message(123, 2)];
    let helper = MergerHelper::new_as_is(2, msgs_a.clone(), msgs_b.clone());

    let (new_dao, new_ds, _tmpdir) = merge(
        &helper,
        dont_replace_both_users(),
        vec![ChatMergeDecision::Merge {
            chat_id: ChatId(1),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: first_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: first_id(&helper.s.msgs),
                })
            ],
        }],
    );

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let (msg_a_regular, msg_b_regular) = match (msgs_a[0].typed(), msgs_b[0].typed()) {
        (message::Typed::Regular(a), message::Typed::Regular(b)) => { (a, b) }
        _ => unreachable!()
    };

    let new_messages = new_dao.first_messages(&new_chats[0].chat, usize::MAX)?;
    assert_eq!(new_messages, vec![Message {
        internal_id: 1,
        source_id_option: msgs_b[0].source_id_option.clone(),
        typed: Some(message_regular! {
            reply_to_message_id_option: msg_b_regular.reply_to_message_id_option,
            ..msg_a_regular.clone()
        }),
        ..msgs_a[0].clone()
    }]);

    Ok(())
}

#[test]
fn merge_chats_keep_single_message() -> EmptyRes {
    let msgs_a = vec![create_regular_message(1, 1)];
    let msgs_b = vec![create_regular_message(2, 2)];
    let helper = MergerHelper::new_as_is(2, msgs_a, msgs_b);

    let (new_dao, new_ds, _tmpdir) = merge(
        &helper,
        dont_replace_both_users(),
        vec![ChatMergeDecision::Merge {
            chat_id: ChatId(1),
            message_merges: vec![
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: first_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: first_id(&helper.s.msgs),
                })
            ],
        }],
    );

    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let new_chat = &new_chats[0].chat;
    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 1);

    assert_practically_equals(&helper.m.msgs[&src_id(1)].0, &helper.m.ds_root, helper.m.cwd(),
                              &new_messages[0], &new_ds_root, &new_chats[0]);

    assert_eq!(new_messages[0], Message { internal_id: 1, ..helper.m.msgs[&src_id(1)].0.clone() });

    Ok(())
}

#[test]
fn merge_chats_keep_single_video() -> EmptyRes {
    use MergeFileHelperTestMode::*;

    merge_files_helper(NoSlaveChat, |helper| vec![
        ChatMergeDecision::Retain { master_chat_id: helper.m.cwd().id() }
    ])?;

    merge_files_helper(NoSlaveMessages, |helper| vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Retain(MergeAnalysisSectionRetention {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                })
            ],
        }
    ])?;

    merge_files_helper(AmendMasterMessagesOnly, |helper| vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ])?;

    merge_files_helper(AmendAllMessages, |helper| vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ])?;

    merge_files_helper(AmendMasterMessagesOnly, |helper| vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ])?;


    merge_files_helper(AmendAllMessages, |helper| vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ])?;

    // With Replace command, content that was previously there will disappear.

    Ok(())
}

enum MergeFileHelperTestMode {
    NoSlaveChat,
    NoSlaveMessages,
    AmendMasterMessagesOnly,
    AmendAllMessages,
}

fn merge_files_helper(mode: MergeFileHelperTestMode,
                      make_chat_merges: impl Fn(&MergerHelper) -> Vec<ChatMergeDecision>) -> EmptyRes {
    use MergeFileHelperTestMode::*;

    let should_amend_all = matches!(mode, AmendAllMessages);
    let msg = create_regular_message(1, 1);

    let mut helper = MergerHelper::new(
        2,
        vec![msg.clone()], if matches!(mode, NoSlaveMessages) { vec![] } else { vec![msg] },
        &|is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
            let content_mode = if is_master || should_amend_all { ContentMode::Full } else { ContentMode::None };
            amend_with_content(content_mode, ds_root, msg)
        },
    );
    if matches!(mode, NoSlaveChat) {
        let dao_holder = create_dao(
            "Two", helper.m.users.clone(), vec![], |_, _| {});
        helper.s = get_simple_dao_entities(dao_holder, SlaveMessage);
    }

    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), make_chat_merges(&helper));
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let new_chat = &new_chats[0].chat;
    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 1);

    let m_files = dataset_files(helper.m.dao_holder.dao.as_ref(), &helper.m.ds.uuid);
    let s_files = dataset_files(helper.s.dao_holder.dao.as_ref(), &helper.s.ds.uuid);
    assert_eq!(m_files.len(), 3);
    assert_eq!(s_files.len(), match mode {
        NoSlaveChat => 0,
        NoSlaveMessages => 1,
        AmendMasterMessagesOnly => 1,
        AmendAllMessages => 3,
    });
    let new_files = dataset_files(&new_dao, &new_ds.uuid);

    let expected_files = if matches!(mode, NoSlaveChat) {
        m_files
    } else {
        // Chat image is taken from the slave chat if present
        [vec![s_files[0].clone()], m_files[1..].to_vec()].into_iter().concat()
    };
    assert_files(&expected_files, &new_files);

    assert_practically_equals(&helper.m.msgs[&src_id(1)].0, &helper.m.ds_root, helper.m.cwd(),
                              &new_messages[0], &new_ds_root, &new_chats[0]);

    Ok(())
}

#[test]
fn merge_chats_replace_single_message() -> EmptyRes {
    let users_a = (1..=2).map(|id| create_user(&ZERO_PB_UUID, id)).collect_vec();
    let users_b = change_users(&users_a, |_| true);
    let msgs_a = vec![create_regular_message(1, 1)];
    let msgs_b = msgs_a.changed(|_| true);

    let helper = {
        let chat = create_personal_chat(&ZERO_PB_UUID, 1, &users_a[1], vec![1, 2], msgs_a.len());
        let cwms = vec![ChatWithMessages { chat, messages: msgs_a }];
        let m_dao = create_dao("One", users_a.clone(), cwms, |_, _| {});

        let chat = create_personal_chat(&ZERO_PB_UUID, 1, &users_b[1], vec![1, 2], msgs_b.len());
        let cwms = vec![ChatWithMessages { chat, messages: msgs_b }];
        let s_dao = create_dao("Two", users_b, cwms, |_, _| {});

        MergerHelper::new_from_daos(m_dao, s_dao)
    };

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                })
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let new_chat = &new_chats[0].chat;
    assert_eq!(new_chat.name_option, users_a[1].pretty_name_option());

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 1);

    assert_practically_equals(&helper.s.msgs[&src_id(1)].0, &helper.s.ds_root, helper.s.cwd(),
                              &new_messages[0], &new_ds_root, &new_chats[0]);

    Ok(())
}

#[test]
fn merge_chats_keep_two_messages() -> EmptyRes {
    let msgs_a = (3..=4).map(|idx| create_regular_message(idx, 1)).collect_vec();
    let msgs_b = msgs_a.changed(|_| true);
    let helper = MergerHelper::new_as_is(2, msgs_a, msgs_b);

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), helper.m.msgs.len());
    assert_eq!(new_chat.msg_count, helper.m.msgs.len() as i32);

    for (MasterMessage(m_msg), new_msg) in helper.m.msgs.values().zip(new_messages.iter()) {
        assert_practically_equals(m_msg, &helper.m.ds_root, helper.m.cwd(),
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

#[test]
fn merge_chats_replace_two_messages() -> EmptyRes {
    let msgs_a = (3..=4).map(|idx| create_regular_message(idx, 1)).collect_vec();
    let msgs_b = msgs_a.changed(|_| true);
    let helper = MergerHelper::new_as_is(2, msgs_a, msgs_b);

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                })
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), helper.s.msgs.len());
    assert_eq!(new_chat.msg_count, helper.s.msgs.len() as i32);

    for (SlaveMessage(s_msg), new_msg) in helper.s.msgs.values().zip(new_messages.iter()) {
        assert_practically_equals(s_msg, &helper.s.ds_root, helper.s.cwd(),
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}


/**
 * ```text
 * Master messages - 1c  2c  3c  4c  5c  6c
 * Slave messages  - 1C  2C  3C* 4C* 5C* 6C*
 * ```
 * `Match(1, 2), Replace(3, 4), DontReplace(5, 6)`
 */
#[test]
fn merge_chats_match_replace_keep() -> EmptyRes {
    let msgs_a = (1..=6).map(|idx| create_regular_message(idx, 1)).collect_vec();
    let msgs_b = vec![
        msgs_a.cloned([1, 2].map(src_id)),
        msgs_a.cloned([3, 4, 5, 6].map(src_id)).changed(|_| true),
    ].into_iter().concat();
    let helper = MergerHelper::new(2, msgs_a, msgs_b,
                                   &|_is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
                                       amend_with_content(ContentMode::Full, ds_root, msg)
                                   });

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                }),
                MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(3)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                }),
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(6)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(6)].typed_id(),
                }),
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 6);
    assert_eq!(new_chat.msg_count, 6);

    let expected = vec![
        PracticalEqTuple::new(&helper.m.msgs[&src_id(1)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.m.msgs[&src_id(2)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.s.msgs[&src_id(3)].0, &helper.s.ds_root, helper.s.cwd()),
        PracticalEqTuple::new(&helper.s.msgs[&src_id(4)].0, &helper.s.ds_root, helper.s.cwd()),
        PracticalEqTuple::new(&helper.m.msgs[&src_id(5)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.m.msgs[&src_id(6)].0, &helper.m.ds_root, helper.m.cwd()),
    ];

    for (old_pet, new_msg) in expected.into_iter().zip(new_messages.iter()) {
        assert_practically_equals(old_pet.v, old_pet.ds_root, old_pet.cwd,
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

/**
 * ```text
 * Master messages - 1c          4c  5c  6c
 * Slave messages  -     2c  3c  4c  5C* 6C*
 * Result messages - 1c  2c      4c  5c  6C*
 * ```
 * `Retain(1), Add(2), DontAdd(3), Match(4), DontReplace(5), Replace(6)`
 */
#[test]
fn merge_chats_merge_all_modes() -> EmptyRes {
    let msgs = (1..=6).map(|idx| create_regular_message(idx as usize, 1)).collect_vec();
    let msgs_a =
        msgs.cloned([1, 4, 5, 6].map(src_id));
    let msgs_b = vec![
        msgs.cloned([2, 3, 4].map(src_id)),
        msgs.cloned([5, 6].map(src_id)).changed(|_| true),
    ].into_iter().concat();
    let helper = MergerHelper::new(
        2, msgs_a, msgs_b,
        &|_is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
            amend_with_content(ContentMode::Full, ds_root, msg)
        },
    );

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Retain(MergeAnalysisSectionRetention {
                    first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                }),
                MessagesMergeDecision::Add(MergeAnalysisSectionAddition {
                    first_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                }),
                MessagesMergeDecision::DontAdd(MergeAnalysisSectionAddition {
                    first_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
                }),
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                }),
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
                }),
                MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(6)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(6)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(6)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(6)].typed_id(),
                }),
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let new_chat = &new_chats[0].chat;
    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 5);
    assert_eq!(new_chat.msg_count, 5);

    let expected = vec![
        PracticalEqTuple::new(&helper.m.msgs[&src_id(1)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.s.msgs[&src_id(2)].0, &helper.s.ds_root, helper.s.cwd()),
        PracticalEqTuple::new(&helper.m.msgs[&src_id(4)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.m.msgs[&src_id(5)].0, &helper.m.ds_root, helper.m.cwd()),
        PracticalEqTuple::new(&helper.s.msgs[&src_id(6)].0, &helper.s.ds_root, helper.s.cwd()),
    ];

    for (old_pet, new_msg) in expected.into_iter().zip(new_messages.iter()) {
        assert_practically_equals(old_pet.v, old_pet.ds_root, old_pet.cwd,
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

/// `Replace(1, n/2-1), DontReplace(n/2, ns)`
///
/// Note: this test is slow due to thousands of files being created, copied and deleted - that's responsible for
/// over 80% of test running time.
#[test]
fn merge_chats_merge_a_lot_of_messages() -> EmptyRes {
    const MAX_MSG_ID: i64 = (BATCH_SIZE as i64) * 3 + 1;

    let msgs_a = (1..=MAX_MSG_ID).map(|idx| create_regular_message(idx as usize, 1)).collect_vec();
    let msgs_b = msgs_a.changed(|_| true);
    let helper = MergerHelper::new(
        2, msgs_a, msgs_b,
        &|_is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
            amend_with_content(ContentMode::Full, ds_root, msg)
        },
    );

    let half = MAX_MSG_ID / 2;

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: helper.m.msgs[&src_id(half - 1)].typed_id(),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: helper.s.msgs[&src_id(half - 1)].typed_id(),
                }),
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(half)].typed_id(),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: helper.s.msgs[&src_id(half)].typed_id(),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                }),
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);

    let new_chat = &new_chats[0].chat;
    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), MAX_MSG_ID as usize);
    assert_eq!(new_chat.msg_count, MAX_MSG_ID as i32);

    let expected = vec![
        (1..half)
            .map(|i| PracticalEqTuple::new(&helper.s.msgs[&src_id(i)].0, &helper.s.ds_root, helper.s.cwd()))
            .collect_vec(),
        (half..=MAX_MSG_ID)
            .map(|i| PracticalEqTuple::new(&helper.m.msgs[&src_id(i)].0, &helper.m.ds_root, helper.m.cwd()))
            .collect_vec(),
    ].into_iter().concat();

    for (old_pet, new_msg) in expected.into_iter().zip(new_messages.iter()) {
        assert_practically_equals(old_pet.v, old_pet.ds_root, old_pet.cwd,
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

#[test]
fn merge_chats_group_messages_with_members_should_adapt_to_renames() -> EmptyRes {
    members_test_helper(
        "Messages are replaced",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::Merge {
                chat_id: helper.m.cwd().id(),
                message_merges: vec![
                    MessagesMergeDecision::Replace(MergeAnalysisSectionConflict {
                        first_master_msg_id: first_id(&helper.m.msgs),
                        last_master_msg_id: last_id(&helper.m.msgs),
                        first_slave_msg_id: first_id(&helper.s.msgs),
                        last_slave_msg_id: last_id(&helper.s.msgs),
                    })
                ],
            }
        ],
    )?;

    members_test_helper(
        "Messages are not replaced",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::Merge {
                chat_id: helper.m.cwd().id(),
                message_merges: vec![
                    MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                        first_master_msg_id: first_id(&helper.m.msgs),
                        last_master_msg_id: last_id(&helper.m.msgs),
                        first_slave_msg_id: first_id(&helper.s.msgs),
                        last_slave_msg_id: last_id(&helper.s.msgs),
                    })
                ],
            }
        ],
    )?;

    members_test_helper(
        "Messages are matching",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::Merge {
                chat_id: helper.m.cwd().id(),
                message_merges: vec![
                    MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                        first_master_msg_id: first_id(&helper.m.msgs),
                        last_master_msg_id: last_id(&helper.m.msgs),
                        first_slave_msg_id: first_id(&helper.s.msgs),
                        last_slave_msg_id: last_id(&helper.s.msgs),
                    })
                ],
            }
        ],
    )?;

    members_test_helper(
        "New messages are added",
        ChatCreationState::CreateNoMessages,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::Merge {
                chat_id: helper.m.cwd().id(),
                message_merges: vec![
                    MessagesMergeDecision::Add(MergeAnalysisSectionAddition {
                        first_slave_msg_id: first_id(&helper.s.msgs),
                        last_slave_msg_id: last_id(&helper.s.msgs),
                    })
                ],
            }
        ],
    )?;

    members_test_helper(
        "Old messages are retained",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::CreateNoMessages,
        |helper| vec![
            ChatMergeDecision::Merge {
                chat_id: helper.m.cwd().id(),
                message_merges: vec![
                    MessagesMergeDecision::Retain(MergeAnalysisSectionRetention {
                        first_master_msg_id: first_id(&helper.m.msgs),
                        last_master_msg_id: last_id(&helper.m.msgs),
                    })
                ],
            }
        ],
    )?;

    members_test_helper(
        "Entire chat is added",
        ChatCreationState::DontCreate,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::Add { slave_chat_id: helper.s.cwd().id() }
        ],
    )?;

    members_test_helper(
        "Entire chat is kept",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::DontCreate,
        |helper| vec![
            ChatMergeDecision::Retain { master_chat_id: helper.m.cwd().id() }
        ],
    )?;

    members_test_helper(
        "Entire chat is kept",
        ChatCreationState::CreateWithMessages,
        ChatCreationState::CreateWithMessages,
        |helper| vec![
            ChatMergeDecision::DontMerge { chat_id: helper.m.cwd().id() }
        ],
    )?;

    Ok(())
}

enum ChatCreationState {
    DontCreate,
    CreateNoMessages,
    CreateWithMessages,
}

/**
 * Creates 4 users, users 3 and 4 are renamed. Creates one message of each type that has members.
 * <p>
 * In all scenarios, outcome should be the same - group messages should be half-baked.
 */

fn members_test_helper(clue: &str,
                       create_master_chat: ChatCreationState,
                       create_slave_chat: ChatCreationState,
                       make_chat_merges: impl Fn(&MergerHelper) -> Vec<ChatMergeDecision>) -> EmptyRes {
    fn make_messages_with_members(users: &[User], group_chat_title: String) -> Vec<Message> {
        use message_service::SealedValueOptional::*;
        let members = users.iter().map(|u| u.pretty_name()).collect_vec();
        let typeds = vec![
            GroupCreate(MessageServiceGroupCreate {
                title: group_chat_title,
                members: members.clone(),
            }),
            GroupInviteMembers(MessageServiceGroupInviteMembers {
                members: members.clone()
            }),
            GroupRemoveMembers(MessageServiceGroupRemoveMembers {
                members: members.clone()
            }),
            PhoneCall(MessageServicePhoneCall {
                duration_sec_option: None,
                discard_reason_option: None,
                members: members.clone()
            }),
        ];
        typeds.into_iter().enumerate().map(|(idx, typed)| {
            Message::new(
                *NO_INTERNAL_ID,
                Some(100 + idx as i64),
                BASE_DATE.timestamp(),
                users[idx].id(),
                vec![RichText::make_plain(format!("Message for a group service message {}", idx + 1))],
                message_service!(typed),
            )
        }).collect_vec()
    }

    let master_users =
        (1..=4).map(|idx| create_user(&ZERO_PB_UUID, idx)).collect_vec();
    let slave_users = master_users.iter()
        .map(|u| User {
            last_name_option: Some(format!("{} (new name)", u.last_name_option.clone().unwrap())),
            ..u.clone()
        })
        .collect_vec();


    let helper = {
        let create_cwms = |state: ChatCreationState, users: &[User]| {
            match state {
                ChatCreationState::DontCreate => vec![],
                ChatCreationState::CreateNoMessages | ChatCreationState::CreateWithMessages => {
                    let mut chat = create_group_chat(
                        &ZERO_PB_UUID, 1, "GC",
                        users.iter().map(|u| u.id).collect_vec(), 9999);
                    let messages = if matches!(state, ChatCreationState::CreateNoMessages) {
                        vec![]
                    } else {
                        make_messages_with_members(users, name_or_unnamed(&chat.name_option))
                    };
                    chat.msg_count = messages.len() as i32;
                    vec![ChatWithMessages { chat, messages }]
                }
            }
        };

        let cwms = create_cwms(create_master_chat, &master_users);
        let m_dao = create_dao("Master", master_users, cwms, |_, _| {});

        let cwms = create_cwms(create_slave_chat, &slave_users);
        let s_dao = create_dao("Slave", slave_users, cwms, |_, _| {});

        MergerHelper::new_from_daos(m_dao, s_dao)
    };

    // Users 1/2 are kept, users 3/4 are replaced.
    let user_merges = vec![
        UserMergeDecision::MatchOrDontReplace(helper.m.users[0].id()),
        UserMergeDecision::MatchOrDontReplace(helper.m.users[1].id()),
        UserMergeDecision::Replace(helper.m.users[2].id()),
        UserMergeDecision::Replace(helper.m.users[3].id()),
    ];

    let expected_members = [&helper.m.users[0], &helper.m.users[1], &helper.s.users[2], &helper.s.users[3]]
        .iter()
        .map(|u| u.pretty_name())
        .collect_vec();
    assert_ne!(expected_members, helper.m.users.iter().map(|u| u.pretty_name()).collect_vec());
    assert_ne!(expected_members, helper.s.users.iter().map(|u| u.pretty_name()).collect_vec());

    let chat_merges = make_chat_merges(&helper);
    let (new_dao, new_ds, _tmpdir) = merge(&helper, user_merges, chat_merges);

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    // New messages will be 4 messages no matter what
    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 4);
    assert_eq!(new_chat.msg_count, 4);

    fn service_value(m: &Message) -> &message_service::SealedValueOptional {
        coerce_enum!(m.typed, Some(message::Typed::Service(ref s)) => s).sealed_value_optional.as_ref().unwrap()
    }

    use message_service::SealedValueOptional::*;
    assert_eq!(coerce_enum!(service_value(&new_messages[0]), GroupCreate(v) => v).members, expected_members, "{clue}");
    assert_eq!(coerce_enum!(service_value(&new_messages[1]), GroupInviteMembers(v) => v).members, expected_members, "{clue}");
    assert_eq!(coerce_enum!(service_value(&new_messages[2]), GroupRemoveMembers(v) => v).members, expected_members, "{clue}");
    assert_eq!(coerce_enum!(service_value(&new_messages[3]), PhoneCall(v) => v).members, expected_members, "{clue}");
    Ok(())
}

#[test]
fn merge_chats_content_preserved_on_match_and_keep() -> EmptyRes {
    let msgs = (1..=4).map(|idx| create_regular_message(idx as usize, 1)).collect_vec();

    // Master messages: have content present
    // Slave messages:  odd messages have no paths, even messages have content missing
    let helper = MergerHelper::new(
        2, msgs.clone(), msgs,
        &|is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
            if is_master {
                amend_with_content(ContentMode::Full, ds_root, msg);
            } else if msg.source_id_option.unwrap() % 2 == 1 {
                amend_with_content(ContentMode::NonePaths, ds_root, msg);
            } else {
                amend_with_content(ContentMode::DeletedPaths, ds_root, msg);
            }
        },
    );

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                }),
                MessagesMergeDecision::DontReplace(MergeAnalysisSectionConflict {
                    first_master_msg_id: helper.m.msgs[&src_id(3)].typed_id(),
                    last_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                    first_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
                    last_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                }),
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 4);
    assert_eq!(new_chat.msg_count, 4);

    for (MasterMessage(m_msg), new_msg) in helper.m.msgs.values().zip(new_messages.iter()) {
        assert_practically_equals(m_msg, &helper.m.ds_root, helper.m.cwd(),
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

#[test]
fn merge_chats_content_appended_on_match() -> EmptyRes {
    let msgs = (1..=4).map(|idx| create_regular_message(idx as usize, 1)).collect_vec();

    // Master messages: odd messages have no paths, even messages have content missing
    // Slave messages: have content present
    let helper = MergerHelper::new(
        2, msgs.clone(), msgs,
        &|is_master: bool, ds_root: &DatasetRoot, msg: &mut Message| {
            if !is_master {
                amend_with_content(ContentMode::Full, ds_root, msg);
            } else if msg.source_id_option.unwrap() % 2 == 1 {
                amend_with_content(ContentMode::NonePaths, ds_root, msg);
            } else {
                amend_with_content(ContentMode::DeletedPaths, ds_root, msg);
            }
        },
    );

    let chat_merges = vec![
        ChatMergeDecision::Merge {
            chat_id: helper.m.cwd().id(),
            message_merges: vec![
                MessagesMergeDecision::Match(MergeAnalysisSectionMatch {
                    first_master_msg_id: first_id(&helper.m.msgs),
                    last_master_msg_id: last_id(&helper.m.msgs),
                    first_slave_msg_id: first_id(&helper.s.msgs),
                    last_slave_msg_id: last_id(&helper.s.msgs),
                }),
            ],
        }
    ];
    let (new_dao, new_ds, _tmpdir) =
        merge(&helper, dont_replace_both_users(), chat_merges);
    let new_ds_root = new_dao.dataset_root(&new_ds.uuid)?;

    let new_chats = new_dao.chats(&new_ds.uuid)?;
    assert_eq!(new_chats.len(), 1);
    let new_chat = &new_chats[0].chat;

    let new_messages = new_dao.first_messages(new_chat, usize::MAX)?;
    assert_eq!(new_messages.len(), 4);
    assert_eq!(new_chat.msg_count, 4);

    for (SlaveMessage(s_msg), new_msg) in helper.s.msgs.values().zip(new_messages.iter()) {
        assert_practically_equals(s_msg, &helper.s.ds_root, helper.s.cwd(),
                                  new_msg, &new_ds_root, &new_chats[0]);
    }

    Ok(())
}

//
// Helpers
//

fn first_id<M, Id>(map: &MsgsMap<M>) -> Id where M: WithTypedId<Item=Id> {
    map.first_key_value().unwrap().1.typed_id()
}

fn last_id<M, Id>(map: &MsgsMap<M>) -> Id where M: WithTypedId<Item=Id> {
    map.last_key_value().unwrap().1.typed_id()
}

fn dont_replace_both_users() -> Vec<UserMergeDecision> {
    vec![UserMergeDecision::MatchOrDontReplace(UserId(1)), UserMergeDecision::MatchOrDontReplace(UserId(2))]
}

fn change_users(users: &[User], id_condition: fn(i64) -> bool) -> Vec<User> {
    users.iter().map(|u| {
        if id_condition(u.id) {
            User {
                first_name_option: Some(format!("ChangedUserFN-{}", u.id)),
                last_name_option: Some(format!("ChangedUserLN-{}", u.id)),
                username_option: Some(format!("ChangedUserUN-{}", u.id)),
                phone_number_option: Some(format!("{}", 123000 + u.id)),
                ..u.clone()
            }
        } else {
            u.clone()
        }
    }).collect_vec()
}

fn merge(helper: &MergerHelper,
         user_merges: Vec<UserMergeDecision>,
         chat_merges: Vec<ChatMergeDecision>) -> (SqliteDao, Dataset, TmpDir) {
    let new_dao_tmpdir = TmpDir::new();
    log::info!("Using temp dir {} for Sqlite DAO", new_dao_tmpdir.path.display());
    let (new_dao, new_ds) = merge_datasets(
        &new_dao_tmpdir.path,
        helper.m.dao_holder.dao.as_ref(),
        &helper.m.ds,
        helper.s.dao_holder.dao.as_ref(),
        &helper.s.ds,
        user_merges,
        chat_merges,
    ).unwrap();
    (new_dao, new_ds, new_dao_tmpdir)
}

fn make_random_video_content(ds_root: &DatasetRoot, none_paths: bool) -> Content {
    make_video_content(ds_root,
                       none_paths,
                       random_alphanumeric(256).as_bytes(),
                       random_alphanumeric(256).as_bytes())
}

fn make_video_content(ds_root: &DatasetRoot, none_paths: bool, f1_content: &[u8], f2_content: &[u8]) -> Content {
    let rand_name = random_alphanumeric(30);
    let path1 = ds_root.0.join(&format!("{rand_name}_1.bin"));
    let path2 = ds_root.0.join(&format!("{rand_name}_2.bin"));
    if !none_paths {
        create_named_file(&path1, f1_content);
        create_named_file(&path2, f2_content);
    }
    Content {
        sealed_value_optional: Some(content::SealedValueOptional::Video(ContentVideo {
            path_option: if none_paths { None } else { Some(ds_root.to_relative(&path1).unwrap()) },
            title_option: Some("My Title".to_owned()),
            performer_option: Some("My Performer".to_owned()),
            width: 111,
            height: 222,
            mime_type: "mt".to_owned(),
            duration_sec_option: Some(10),
            thumbnail_path_option: if none_paths { None } else { Some(ds_root.to_relative(&path2).unwrap()) },
            is_one_time: false,
        }))
    }
}

enum ContentMode {
    None,
    Full,
    DeletedPaths,
    NonePaths,
}

fn assert_practically_equals(src: &Message, src_ds_root: &DatasetRoot, src_cwd: &ChatWithDetails,
                             dst: &Message, dst_ds_root: &DatasetRoot, dst_cwd: &ChatWithDetails) {
    let src_pet = PracticalEqTuple::new(src, src_ds_root, src_cwd);
    let new_pet = PracticalEqTuple::new(dst, dst_ds_root, dst_cwd);
    assert!(new_pet.practically_equals(&src_pet).unwrap(),
            "Message differs:\nWas    {:?}\nBecame {:?}", src, dst);
    assert_files(&src_pet.v.files(src_ds_root), &new_pet.v.files(dst_ds_root));
}

fn amend_with_content(mode: ContentMode, ds_root: &DatasetRoot, msg: &mut Message) {
    let content_field =
        &mut coerce_enum!(msg.typed, Some(message::Typed::Regular(ref mut mr)) => mr).content_option;

    match mode {
        ContentMode::None => {
            *content_field = None;
        }
        ContentMode::Full => {
            *content_field = Some(make_random_video_content(ds_root, false));
        }
        ContentMode::DeletedPaths => {
            *content_field = Some(make_random_video_content(ds_root, false));
            for f in msg.files(ds_root) {
                fs::remove_file(f).unwrap()
            }
        }
        ContentMode::NonePaths => {
            *content_field = Some(make_random_video_content(ds_root, true));
        }
    }
}
