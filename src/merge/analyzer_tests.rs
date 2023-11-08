#![allow(unused_imports)]

use std::fmt::format;
use chrono::Duration;
use chrono::prelude::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use pretty_assertions::{assert_eq, assert_ne};

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::protobuf::history::*;
use crate::protobuf::history::message::Typed;

use super::*;

const MAX_MSG_ID: MessageSourceId = MessageSourceId((BATCH_SIZE as i64) * 3 + 1);

#[test]
fn same_single_message() -> EmptyRes {
    let msgs = vec![create_regular_message(0, 1)];
    let helper = MergerHelper::new_as_is(msgs.clone(), msgs);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            }
        ]
    );
    Ok(())
}

#[test]
fn same_multiple_messages() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs = create_messages(max_id);
    let helper = MergerHelper::new_as_is(msgs.clone(), msgs);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            }
        ]
    );
    Ok(())
}

#[test]
fn no_slave_messages() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs = create_messages(max_id);
    let helper = MergerHelper::new_as_is(msgs, vec![]);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
            }
        ]
    );
    Ok(())
}

#[test]
fn no_new_slave_messages_and_matching_sequence_in_the_middle() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs1 = create_messages(max_id);
    let msgs2 = msgs1.iter().filter(|m| (5..=10).contains(&*m.source_id())).cloned().collect_vec();
    let helper = MergerHelper::new_as_is(msgs1, msgs2);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(10)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(10)].typed_id(),
            },
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(11)].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

#[test]
fn added_one_message_in_the_middle() -> EmptyRes {
    let msgs012 = create_messages(src_id(2));
    let msgs02 = msgs012.iter().filter(|m| *m.source_id() != 1).cloned().collect_vec();
    let helper = MergerHelper::new_as_is(msgs02, msgs012);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
            },
        ]
    );
    Ok(())
}

#[test]
fn changed_one_message_in_the_middle() -> EmptyRes {
    let msgs_a = create_messages(src_id(2));
    let msgs_b = msgs_a.changed(|id| *id == 1);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages -         N
 * Slave messages  - 0 1 ... N
 * ```
 */
#[test]
fn added_multiple_message_in_the_beginning() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_b = create_messages(max_id);
    let msgs_a = msgs_b.last().into_iter().cloned().collect_vec();
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(*max_id - 1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0  1  ...  N
 * Slave messages  - 0* 1* ...* N
 * ```
 */
#[test]
fn changed_multiple_messages_in_the_beginning() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_a = create_messages(max_id);
    let msgs_b = msgs_a.changed(|id| id != max_id);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(*max_id - 1)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(*max_id - 1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0       N
 * Slave messages  - 0 1 ... N
 * ```
 */
#[test]
fn added_multiple_messages_in_the_middle() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_b = create_messages(max_id);
    let msgs_a = msgs_b.cloned([0, *max_id as usize]);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(*max_id - 1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0  1  ...  N
 * Slave messages  - 0  1* ...* N
 * ```
 */
#[test]
fn changed_multiple_messages_in_the_middle() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_a = create_messages(max_id);
    let msgs_b = msgs_a.changed(|id| *id != 0 && id != max_id);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(*max_id - 1)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(*max_id - 1)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0
 * Slave messages  - 0 1 ... N
 * ```
 */
#[test]
fn added_multiple_messages_in_the_end() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_b = create_messages(max_id);
    let msgs_a = msgs_b.cloned([0]);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0  1  ...  N
 * Slave messages  - 0  1* ...* N*
 * ```
 */
#[test]
fn changed_multiple_messages_in_the_end() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_a = create_messages(max_id);
    let msgs_b = msgs_a.changed(|id| *id != 0);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - <none>
 * Slave messages  - 0* 1* ...* N*
 * ```
 */
#[test]
fn added_all_messages() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_a = vec![];
    let msgs_b = create_messages(max_id);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0  1  ...  N
 * Slave messages  - 0* 1* ...* N*
 * ```
 */
#[test]
fn changed_all_messages() -> EmptyRes {
    let max_id = MAX_MSG_ID;
    let msgs_a = create_messages(max_id);
    let msgs_b = msgs_a.changed(|_| true);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&max_id].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&max_id].typed_id(),
            },
        ]
    );
    Ok(())
}


/**
 * ```text
 * Master messages - 0 1 2 3 4
 * Slave messages  -   1   3
 * ```
 */
#[test]
fn missing_messages_in_slave() -> EmptyRes {
    let msgs_a = create_messages(src_id(4));
    let msgs_b = msgs_a.cloned([1, 3]);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
            },
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(3)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(3)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
            },
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages - 0 1     4  5  6 7 8  9
 * Slave messages  -     2 3 4* 5* 6 7 8* 9* 10 11
 * ```
 */
#[test]
fn everything() -> EmptyRes {
    let msgs = create_messages(src_id(11));
    let msgs_a = msgs.cloned([0, 1, 4, 5, 6, 7, 8, 9]);
    let msgs_b = msgs.cloned([2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).changed(|id| [4, 5, 8, 9].contains(&*id));
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(0)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(1)].typed_id(),
            },
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(2)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(3)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(6)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(7)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(6)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(7)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(8)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(9)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(8)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(9)].typed_id(),
            },
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(10)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(11)].typed_id(),
            },
        ]
    );
    Ok(())
}

/**
 * ```text
 * Master messages -     2 3 4* 5* 6 7 8* 9* 10 11
 * Slave messages  - 0 1     4  5  6 7 8  9
 * ```
 */
#[test]
fn everything_inverted() -> EmptyRes {
    let msgs = create_messages(src_id(11));
    let msgs_a = msgs.cloned([2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).changed(|id| [4, 5, 8, 9].contains(&*id));
    let msgs_b = msgs.cloned([0, 1, 4, 5, 6, 7, 8, 9]);
    let helper = MergerHelper::new_as_is(msgs_a, msgs_b);
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Addition {
                first_slave_msg_id: helper.s.msgs[&src_id(0)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(1)].typed_id(),
            },
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(2)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(3)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(4)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(5)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(4)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(5)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(6)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(7)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(6)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(7)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(8)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(9)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(8)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(9)].typed_id(),
            },
            MergeAnalysisSection::Retention {
                first_master_msg_id: helper.m.msgs[&src_id(10)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(11)].typed_id(),
            },
        ]
    );
    Ok(())
}

/// "not found" should NOT conflict with "not downloaded" and vice versa
#[test]
fn present_absent_not_downloaded() -> EmptyRes {
    let user_id = MergerHelper::random_user_id();

    let not_found = ContentPhoto {
        path_option: Some("non/existent/path.jpg".to_owned()),
        width: 100500,
        height: 100600,
        is_one_time: false,
    };

    let not_downloaded = ContentPhoto { path_option: None, ..not_found };

    let placeholder1 = ContentPhoto {
        path_option: Some("placeholder-1".to_owned()),
        width: -1,
        height: -1,
        is_one_time: false,
    };

    let placeholder2 = ContentPhoto {
        path_option: Some("placeholder-2".to_owned()),
        ..not_found
    };

    let placeholder1_content = random_alphanumeric(256);
    let placeholder2_content = random_alphanumeric(256);

    let make_msg_photo = |idx: i64, is_regular: bool, photo: &ContentPhoto| {
        let typed: message::Typed = if is_regular {
            message::Typed::Regular(MessageRegular {
                edit_timestamp_option: Some((BASE_DATE.clone() + Duration::minutes(10 + idx)).timestamp()),
                is_deleted: false,
                reply_to_message_id_option: None,
                forward_from_name_option: Some("some user".to_owned()),
                content_option: Some(Content {
                    sealed_value_optional: Some(content::SealedValueOptional::Photo(photo.clone()))
                }),
            })
        } else {
            message::Typed::Service(MessageService {
                sealed_value_optional: Some(message_service::SealedValueOptional::GroupEditPhoto(
                    MessageServiceGroupEditPhoto { photo: Some(photo.clone()) }
                ))
            })
        };
        let text = vec![RichText::make_plain(format!("Message for a photo {idx}"))];
        Message {
            internal_id: 100 + idx,
            source_id_option: Some(100 + idx),
            timestamp: BASE_DATE.timestamp(),
            from_id: user_id as i64,
            searchable_string: make_searchable_string(&text, &typed),
            text: text,
            typed: Some(typed),
        }
    };

    let msgs_a = vec![
        make_msg_photo(1, /* regular */ true, &not_found),
        make_msg_photo(2, /* regular */ true, &not_downloaded),
        make_msg_photo(3, /* regular */ false, &not_found),
        make_msg_photo(4, /* regular */ false, &not_downloaded),
        //
        make_msg_photo(5, /* regular */ true, &placeholder1),
        make_msg_photo(6, /* regular */ true, &placeholder1),
        make_msg_photo(7, /* regular */ true, &placeholder1),
        make_msg_photo(8, /* regular */ true, &placeholder1),
        make_msg_photo(9, /* regular */ true, &not_downloaded),
        make_msg_photo(10, /* regular */ true, &not_found),
        //
        make_msg_photo(11, /* regular */ false, &placeholder1),
        make_msg_photo(12, /* regular */ false, &placeholder1),
        make_msg_photo(13, /* regular */ false, &placeholder1),
        make_msg_photo(14, /* regular */ false, &placeholder1),
        make_msg_photo(15, /* regular */ false, &not_downloaded),
        make_msg_photo(16, /* regular */ false, &not_found),
    ];
    let msgs_b = vec![
        make_msg_photo(1, /* regular */ true, &not_downloaded),
        make_msg_photo(2, /* regular */ true, &not_found),
        make_msg_photo(3, /* regular */ false, &not_downloaded),
        make_msg_photo(4, /* regular */ false, &not_found),
        //
        make_msg_photo(5, /* regular */ true, &placeholder1),
        make_msg_photo(6, /* regular */ true, &not_downloaded),
        make_msg_photo(7, /* regular */ true, &not_found),
        make_msg_photo(8, /* regular */ true, &placeholder2),
        make_msg_photo(9, /* regular */ true, &placeholder1),
        make_msg_photo(10, /* regular */ true, &placeholder1),
        //
        make_msg_photo(11, /* regular */ false, &placeholder1),
        make_msg_photo(12, /* regular */ false, &not_downloaded),
        make_msg_photo(13, /* regular */ false, &not_found),
        make_msg_photo(14, /* regular */ false, &placeholder2),
        make_msg_photo(15, /* regular */ false, &placeholder1),
        make_msg_photo(16, /* regular */ false, &placeholder1),
    ];

    let helper = MergerHelper::new(msgs_a, msgs_b, &|_is_master, ds_root, msg| {
        let transform = |photo: &mut ContentPhoto| {
            let filename_option: Option<(&str, &[u8])> =
                if photo == &not_found || photo == &not_downloaded {
                    None
                } else if photo == &placeholder1 {
                    Some((photo.path_option.unwrap_ref(), placeholder1_content.as_bytes()))
                } else if photo == &placeholder2 {
                    Some((photo.path_option.unwrap_ref(), placeholder2_content.as_bytes()))
                } else {
                    unreachable!("{:?}", photo)
                };
            if let Some((filename, content)) = filename_option {
                let file_path = ds_root.0.join(filename);
                if !file_path.exists() {
                    create_named_file(&file_path, content);
                }
                photo.path_option = Some(ds_root.to_relative(&file_path).unwrap())
            }
        };
        use message::Typed::*;
        use content::SealedValueOptional::*;
        use message_service::SealedValueOptional::*;
        match msg.typed_mut() {
            Regular(MessageRegular { content_option: Some(Content { sealed_value_optional: Some(Photo(ref mut photo)) }), .. }) => {
                transform(photo)
            }
            Service(MessageService { sealed_value_optional: Some(GroupEditPhoto(ref mut edit_photo)) }) => {
                transform(edit_photo.photo.as_mut().unwrap())
            }
            _ => unreachable!()
        };
    });
    let analysis = analyzer(&helper).analyze(&helper.m.cwd, &helper.s.cwd, "")?;
    assert_eq!(
        analysis, vec![
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(101)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(107)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(101)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(107)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(108)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(108)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(108)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(108)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(109)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(113)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(109)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(113)].typed_id(),
            },
            MergeAnalysisSection::Conflict {
                first_master_msg_id: helper.m.msgs[&src_id(114)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(114)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(114)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(114)].typed_id(),
            },
            MergeAnalysisSection::Match {
                first_master_msg_id: helper.m.msgs[&src_id(115)].typed_id(),
                last_master_msg_id: helper.m.msgs[&src_id(116)].typed_id(),
                first_slave_msg_id: helper.s.msgs[&src_id(115)].typed_id(),
                last_slave_msg_id: helper.s.msgs[&src_id(116)].typed_id(),
            },
        ]
    );
    Ok(())
}

//
// Helpers
//

fn create_messages(max_id: MessageSourceId) -> Vec<Message> {
    (0..=(*max_id as usize)).map(|i| create_regular_message(i, MergerHelper::random_user_id())).collect_vec()
}

fn analyzer(helper: &MergerHelper) -> DatasetDiffAnalyzer {
    DatasetDiffAnalyzer::new(
        helper.m.dao_holder.dao.as_ref(),
        &helper.m.ds,
        helper.s.dao_holder.dao.as_ref(),
        &helper.s.ds,
    )
}

fn src_id(id: i64) -> MessageSourceId { MessageSourceId(id) }

trait MsgVec {
    fn cloned<const N: usize>(&self, indexes: [usize; N]) -> Self;
    fn changed(&self, condition: impl Fn(MessageSourceId) -> bool) -> Self;
}

impl MsgVec for Vec<Message> {
    fn cloned<const N: usize>(&self, indexes: [usize; N]) -> Self {
        indexes.iter().map(|&i| self[i].clone()).collect_vec()
    }

    fn changed(&self, condition: impl Fn(MessageSourceId) -> bool) -> Self {
        self.iter().cloned().map(|m| match m {
            Message {
                typed: Some(ref typed @ message::Typed::Regular(_)), ..
            } if condition(m.source_id()) => {
                let text = vec![RichText::make_plain(format!("Different message {}", *m.source_id()))];
                let searchable_string = make_searchable_string(&text, typed);
                Message { text, searchable_string, ..m }
            }
            m => m
        }).collect_vec()
    }
}
