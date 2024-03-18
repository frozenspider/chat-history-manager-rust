#![allow(unused_imports)]

use pretty_assertions::{assert_eq, assert_ne};

use crate::prelude::*;

use super::*;

#[test]
fn sort_messages() -> EmptyRes {
    const NULL: u32 = 0x0000;
    fn make_msg(filetime: u64, offset: u32, next_message_addr: u32) -> DbMessage {
        DbMessage {
            offset,
            header: DbMessageHeader { next_message_addr, filetime, ..Default::default() },
            ..Default::default()
        }
    }

    // Trivial case, one message
    let mut msgs = vec![
        make_msg(100000, 0x00000000, NULL),
    ];
    let expected = msgs.clone();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Trivial case, different messages
    let mut msgs = vec![
        make_msg(100000, 0x00000000, NULL),
        make_msg(200000, 0x10000000, NULL),
        make_msg(400000, 0x20000000, NULL),
        make_msg(300000, 0x30000000, NULL),
        make_msg(000000, 0x40000000, NULL),
    ];
    let expected = msgs.iter().cloned().sorted_by_key(|m| m.header.filetime).collect_vec();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Already sorted
    let mut msgs = vec![
        make_msg(100000, 0x00000000, NULL),
        make_msg(200000, 0x10000000, 0x20000000),
        make_msg(200000, 0x20000000, 0x30000000),
        make_msg(200000, 0x30000000, NULL),
        make_msg(300000, 0x40000000, NULL),
    ];
    let expected = msgs.clone();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Reverse order
    let mut msgs = vec![
        make_msg(300000, 0x40000000, NULL),
        make_msg(200000, 0x30000000, NULL),
        make_msg(200000, 0x20000000, 0x30000000),
        make_msg(200000, 0x10000000, 0x20000000),
        make_msg(100000, 0x00000000, NULL),
    ];
    let expected = msgs.iter().cloned().rev().collect_vec();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Reverse order 2
    let mut msgs = vec![
        make_msg(200000, 0x10000000, NULL),
        make_msg(200000, 0x20000000, 0x10000000),
        make_msg(200000, 0x30000000, 0x20000000),
        make_msg(200000, 0x40000000, 0x30000000),
        make_msg(200000, 0x50000000, 0x40000000),
    ];
    let expected = msgs.iter().cloned().rev().collect_vec();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Random order
    let mut msgs = vec![
        make_msg(200000, 0x20000000, 0x10000000),
        make_msg(200000, 0x40000000, 0x30000000),
        make_msg(200000, 0x30000000, 0x20000000),
        make_msg(200000, 0x10000000, NULL),
        make_msg(200000, 0x50000000, 0x40000000),
    ];
    let expected = vec![
        make_msg(200000, 0x50000000, 0x40000000),
        make_msg(200000, 0x40000000, 0x30000000),
        make_msg(200000, 0x30000000, 0x20000000),
        make_msg(200000, 0x20000000, 0x10000000),
        make_msg(200000, 0x10000000, NULL),
    ];
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // All messages have NULL next message address, order should be left unchanged
    let mut msgs = vec![
        make_msg(200000, 0x20000000, NULL),
        make_msg(200000, 0x40000000, NULL),
        make_msg(200000, 0x30000000, NULL),
        make_msg(200000, 0x10000000, NULL),
        make_msg(200000, 0x50000000, NULL),
    ];
    let expected = msgs.clone();
    super::sort_messages(&mut msgs)?;
    assert_eq!(msgs, expected);

    // Failure: two messages have NULL next addr
    let mut msgs = vec![
        make_msg(200000, 0x20000000, 0x10000000),
        make_msg(200000, 0x40000000, 0x30000000),
        make_msg(200000, 0x30000000, 0x20000000),
        make_msg(200000, 0x10000000, NULL),
        make_msg(200000, 0x50000000, NULL),
    ];
    assert_matches!(super::sort_messages(&mut msgs), Err(_), "Messages: {:#?}", msgs);

    // Failure: two messages have same next addr
    let mut msgs = vec![
        make_msg(200000, 0x20000000, 0x10000000),
        make_msg(200000, 0x40000000, 0x30000000),
        make_msg(200000, 0x30000000, 0x20000000),
        make_msg(200000, 0x10000000, NULL),
        make_msg(200000, 0x50000000, 0x20000000),
    ];
    assert_matches!(super::sort_messages(&mut msgs), Err(_), "Messages: {:#?}", msgs);

    Ok(())
}
