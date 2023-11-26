#![allow(unused_imports)]

use std::cmp::{max, min};
use std::collections::HashSet;
use std::env::temp_dir;
use chrono::prelude::*;
use itertools::Itertools;
use lazy_static::lazy_static;
use pretty_assertions::{assert_eq, assert_ne};
use regex::Regex;

use crate::{NoChooser, User};
use crate::dao::ChatHistoryDao;
use crate::entity_utils::*;
use crate::loader::Loader;
use crate::protobuf::history::*;
use crate::protobuf::history::content::SealedValueOptional::*;
use crate::protobuf::history::message::*;
use crate::protobuf::history::message_service::SealedValueOptional::*;

use super::*;

const TELEGRAM_DIR: &str = "telegram_2020-01";

lazy_static! {
    static ref LOADER: Loader<NoChooser> = Loader::new::<MockHttpClient>(&HTTP_CLIENT, NoChooser);
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
        assert_eq!(dst_msgs_count, max(all_dst_msgs.len() as i32, 0) as usize);
        assert_eq!(src_msgs_count, dst_msgs_count);

        let (src_msgs_count, dst_msgs_count) =
            count(&|dao, cwd, all| dao.messages_slice_len(
                &cwd.chat, all[1].internal_id(), all.last().unwrap().internal_id()))?;
        assert_eq!(dst_msgs_count, max(all_dst_msgs.len() as i32 - 1, 0) as usize);
        assert_eq!(src_msgs_count, dst_msgs_count);

        let (src_msgs_count, dst_msgs_count) =
            count(&|dao, cwd, all| dao.messages_slice_len(
                &cwd.chat, all[0].internal_id(), all.smart_slice(..-1).last().unwrap().internal_id()))?;
        assert_eq!(dst_msgs_count, max(all_dst_msgs.len() as i32 - 1, 0) as usize);
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
    let src_ds_root = src_dao.dataset_root(ds_uuid);

    let (mut dst_dao, _dst_dao_tmpdir) = create_sqlite_dao();
    let dst_ds_root = dst_dao.dataset_root(ds_uuid);
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
fn update_user() -> EmptyRes {
    Ok(())
}

/*
  test("update user") {
    val myself = h2dao.myself(dsUuid)

    def personalChatWith(u: User): Option[Chat] =
      h2dao.chats(dsUuid) map (_.chat) find { c =>
        c.tpe == ChatType.Personal &&
        c.memberIds.contains(u.id) &&
        h2dao.firstMessages(c, 99999).exists(_.fromId == myself.id)
      }

    val users = h2dao.users(dsUuid)
    val user1 = users.find(u => u != myself && personalChatWith(u).isDefined).get

    def doUpdate(u: User): Unit = {
      h2dao.updateUser(u)
      val usersA = h2dao.users(dsUuid)
      assert(usersA.find(_.id == user1.id).get === u)

      val chatA = personalChatWith(u) getOrElse fail("Chat not found after updating!")
      assert(chatA.nameOption === u.prettyNameOption)
    }

    doUpdate(
      user1.copy(
        firstNameOption   = Some("fn"),
        lastNameOption    = Some("ln"),
        usernameOption    = Some("un"),
        phoneNumberOption = Some("+123")
      )
    )

    doUpdate(
      user1.copy(
        firstNameOption   = None,
        lastNameOption    = None,
        usernameOption    = None,
        phoneNumberOption = None
      )
    )

    // Renaming self should not affect private chats
    {
      val chat1Before = personalChatWith(user1) getOrElse fail("Chat not found before updating!")
      h2dao.updateUser(
        myself.copy(
          firstNameOption = Some("My New"),
          lastNameOption  = Some("Name"),
        )
      )
      val chat1After = personalChatWith(user1) getOrElse fail("Chat not found after updating!")
      assert(chat1After.nameOption === chat1Before.nameOption)
    }
  }*/

#[test]
fn delete_chat() -> EmptyRes {
    Ok(())
}

/*test("delete chat") {
  val chats = h2dao.chats(dsUuid).map(_.chat)
  val users = h2dao.users(dsUuid)

  {
    // User is not deleted because it participates in another chat
    val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 9777777777L).get
    h2dao.deleteChat(chatToDelete)
    assert(h2dao.chats(dsUuid).size === chats.size - 1)
    assert(h2dao.users(dsUuid).size === users.size)
    assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
  }

  {
    // User is deleted
    val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 4321012345L).get
    h2dao.deleteChat(chatToDelete)
    assert(h2dao.chats(dsUuid).size === chats.size - 2)
    assert(h2dao.users(dsUuid).size === users.size - 1)
    assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
  }
} */

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

#[test]
fn delete_dataset() -> EmptyRes {
    Ok(())
}

/*
test("delete dataset") {
  h2dao.deleteDataset(dsUuid)
  assert(h2dao.datasets.isEmpty)

  // Dataset files has been moved to a backup dir
  assert(!h2dao.datasetRoot(dsUuid).exists())
  assert(new File(h2dao.getBackupPath(), dsUuid.value).exists())
}
*/

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
    let src_dao = LOADER.load(&src_dir).unwrap();
    init_from(src_dao, src_dir, None)
}

fn init_from(src_dao: Box<InMemoryDao>, src_dir: PathBuf, src_dao_tmpdir: Option<TmpDir>) -> TestDaos {
    let (dst_dao, dst_dao_tmpdir) = create_sqlite_dao();
    dst_dao.copy_all_from(src_dao.as_ref()).unwrap();
    let ds_uuid = src_dao.datasets().unwrap()[0].uuid().clone();
    let src_ds_root = src_dao.dataset_root(&ds_uuid);
    let dst_ds_root = dst_dao.dataset_root(&ds_uuid);
    TestDaos { src_dao, src_dir, src_dao_tmpdir, dst_dao, dst_dao_tmpdir, ds_uuid, src_ds_root, dst_ds_root }
}

fn create_sqlite_dao() -> (SqliteDao, TmpDir) {
    let tmp_dir = TmpDir::new();
    log::info!("Using temp dir {} for Sqlite DAO", path_to_str(&tmp_dir.path).unwrap());
    let dao = SqliteDao::create(&tmp_dir.path.join(SqliteDao::FILENAME)).unwrap();
    (dao, tmp_dir)
}

fn read_all_files(p: &Path) -> Vec<PathBuf> {
    let mut res = vec![];
    for entry in p.read_dir().unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            res.push(path);
        } else {
            res.extend(read_all_files(&path).into_iter());
        }
    }
    res
}
