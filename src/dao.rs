use std::cell::{Ref, RefCell};
use std::path::Path;
use std::thread::JoinHandle;

use deepsize::DeepSizeOf;
use itertools::Itertools;

use crate::prelude::*;

pub mod in_memory_dao;
pub mod sqlite_dao;

pub trait WithCache {
    /// For internal use
    fn get_cache_unchecked(&self) -> &DaoCache;

    /// For internal use
    fn get_cache_mut_unchecked(&mut self) -> &mut DaoCache;

    /// For internal use: lazily initialize the cache, and return a reference to it
    fn init_cache(&self, inner: &mut DaoCacheInner) -> EmptyRes;

    /// For internal use: lazily initialize the cache, and return a reference to immutable inner cache
    fn get_cache(&self) -> Result<Ref<DaoCacheInner>> {
        let cache = self.get_cache_unchecked();
        if !cache.inner.borrow().initialized {
            let mut inner_mut = cache.inner.borrow_mut();
            self.init_cache(&mut inner_mut)?;
            inner_mut.initialized = true;
            drop(inner_mut);
        }
        Ok(cache.inner.borrow())
    }

    /// For internal use, mark cache as invalid
    fn invalidate_cache(&self) -> EmptyRes {
        let cache = self.get_cache_unchecked();
        let mut cache = (*cache.inner).borrow_mut();
        cache.initialized = false;
        Ok(())
    }
}

/**
 * Everything except for messages should be pre-cached and readily available.
 * Should support equality.
 */
pub trait ChatHistoryDao: WithCache + Send {
    /** User-friendly name of a loaded data */
    fn name(&self) -> &str;

    /** Directory which stores eveything - including database itself at the root level */
    fn storage_path(&self) -> &Path;

    fn datasets(&self) -> Result<Vec<Dataset>> {
        Ok(self.get_cache()?.datasets.clone())
    }

    /** Directory which stores eveything in the dataset. All files are guaranteed to have this as a prefix. */
    fn dataset_root(&self, ds_uuid: &PbUuid) -> Result<DatasetRoot>;

    fn myself(&self, ds_uuid: &PbUuid) -> Result<User> {
        let cache = self.get_cache()?;
        let users_cache = &cache.users[ds_uuid];
        Ok(users_cache.user_by_id[&users_cache.myself_id].clone())
    }

    /** Contains myself as the first element, other users are sorted by ID. Method is expected to be fast. */
    fn users(&self, ds_uuid: &PbUuid) -> Result<Vec<User>> {
        let (mut users, myself_id) = self.users_inner(ds_uuid)?;
        users.sort_by_key(|u| if u.id == *myself_id { i64::MIN } else { u.id });
        Ok(users)
    }

    /** Returns all users, as well as myself ID. Method is expected to be fast. */
    fn users_inner(&self, ds_uuid: &PbUuid) -> Result<(Vec<User>, UserId)> {
        let cache = self.get_cache()?;
        let users_cache = &cache.users[ds_uuid];
        let users = users_cache.user_by_id.values().cloned().collect_vec();
        Ok((users, users_cache.myself_id))
    }

    fn user_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<User>> {
        Ok(self.get_cache()?.users[ds_uuid].user_by_id.get(&UserId(id)).cloned())
    }

    /**
     * Returns chats ordered by last message timestamp, descending.
     * Note: This should contain enough info to show chats list in GUI
     */
    fn chats(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        let mut chats = self.chats_inner(ds_uuid)?;
        chats.sort_by_key(|cwd| // Minus used to reverse order
            cwd.last_msg_option.as_ref().map(|m| -m.timestamp).unwrap_or(i64::MAX));
        Ok(chats)
    }

    fn chats_inner(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>>;

    fn chat_option(&self, ds_uuid: &PbUuid, id: i64) -> Result<Option<ChatWithDetails>> {
        // Not an optimal implementation, but often is good enough
        Ok(self.chats(ds_uuid)?.into_iter().find(|c| c.chat.id == id))
    }

    /// Return N messages after skipping first M of them. Trivial pagination in a nutshell.
    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>>;

    fn first_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        self.scroll_messages(chat, 0, limit)
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>>;

    /// Return N messages before the given one (exclusive). Message must be present.
    fn messages_before(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        if limit == 0 { bail!("Limit is zero!"); }
        let result = self.messages_before_impl(chat, msg_id, limit)?;
        assert!(result.len() <= limit);
        Ok(result)
    }

    fn messages_before_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>>;

    /// Return N messages after the given one (exclusive). Message must be present.
    fn messages_after(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        if limit == 0 { bail!("Limit is zero!"); }
        let result = self.messages_after_impl(chat, msg_id, limit)?;
        assert!(result.len() <= limit);
        Ok(result)
    }

    fn messages_after_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>>;

    /// Return N messages between the given ones (inclusive). Messages must be present.
    /// Note: this might need rework in future, as the returned slice is unbounded.
    fn messages_slice(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<Vec<Message>>;

    /// Return N messages between the given ones (inclusive). Messages must be present.
    /// If total number of messages is within `combined_limit`, it will be in the first vector, the rest will be
    /// zero/empty.
    /// Otherwise return `abbreviated_limit` messages from both sides, specifying number of messages in-between.
    fn messages_abbreviated_slice(&self, chat: &Chat,
                                  msg1_id: MessageInternalId,
                                  msg2_id: MessageInternalId,
                                  combined_limit: usize,
                                  abbreviated_limit: usize) -> Result<(Vec<Message>, usize, Vec<Message>)> {
        require!(combined_limit >= 2);
        require!(abbreviated_limit >= 1);
        require!(combined_limit >= 2 * abbreviated_limit);
        self.messages_abbreviated_slice_inner(chat, msg1_id, msg2_id, combined_limit, abbreviated_limit)
    }

    fn messages_abbreviated_slice_inner(&self, chat: &Chat,
                                        msg1_id: MessageInternalId,
                                        msg2_id: MessageInternalId,
                                        combined_limit: usize,
                                        abbreviated_limit: usize) -> Result<(Vec<Message>, usize, Vec<Message>)>;

    /// Count messages between the given ones (inclusive). Messages must be present.
    fn messages_slice_len(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<usize>;

    /** Returns N messages before and N at-or-after the given date */
    fn messages_around_date(&self, chat: &Chat, date_ts: Timestamp, limit: usize) -> Result<(Vec<Message>, Vec<Message>)>;

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>>;

    /** Whether given data path is the one loaded in this DAO */
    fn is_loaded(&self, storage_path: &Path) -> bool {
        self.storage_path() == storage_path
    }

    /// Return self as mutable if applicable, otherwise error out
    fn as_mutable(&mut self) -> Result<&mut dyn MutableChatHistoryDao>;

    /// Return self as shiftable if applicable, otherwise error out
    fn as_shiftable(&mut self) -> Result<&mut dyn ShiftableChatHistoryDao>;
}

pub trait MutableChatHistoryDao: ChatHistoryDao {
    fn backup(&mut self) -> Result<JoinHandle<()>>;

    /// Inserts dataset as-is, with the UUID already set.
    fn insert_dataset(&mut self, ds: Dataset) -> Result<Dataset>;

    fn update_dataset(&mut self, old_uuid: PbUuid, ds: Dataset) -> Result<Dataset>;

    /// Delete a dataset with all the related entities. Deleted dataset root will be moved to backup folder.
    fn delete_dataset(&mut self, uuid: PbUuid) -> EmptyRes;

    fn insert_user(&mut self, user: User, is_myself: bool) -> Result<User>;

    /// Update a user, renaming relevant personal chats and updating messages mentioning that user in plaintext.
    fn update_user(&mut self, old_id: UserId, user: User) -> Result<User>;

    /// Copies image (if any) from dataset root.
    fn insert_chat(&mut self, chat: Chat, src_ds_root: &DatasetRoot) -> Result<Chat>;

    /// Note that chat members won't be changed and image won't be copied/deleted.
    fn update_chat(&mut self, old_id: ChatId, chat: Chat) -> Result<Chat>;

    /// Delete a chat, as well as orphan users. Deleted files will be moved to backup folder.
    fn delete_chat(&mut self, chat: Chat) -> EmptyRes;

    /// Set master chat as a main chat for slave. Both chats have to be main.
    fn combine_chats(&mut self, master_chat: Chat, slave_chat: Chat) -> EmptyRes;

    /// Insert a new message for the given chat.
    /// Internal ID will be ignored.
    /// Content will be resolved based on the given dataset root and copied accordingly.
    fn insert_messages(&mut self, msgs: Vec<Message>, chat: &Chat, src_ds_root: &DatasetRoot) -> EmptyRes;
}

pub trait ShiftableChatHistoryDao: ChatHistoryDao {
    /// Shift time of all timestamps in the dataset to accommodate timezone differences.
    fn shift_dataset_time(&mut self, uuid: &PbUuid, hours_shift: i32) -> EmptyRes;
}

type UserCache = HashMap<PbUuid, UserCacheForDataset>;

#[derive(DeepSizeOf)]
pub struct UserCacheForDataset {
    pub myself_id: UserId,
    pub user_by_id: HashMap<UserId, User>,
}

impl std::hash::Hash for PbUuid {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.value.hash(hasher)
    }
}

impl Eq for PbUuid {}

#[derive(DeepSizeOf)]
pub struct DaoCache {
    pub inner: Box<RefCell<DaoCacheInner>>,
}

#[derive(Default, DeepSizeOf)]
pub struct DaoCacheInner {
    pub initialized: bool,
    pub datasets: Vec<Dataset>,
    pub users: UserCache,
}

impl DaoCache {
    fn new() -> Self {
        DaoCache {
            inner: Box::new(RefCell::new(DaoCacheInner { initialized: false, ..Default::default() }))
        }
    }
}

const BATCH_SIZE: usize = 5_000;

pub fn get_datasets_diff(master_dao: &dyn ChatHistoryDao,
                         master_ds_uuid: &PbUuid,
                         slave_dao: &dyn ChatHistoryDao,
                         slave_ds_uuid: &PbUuid,
                         max_diffs: usize) -> Result<Vec<Difference>> {
    let mut differences = Vec::with_capacity(max_diffs);

    macro_rules! check_diff {
        ($expr:expr, $critical:literal, $msg:expr, $values:expr) => {
            if !$expr {
                let values: Option<(_, _)> = $values;
                differences.push(Difference {
                    message: $msg.to_string(),
                    values: values.map(|vs| DifferenceValues { old: vs.0.to_string(), new: vs.1.to_string()}),
                });
                if $critical || differences.len() >= max_diffs { return Ok(differences.clone()); }
            }
        };
    }

    measure(|| {
        let master_ds = master_dao.datasets()?.into_iter().find(|ds| &ds.uuid == master_ds_uuid)
            .with_context(|| format!("Dataset {} not found in master DAO!", master_ds_uuid.value))?;
        let mut slave_ds = slave_dao.datasets()?.into_iter().find(|ds| &ds.uuid == slave_ds_uuid)
            .with_context(|| format!("Dataset {} not found in slave DAO!", slave_ds_uuid.value))?;
        slave_ds.uuid = master_ds_uuid.clone();
        check_diff!(master_ds == slave_ds, false,
                    "Dataset differs", Some((format!("{master_ds:?}"), format!("{slave_ds:?}"))));
        let master_ds_root = master_dao.dataset_root(master_ds_uuid)?;
        let slave_ds_root = slave_dao.dataset_root(slave_ds_uuid)?;
        check_diff!(*master_ds_root != *slave_ds_root, false,
                    "Master and slave dataset root paths are the same!", None::<(String, String)>);

        let maybe_result = measure(|| {
            let master_users = master_dao.users(master_ds_uuid)?;
            let slave_users = slave_dao.users(slave_ds_uuid)?;
            check_diff!(master_users.len() == slave_users.len(), true,
                        "User count differs", Some((
                            format!("{} ({:?})", master_users.len(), master_users),
                            format!("{} ({:?})", slave_users.len(), slave_users)
                        )));
            for (i, (master_user, mut slave_user)) in master_users.iter().zip(slave_users.into_iter()).enumerate() {
                slave_user.ds_uuid = master_ds_uuid.clone();
                check_diff!(*master_user == slave_user, false,
                            format!("User #{i} differs"), Some((format!("{master_user:?}"), format!("{slave_user:?}"))));
            }
            Ok(vec![])
        }, |_: &Result<_>, t| log::info!("Users checked in {t} ms"))?;
        if !maybe_result.is_empty() { return Ok(maybe_result); }

        let master_chats = master_dao.chats(master_ds_uuid)?;
        let slave_chats = slave_dao.chats(slave_ds_uuid)?;
        check_diff!(master_chats.len() == slave_chats.len(), true,
                    "Chat count differs", Some((format!("{}", master_chats.len()), format!("{}", slave_chats.len()))));

        for (i, (master_cwd, slave_cwd)) in master_chats.iter().zip(slave_chats.iter()).enumerate() {
            let maybe_result = measure(|| {
                {
                    let mut slave_cwd = slave_cwd.clone();
                    slave_cwd.chat.ds_uuid = master_ds_uuid.clone();

                    check_diff!(PracticalEqTuple::new(&master_cwd.chat, &master_ds_root, master_cwd).practically_equals(
                                    &PracticalEqTuple::new(&slave_cwd.chat, &slave_ds_root, &slave_cwd))?, false,
                                format!("Chat #{i} differs"),
                                Some((format!("{:?}", master_cwd.chat), format!("{:?}", slave_cwd.chat))));
                }

                let msg_count = master_cwd.chat.msg_count as usize;
                let mut offset: usize = 0;
                while offset < msg_count {
                    let master_messages = master_dao.scroll_messages(&master_cwd.chat, offset, BATCH_SIZE)?;
                    let slave_messages = slave_dao.scroll_messages(&slave_cwd.chat, offset, BATCH_SIZE)?;
                    // This can only happen when message count is different
                    check_diff!(!master_messages.is_empty() && !slave_messages.is_empty(), false,
                                "Empty messages batch encountered", None::<(String, String)>);
                    check_diff!(master_messages.len() == slave_messages.len(), false,
                                format!("Messages size for chat {} differs", master_cwd.chat.qualified_name()),
                                Some((master_chats.len(), slave_chats.len())));

                    for (j, (master_msg, slave_msg)) in master_messages.iter().zip(slave_messages.iter()).enumerate() {
                        let master_pet = PracticalEqTuple::new(master_msg, &master_ds_root, master_cwd);
                        let slave_pet = PracticalEqTuple::new(slave_msg, &slave_ds_root, slave_cwd);
                        check_diff!(master_pet.practically_equals(&slave_pet)?, false,
                                    format!("Message #{j} for chat {} differs", master_cwd.chat.qualified_name()),
                                    Some((format!("{:?}", master_msg), format!("{:?}", slave_msg))));
                    }
                    offset += master_messages.len();
                }
                Ok(vec![])
            }, |_: &Result<_>, t| log::info!("Chat {} ({} messages) checked in {t} ms", slave_cwd.chat.qualified_name(),
                                                                                        slave_cwd.chat.msg_count))?;
            if !maybe_result.is_empty() { return Ok(maybe_result); }
        }

        Ok(differences)
    }, |_, t| log::info!("Dataset equality checked in {t} ms"))
}
