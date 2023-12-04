use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::path::Path;
use std::thread::JoinHandle;
use deepsize::DeepSizeOf;

use crate::*;
use crate::protobuf::history::*;

pub mod in_memory_dao;
pub mod sqlite_dao;
pub mod grpc_remote_dao;

pub trait WithCache {
    /// For internal use
    fn get_cache_unchecked(&self) -> &DaoCache;

    /// For internal use: lazily initialize the cache, and return a reference to it
    fn init_cache(&self, inner: &mut DaoCacheInner) -> EmptyRes;

    /// For internal use: lazily initialize the cache, and return a reference to immutable inner cache
    fn get_cache(&self) -> Result<Ref<DaoCacheInner>> {
        let cache = self.get_cache_unchecked();
        if cache.inner.borrow().initialized == false {
            let mut inner_mut = cache.inner.borrow_mut();
            self.init_cache(&mut *inner_mut)?;
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
        Ok(self.get_cache()?.users[ds_uuid].myself.clone())
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
        let users_cache = cache.users.get(ds_uuid).context("Dataset has no users!")?;
        let users = users_cache.user_by_id.values().cloned().collect_vec();
        Ok((users, UserId(users_cache.myself.id)))
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

    /// Count messages between the given ones (inclusive). Messages must be present.
    fn messages_slice_len(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<usize>;

    /** Returns N messages before and N at-or-after the given date */
    fn messages_around_date(&self, chat: &Chat, date_ts: Timestamp, limit: usize) -> Result<(Vec<Message>, Vec<Message>)>;

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>>;

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>>;

    /** Whether given data path is the one loaded in this DAO */
    fn is_loaded(&self, storage_path: &Path) -> bool {
        self.storage_path() == storage_path
    }

    /// Return self as mutable if applicable, otherwise error out
    fn as_mutable(&mut self) -> Result<&mut dyn MutableChatHistoryDao>;
}

pub trait MutableChatHistoryDao: ChatHistoryDao {
    fn backup(&mut self) -> Result<JoinHandle<()>>;

    // Inserts dataset as-is, with the UUID already set.
    fn insert_dataset(&mut self, ds: Dataset) -> Result<Dataset>;

    fn update_dataset(&mut self, ds: Dataset) -> Result<Dataset>;

    /// Delete a dataset with all the related entities. Deleted dataset root will be moved to backup folder.
    fn delete_dataset(&mut self, uuid: PbUuid) -> EmptyRes;

    fn insert_user(&mut self, user: User, is_myself: bool) -> Result<User>;

    /// Update a user, renaming relevant personal chats and updating messages mentioning that user in plaintext.
    fn update_user(&mut self, user: User) -> Result<User>;

    /// Copies image (if any) from dataset root.
    fn insert_chat(&mut self, chat: Chat, src_ds_root: &DatasetRoot) -> Result<Chat>;

    /// Note that chat members and image won't be changed!
    fn update_chat(&mut self, chat: Chat) -> Result<Chat>;

    /// Delete a chat, as well as orphan users. Deleted files will be moved to backup folder.
    fn delete_chat(&mut self, chat: Chat) -> EmptyRes;

    /// Insert a new message for the given chat.
    /// Internal ID will be ignored.
    /// Content will be resolved based on the given dataset root and copied accordingly.
    fn insert_messages(&mut self, msgs: Vec<Message>, chat: &Chat, src_ds_root: &DatasetRoot) -> EmptyRes;
}

type UserCache = HashMap<PbUuid, UserCacheForDataset>;

#[derive(DeepSizeOf)]
pub struct UserCacheForDataset {
    pub myself: User,
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

pub fn ensure_datasets_are_equal(src: &dyn ChatHistoryDao,
                                 dst: &dyn ChatHistoryDao,
                                 src_ds_uuid: &PbUuid,
                                 dst_ds_uuid: &PbUuid) -> EmptyRes {
    measure(|| {
        let src_ds = src.datasets()?.into_iter().find(|ds| ds.uuid() == src_ds_uuid)
            .with_context(|| format!("Dataset {} not found in source DAO!", src_ds_uuid.value))?;
        let mut dst_ds = src.datasets()?.into_iter().find(|ds| ds.uuid() == dst_ds_uuid)
            .with_context(|| format!("Dataset {} not found in destination DAO!", dst_ds_uuid.value))?;
        dst_ds.uuid = Some(src_ds_uuid.clone());
        require!(src_ds == dst_ds, "Destination dataset is not the same as original");
        let src_ds_root = src.dataset_root(src_ds_uuid)?;
        let dst_ds_root = dst.dataset_root(dst_ds_uuid)?;
        require!(*src_ds_root != *dst_ds_root, "Source and destination dataset root paths are the same!");

        measure(|| {
            let src_users = src.users(src_ds_uuid)?;
            let dst_users = dst.users(dst_ds_uuid)?;
            require!(src_users.len() == dst_users.len(),
                     "User count differs:\nWas    {} ({:?})\nBecame {} ({:?})",
                     src_users.len(), src_users, dst_users.len(), dst_users);
            for (i, (src_user, mut dst_user)) in src_users.iter().zip(dst_users.into_iter()).enumerate() {
                dst_user.ds_uuid = Some(src_ds_uuid.clone());
                require!(*src_user == dst_user,
                         "User #{i} differs:\nWas    {:?}\nBecame {:?}", src_user, dst_user);
            }
            Ok(())
        }, |_, t| log::info!("Users checked in {t} ms"))?;

        let src_chats = src.chats(src_ds_uuid)?;
        let dst_chats = dst.chats(dst_ds_uuid)?;
        require!(src_chats.len() == dst_chats.len(),
                 "Chat count differs:\nWas    {}\nBecame {}", src_chats.len(), dst_chats.len());

        for (i, (src_cwd, dst_cwd)) in src_chats.iter().zip(dst_chats.iter()).enumerate() {
            measure(|| {
                let mut dst_cwd = dst_cwd.clone();
                dst_cwd.chat.ds_uuid = Some(src_ds_uuid.clone());

                require!(PracticalEqTuple::new(&src_cwd.chat, &src_ds_root, src_cwd).practically_equals(
                        &PracticalEqTuple::new(&dst_cwd.chat, &dst_ds_root, &dst_cwd))?,
                         "Chat #{i} differs:\nWas    {:?}\nBecame {:?}", src_cwd.chat, dst_cwd.chat);

                let msg_count = src_cwd.chat.msg_count as usize;
                let mut offset: usize = 0;
                while offset < msg_count {
                    let src_messages = src.scroll_messages(&src_cwd.chat, offset, BATCH_SIZE)?;
                    let dst_messages = dst.scroll_messages(&dst_cwd.chat, offset, BATCH_SIZE)?;
                    require!(!src_messages.is_empty() && !dst_messages.is_empty(),
                             "Empty messages batch returned, either flawed batching logic or incorrect src_chat.msgCount");
                    require!(src_messages.len() == dst_messages.len(),
                             "Messages size for chat {} differs:\nWas    {}\nBecame {}",
                             src_cwd.chat.qualified_name(), src_chats.len(), dst_chats.len());

                    for (j, (src_msg, dst_msg)) in src_messages.iter().zip(dst_messages.iter()).enumerate() {
                        let src_pet = PracticalEqTuple::new(src_msg, &src_ds_root, &src_cwd);
                        let dst_pet = PracticalEqTuple::new(dst_msg, &dst_ds_root, &dst_cwd);
                        require!(src_pet.practically_equals(&dst_pet)?,
                                 "Message #{j} for chat {} differs:\nWas    {:?}\nBecame {:?}",
                                 src_cwd.chat.qualified_name(), src_msg, dst_msg);
                    }
                    offset += src_messages.len();
                }
                Ok(())
            }, |_, t| log::info!("Chat {} ({} messages) checked in {t} ms", dst_cwd.chat.qualified_name(),
                                                                            dst_cwd.chat.msg_count))?;
        }

        Ok(())
    }, |_, t| log::info!("Dataset '{}' checked in {t} ms", src_ds_uuid.value))
}
