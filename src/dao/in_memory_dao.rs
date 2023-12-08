use std::cmp;
use std::path::{Path, PathBuf};

use deepsize::DeepSizeOf;

use crate::*;
use crate::protobuf::history::message::Typed;

use super::*;

#[cfg(test)]
#[path = "in_memory_dao_tests.rs"]
mod tests;

macro_rules! subtract_or_zero {
    ($idx:expr, $limit:expr) => { if $idx > $limit { $idx - $limit } else { 0 } };
}

#[derive(DeepSizeOf)]
pub struct InMemoryDao {
    pub name: String,
    pub ds_uuid: PbUuid,
    pub ds_root: PathBuf,
    pub cwms: Vec<ChatWithMessages>,
    cache: DaoCache,
}

impl InMemoryDao {
    pub fn new(name: String,
               dataset: Dataset,
               ds_root: PathBuf,
               myself: User,
               users: Vec<User>,
               cwms: Vec<ChatWithMessages>) -> Self {
        let ds_root = ds_root.canonicalize().expect("Could not canonicalize dataset root");
        let ds_uuid = dataset.uuid().clone();
        assert!(users.iter().any(|u| *u == myself));

        let cache = DaoCache::new();
        let mut cache_inner = (*cache.inner).borrow_mut();
        cache_inner.initialized = true;
        cache_inner.datasets = vec![dataset];
        cache_inner.users.insert(ds_uuid.clone(), UserCacheForDataset {
            myself,
            user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
        });
        drop(cache_inner);

        InMemoryDao { name, ds_uuid, ds_root, cwms, cache }
    }

    pub fn in_mem_dataset(&self) -> Dataset {
        self.get_cache().unwrap().datasets.first().unwrap().clone()
    }

    pub fn in_mem_myself(&self) -> User {
        self.get_cache().unwrap().users[&self.ds_uuid].myself.clone()
    }

    pub fn in_mem_users(&self) -> Vec<User> {
        self.users(&self.ds_uuid).unwrap()
    }

    fn chat_members(&self, chat: &Chat) -> Vec<User> {
        let me = self.in_mem_myself();
        let mut members = chat.member_ids.iter()
            .filter(|&id| *id != me.id)
            .map(|id| self.user_option(chat.ds_uuid(), *id)
                .unwrap()
                .unwrap_or_else(|| panic!("No member with id {id} found for chat {}", chat.qualified_name())))
            .sorted_by_key(|u| u.id)
            .collect_vec();
        members.insert(0, me);
        members
    }

    fn cwm_option(&self, id: i64) -> Option<&ChatWithMessages> {
        self.cwms.iter()
            .find(|cwm| cwm.chat.iter().any(|c| c.id == id))
    }

    fn messages_option(&self, chat_id: i64) -> Option<&Vec<Message>> {
        self.cwm_option(chat_id).map(|cwm| &cwm.messages)
    }

    fn cwm_to_cwd(&self, cwm: &ChatWithMessages) -> ChatWithDetails {
        ChatWithDetails {
            chat: cwm.chat.clone().unwrap(),
            last_msg_option: cwm.messages.last().cloned(),
            members: self.chat_members(cwm.chat.as_ref().unwrap()),
        }
    }
}

impl WithCache for InMemoryDao {
    fn get_cache_unchecked(&self) -> &DaoCache { &self.cache }

    fn init_cache(&self, _inner: &mut DaoCacheInner) -> EmptyRes { Ok(()) }

    fn invalidate_cache(&self) -> EmptyRes { err!("Cannot invalidate cache of in-memory DAO!") }
}

impl ChatHistoryDao for InMemoryDao {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn storage_path(&self) -> &Path {
        &self.ds_root
    }

    fn dataset_root(&self, _ds_uuid: &PbUuid) -> Result<DatasetRoot> {
        Ok(DatasetRoot(self.storage_path().to_owned()))
    }

    fn chats_inner(&self, _ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        Ok(self.cwms.iter().map(|cwm| self.cwm_to_cwd(cwm)).collect_vec())
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        Ok(self.messages_option(chat.id)
            .map(|msgs| cutout(msgs, offset, offset + limit))
            .unwrap_or_default())
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        Ok(self.messages_option(chat.id)
            .map(|msgs| {
                cutout(msgs, subtract_or_zero!(msgs.len(), limit), msgs.len()).to_vec()
            })
            .unwrap_or_default())
    }

    fn messages_before_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let msgs = self.messages_option(chat.id).unwrap();
        let idx = msgs.iter().rposition(|m| m.internal_id == *msg_id);
        match idx {
            None => err!("Message not found!"),
            Some(idx) => {
                Ok(cutout(msgs, subtract_or_zero!(idx, limit), idx))
            }
        }
    }

    fn messages_after_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let msgs = self.messages_option(chat.id).unwrap();
        let idx = msgs.iter().position(|m| m.internal_id == *msg_id);
        match idx {
            None => err!("Message not found!"),
            Some(idx) => {
                let start = idx + 1;
                Ok(cutout(msgs, start, start + limit))
            }
        }
    }

    fn messages_slice(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<Vec<Message>> {
        let msgs = self.messages_option(chat.id).unwrap();
        let idx1 = msgs.iter().position(|m| m.internal_id == *msg1_id);
        let idx2 = msgs.iter().rposition(|m| m.internal_id == *msg2_id);
        match (idx1, idx2) {
            (None, _) => err!("Message 1 not found!"),
            (_, None) => err!("Message 2 not found!"),
            (Some(idx1), Some(idx2)) if idx1 > idx2 => Ok(vec![]),
            (Some(idx1), Some(idx2)) => Ok(msgs[idx1..=idx2].to_vec())
        }
    }

    fn messages_abbreviated_slice_inner(&self,
                                        chat: &Chat,
                                        msg1_id: MessageInternalId,
                                        msg2_id: MessageInternalId,
                                        combined_limit: usize,
                                        abbreviated_limit: usize) -> Result<(Vec<Message>, usize, Vec<Message>)> {
        let msgs = self.messages_option(chat.id).unwrap();
        let idx1 = msgs.iter().position(|m| m.internal_id == *msg1_id);
        let idx2 = msgs.iter().rposition(|m| m.internal_id == *msg2_id);
        match (idx1, idx2) {
            (None, _) => err!("Message 1 not found!"),
            (_, None) => err!("Message 2 not found!"),
            (Some(idx1), Some(idx2)) if idx1 > idx2 =>
                Ok((vec![], 0, vec![])),
            (Some(idx1), Some(idx2)) if idx2 - idx1 < combined_limit =>
                Ok((msgs[idx1..=idx2].to_vec(), 0, vec![])),
            (Some(idx1), Some(idx2)) => {
                let left_msgs = msgs[idx1..(idx1 + abbreviated_limit)].to_vec();
                let right_msgs = msgs[(idx2 - abbreviated_limit + 1)..=idx2].to_vec();
                let in_between = idx2 - idx1 + 1 - 2 * abbreviated_limit;
                Ok((left_msgs, in_between, right_msgs))
            }
        }
    }

    fn messages_slice_len(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<usize> {
        // Inefficient!
        self.messages_slice(chat, msg1_id, msg2_id).map(|msgs| msgs.len())
    }

    fn messages_around_date(&self, chat: &Chat, date_ts: Timestamp, limit: usize)
                            -> Result<(Vec<Message>, Vec<Message>)> {
        let messages = self.messages_option(chat.id).unwrap();
        let idx = messages.iter().position(|m| m.timestamp >= *date_ts);
        Ok(match idx {
            None => {
                // Not found
                (self.last_messages(chat, limit)?, vec![])
            }
            Some(idx) => {
                let (p1, p2) = messages.split_at(idx);
                (cutout(p1, subtract_or_zero!(p1.len(), limit), p1.len()),
                 cutout(p2, 0, limit))
            }
        })
    }

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>> {
        Ok(self.messages_option(chat.id).unwrap()
            .iter().find(|m| m.source_id_option.iter().contains(&*source_id)).cloned())
    }

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>> {
        Ok(self.messages_option(chat.id).unwrap()
            .iter().find(|m| m.internal_id == *internal_id).cloned())
    }

    fn as_mutable(&mut self) -> Result<&mut dyn MutableChatHistoryDao> {
        err!("InMemoryDao does not implement MutableChatHistoryDao")
    }

    fn as_shiftable(&mut self) -> Result<&mut dyn ShiftableChatHistoryDao> {
        Ok(self)
    }
}

impl ShiftableChatHistoryDao for InMemoryDao {
    fn shift_dataset_time(&mut self, uuid: PbUuid, hours_shift: i32) -> EmptyRes {
        require!(uuid == self.ds_uuid, "Wrong dataset UUID!");
        let timestamp_shift: i64 = (hours_shift * 60 * 60).into();
        for cwm in self.cwms.iter_mut() {
            for m in cwm.messages.iter_mut() {
                m.timestamp = m.timestamp + timestamp_shift;
                match m.typed_mut() {
                    Typed::Regular(mr) =>
                        mr.edit_timestamp_option.iter_mut().for_each(|ts| *ts = *ts + timestamp_shift),
                    Typed::Service(_) => { /* NOOP */ }
                }
            }
        }
        Ok(())
    }
}

fn cutout<T: Clone>(slice: &[T], start_inc: usize, end_exc: usize) -> Vec<T> {
    fn sanitize<T>(idx: usize, slice: &[T]) -> usize {
        cmp::min(cmp::max(idx, 0), slice.len())
    }
    slice[sanitize(start_inc, slice)..sanitize(end_exc, slice)].to_vec()
}
