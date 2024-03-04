use std::cmp;
use std::path::{Path, PathBuf};

use deepsize::DeepSizeOf;
use itertools::Itertools;

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
    pub storage_path: PathBuf,
    pub ds_roots: HashMap<PbUuid, DatasetRoot>,
    pub cwms: HashMap<PbUuid, Vec<ChatWithMessages>>,
    cache: DaoCache,
}

impl InMemoryDao {
    pub fn new_single(name: String,
                      ds: Dataset,
                      ds_root: PathBuf,
                      myself_id: UserId,
                      users: Vec<User>,
                      cwms: Vec<ChatWithMessages>) -> Self {
        Self::new(name, ds_root.clone(), vec![DatasetEntry { ds, ds_root, myself_id, users, cwms }])
    }

    pub fn new(name: String,
               storage_path: PathBuf,
               data: Vec<DatasetEntry>) -> Self {
        let cache = DaoCache::new();
        let mut ds_roots = HashMap::new();
        let mut cwms_map = HashMap::new();
        let mut cache_inner = (*cache.inner).borrow_mut();
        cache_inner.initialized = true;
        for DatasetEntry { ds, ds_root, myself_id, users, cwms } in data {
            assert!(users.iter().any(|u| u.id() == myself_id));
            assert!(users.iter().all(|u| u.ds_uuid == ds.uuid));
            assert!(cwms.iter().all(|cwm| cwm.chat.as_ref().unwrap().ds_uuid == ds.uuid));
            let ds_uuid = ds.uuid().clone();
            cache_inner.datasets.push(ds);
            cache_inner.users.insert(ds_uuid.clone(), UserCacheForDataset {
                myself_id,
                user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
            });
            ds_roots.insert(ds_uuid.clone(),
                            DatasetRoot(ds_root.canonicalize().expect("Could not canonicalize dataset root")));
            cwms_map.insert(ds_uuid, cwms);
        }

        drop(cache_inner);

        InMemoryDao { name, storage_path, ds_roots, cwms: cwms_map, cache }
    }

    fn chat_members(&self, chat: &Chat) -> Result<Vec<User>> {
        let me = self.myself(chat.ds_uuid())?;
        let mut members = chat.member_ids.iter()
            .filter(|&id| *id != me.id)
            .map(|id| self.user_option(chat.ds_uuid(), *id)
                .unwrap()
                .unwrap_or_else(|| panic!("No member with id {id} found for chat {}", chat.qualified_name())))
            .sorted_by_key(|u| u.id)
            .collect_vec();
        members.insert(0, me);
        Ok(members)
    }

    fn cwm_option(&self, ds_uuid: &PbUuid, id: i64) -> Option<&ChatWithMessages> {
        self.cwms[ds_uuid].iter()
            .find(|cwm| cwm.chat.iter().any(|c| c.id == id))
    }

    fn messages_option(&self, ds_uuid: &PbUuid, chat_id: i64) -> Option<&Vec<Message>> {
        self.cwm_option(ds_uuid, chat_id).map(|cwm| &cwm.messages)
    }

    fn cwm_to_cwd(&self, cwm: &ChatWithMessages) -> ChatWithDetails {
        ChatWithDetails {
            chat: cwm.chat.clone().unwrap(),
            last_msg_option: cwm.messages.last().cloned(),
            members: self.chat_members(cwm.chat.as_ref().unwrap()).unwrap(),
        }
    }

    pub fn remove_orphan_users(&mut self) {
        let member_ids: HashSet<_> =
            self.cwms.values().flatten().flat_map(|cwm| &cwm.chat.as_ref().unwrap().member_ids).collect();

        let mut num_removed = 0;
        let mut cache = self.cache.inner.borrow_mut();
        for users_for_ds in cache.users.values_mut() {
            let user_ids = users_for_ds.user_by_id.keys().cloned().collect_vec();
            for user_id in user_ids {
                if !member_ids.contains(&*user_id) {
                    log::debug!("Removing orphan user {:?}", users_for_ds.user_by_id[&user_id]);
                    users_for_ds.user_by_id.remove(&user_id);
                    num_removed += 1;
                }
            }
        }

        if num_removed == 0 {
            log::debug!("No orphan users found");
        } else {
            log::debug!("Removed {num_removed} orphan users");
        }
    }
}

impl WithCache for InMemoryDao {
    fn get_cache_unchecked(&self) -> &DaoCache { &self.cache }

    fn get_cache_mut_unchecked(&mut self) -> &mut DaoCache { &mut self.cache }

    fn init_cache(&self, _inner: &mut DaoCacheInner) -> EmptyRes { Ok(()) }

    fn invalidate_cache(&self) -> EmptyRes { err!("Cannot invalidate cache of in-memory DAO!") }
}

impl ChatHistoryDao for InMemoryDao {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn storage_path(&self) -> &Path {
        &self.storage_path
    }

    fn dataset_root(&self, ds_uuid: &PbUuid) -> Result<DatasetRoot> {
        Ok(self.ds_roots[ds_uuid].clone())
    }

    fn chats_inner(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        Ok(self.cwms[ds_uuid].iter().map(|cwm| self.cwm_to_cwd(cwm)).collect_vec())
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        Ok(self.messages_option(chat.ds_uuid(), chat.id)
            .map(|msgs| cutout(msgs, offset, offset + limit))
            .unwrap_or_default())
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        Ok(self.messages_option(chat.ds_uuid(), chat.id)
            .map(|msgs| {
                cutout(msgs, subtract_or_zero!(msgs.len(), limit), msgs.len()).to_vec()
            })
            .unwrap_or_default())
    }

    fn messages_before_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let msgs = self.messages_option(chat.ds_uuid(), chat.id).unwrap();
        let idx = msgs.iter().rposition(|m| m.internal_id == *msg_id);
        match idx {
            None => err!("Message not found!"),
            Some(idx) => {
                Ok(cutout(msgs, subtract_or_zero!(idx, limit), idx))
            }
        }
    }

    fn messages_after_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let msgs = self.messages_option(chat.ds_uuid(), chat.id).unwrap();
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
        let msgs = self.messages_option(chat.ds_uuid(), chat.id).unwrap();
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
        let msgs = self.messages_option(chat.ds_uuid(), chat.id).unwrap();
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
        let messages = self.messages_option(chat.ds_uuid(), chat.id).unwrap();
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
        Ok(self.messages_option(chat.ds_uuid(), chat.id).unwrap()
            .iter().find(|m| m.source_id_option.iter().contains(&*source_id)).cloned())
    }

    fn as_mutable(&mut self) -> Result<&mut dyn MutableChatHistoryDao> {
        err!("InMemoryDao does not implement MutableChatHistoryDao")
    }

    fn as_shiftable(&mut self) -> Result<&mut dyn ShiftableChatHistoryDao> {
        Ok(self)
    }
}

impl ShiftableChatHistoryDao for InMemoryDao {
    fn shift_dataset_time(&mut self, uuid: &PbUuid, hours_shift: i32) -> EmptyRes {
        let timestamp_shift: i64 = (hours_shift * 60 * 60).into();
        let cwms = self.cwms.get_mut(uuid).unwrap();
        for cwm in cwms.iter_mut() {
            for m in cwm.messages.iter_mut() {
                m.timestamp += timestamp_shift;
                match m.typed_mut() {
                    Typed::Regular(mr) =>
                        mr.edit_timestamp_option.iter_mut().for_each(|ts| *ts += timestamp_shift),
                    Typed::Service(_) => { /* NOOP */ }
                }
            }
        }
        Ok(())
    }
}


pub struct DatasetEntry {
    pub ds: Dataset,
    pub ds_root: PathBuf,
    pub myself_id: UserId,
    pub users: Vec<User>,
    pub cwms: Vec<ChatWithMessages>,
}

fn cutout<T: Clone>(slice: &[T], start_inc: usize, end_exc: usize) -> Vec<T> {
    fn sanitize<T>(idx: usize, slice: &[T]) -> usize {
        cmp::min(cmp::max(idx, 0), slice.len())
    }
    slice[sanitize(start_inc, slice)..sanitize(end_exc, slice)].to_vec()
}
