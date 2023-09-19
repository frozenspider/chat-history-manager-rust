use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::protobuf::history::*;
use crate::entity_utils::*;
use crate::*;

pub mod in_memory_dao;

/**
 * Everything except for messages should be pre-cached and readily available.
 * Should support equality.
 */
pub trait ChatHistoryDao {
    /** User-friendly name of a loaded data */
    fn name(&self) -> &str;

    /** Directory which stores eveything - including database itself at the root level */
    fn storage_path(&self) -> &Path;

    fn datasets(&self) -> Vec<Dataset>;

    /** Directory which stores eveything in the dataset. All files are guaranteed to have this as a prefix. */
    fn dataset_root(&self, ds_uuid: &PbUuid) -> DatasetRoot;

    /** List all files referenced by entities of this dataset. Some might not exist. */
    fn dataset_files(&self, ds_uuid: &PbUuid) -> HashSet<PathBuf>;

    fn myself(&self, ds_uuid: &PbUuid) -> User;

    /** Contains myself as the first element. Order must be stable. Method is expected to be fast. */
    fn users(&self, ds_uuid: &PbUuid) -> Vec<User>;

    fn user_option(&self, ds_uuid: &PbUuid, id: i64) -> Option<User>;

    fn chats(&self, ds_uuid: &PbUuid) -> Vec<ChatWithDetails>;

    fn chat_option(&self, ds_uuid: &PbUuid, id: i64) -> Option<ChatWithDetails>;

    /// Return N messages after skipping first M of them. Trivial pagination in a nutshell.
    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Vec<Message>;

    fn first_messages(&self, chat: &Chat, limit: usize) -> Vec<Message> {
        self.scroll_messages(chat, 0, limit)
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Vec<Message>;

    /**
     * Return N messages before the given one (inclusive).
     * Message must be present, so the result would contain at least one element.
     */
    fn messages_before(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        if limit == 0 { bail!("Limit is zero!"); }
        let result = self.messages_before_impl(chat, msg, limit)?;
        assert!(!result.is_empty());
        assert!(result.len() <= limit);
        assert_eq!(result.last().as_ref().unwrap().source_id_option, msg.source_id_option);
        assert_eq!(result.last().as_ref().unwrap().internal_id, msg.internal_id);
        Ok(result)
    }

    fn messages_before_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>>;

    // TODO: Rework to exclude given message itself
    /**
     * Return N messages after the given one (inclusive).
     * Message must be present, so the result would contain at least one element.
     */
    fn messages_after(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>> {
        if limit == 0 { bail!("Limit is zero!"); }
        let result = self.messages_after_impl(chat, msg, limit)?;
        assert!(!result.is_empty());
        assert!(result.len() <= limit);
        assert_eq!(&result[0].source_id_option, &msg.source_id_option);
        assert_eq!(&result[0].internal_id, &msg.internal_id);
        Ok(result)
    }

    fn messages_after_impl(&self, chat: &Chat, msg: &Message, limit: usize) -> Result<Vec<Message>>;

    /**
     * Return N messages between the given ones (inclusive).
     * Messages must be present, so the result would contain at least one element (if both are the same message).
     */
    fn messages_between(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<Vec<Message>> {
        let result = self.messages_between_impl(chat, msg1, msg2)?;
        assert!(!result.is_empty());
        assert_eq!(result[0].source_id_option, msg1.source_id_option);
        assert_eq!(result[0].internal_id, msg1.internal_id);
        assert_eq!(result.last().unwrap().source_id_option, msg2.source_id_option);
        assert_eq!(result.last().unwrap().internal_id, msg2.internal_id);
        Ok(result)
    }

    fn messages_between_impl(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> Result<Vec<Message>>;

    /**
     * Count messages between the given ones (exclusive, unlike messages_between).
     * Messages must be present.
     */
    fn count_messages_between(&self, chat: &Chat, msg1: &Message, msg2: &Message) -> usize;

    /** Returns N messages before and N at-or-after the given date */
    fn messages_around_date(&self, chat: &Chat, date_ts: Timestamp, limit: usize) -> (Vec<Message>, Vec<Message>);

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Option<Message>;

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Option<Message>;

    /** Whether given data path is the one loaded in this DAO */
    fn is_loaded(&self, storage_path: &Path) -> bool;
}
