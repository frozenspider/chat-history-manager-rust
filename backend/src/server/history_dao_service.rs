use std::fs;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use tonic::Request;

use crate::dao::sqlite_dao::SqliteDao;
use crate::protobuf::history::history_dao_service_server::HistoryDaoService;

use super::*;

macro_rules! with_dao_by_key {
    ($self:ident, $req:ident, $dao:ident, $code:block) => {
        $self.process_request_with_dao(&$req, &$req.get_ref().key, |#[allow(unused)] $req, $dao| { $code })
    };
}

#[tonic::async_trait]
impl HistoryDaoService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn save_as(&self, req: Request<SaveAsRequest>) -> TonicResult<LoadedFile> {
        let mut new_dao: Option<DaoRefCell> = None;
        let mut new_key: String = String::new();

        let res = with_dao_by_key!(self, req, dao, {
            let new_storage_path =
                dao.storage_path().parent().map(|p| p.join(&req.new_folder_name)).context("Cannot resolve new folder")?;
            if !new_storage_path.exists() {
                fs::create_dir(&new_storage_path)?;
            }
            for entry in fs::read_dir(&new_storage_path)? {
                let file_name = path_file_name(&entry?.path())?.to_owned();
                if !file_name.starts_with('.') {
                    bail!("Directory {} is not empty! Found {file_name} there", new_storage_path.display())
                }
            }
            let new_db_file = new_storage_path.join(SqliteDao::FILENAME);
            let sqlite_dao = SqliteDao::create(&new_db_file)?;
            sqlite_dao.copy_datasets_from(dao, &dao.datasets()?.into_iter().map(|ds| ds.uuid).collect_vec())?;
            new_key =  path_to_str(&new_db_file)?.to_owned();
            let name = sqlite_dao.name().to_owned();
            new_dao = Some(DaoRefCell::new(Box::new(sqlite_dao)));
            Ok(LoadedFile { key: new_key.clone(), name })
        });

        if let Some(new_dao) = new_dao {
            let mut self_lock = lock_or_status(self)?;
            if self_lock.loaded_daos.contains_key(&new_key) {
                return Err(Status::new(Code::Internal,
                                       format!("Key {} is already taken!", new_key)));
            }
            self_lock.loaded_daos.insert(new_key, new_dao);
        }

        res
    }

    async fn name(&self, req: Request<NameRequest>) -> TonicResult<NameResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(NameResponse { name: dao.name().to_owned() })
        })
    }

    async fn storage_path(&self, req: Request<StoragePathRequest>) -> TonicResult<StoragePathResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(StoragePathResponse { path: dao.storage_path().to_str().unwrap().to_owned() })
        })
    }

    async fn datasets(&self, req: Request<DatasetsRequest>) -> TonicResult<DatasetsResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(DatasetsResponse { datasets: dao.datasets()? })
        })
    }

    async fn dataset_root(&self, req: Request<DatasetRootRequest>) -> TonicResult<DatasetRootResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(DatasetRootResponse {
                path: dao.dataset_root(&req.ds_uuid)?.0.to_str().unwrap().to_owned()
            })
        })
    }

    async fn myself(&self, req: Request<MyselfRequest>) -> TonicResult<MyselfResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MyselfResponse { myself: dao.myself(&req.ds_uuid)? })
        })
    }

    async fn users(&self, req: Request<UsersRequest>) -> TonicResult<UsersResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(UsersResponse { users: dao.users(&req.ds_uuid)? })
        })
    }

    async fn chats(&self, req: Request<ChatsRequest>) -> TonicResult<ChatsResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(ChatsResponse {
                cwds: dao.chats(&req.ds_uuid)?
                    .into_iter()
                    .map(|cwd| cwd.into())
                    .collect_vec()
            })
        })
    }

    async fn scroll_messages(&self, req: Request<ScrollMessagesRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.scroll_messages(&req.chat, req.offset as usize, req.limit as usize)?
            })
        })
    }

    async fn last_messages(&self, req: Request<LastMessagesRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.last_messages(&req.chat, req.limit as usize)?
            })
        })
    }

    async fn messages_before(&self, req: Request<MessagesBeforeRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_before(&req.chat,
                                              MessageInternalId(req.message_internal_id),
                                              req.limit as usize)?
            })
        })
    }

    async fn messages_after(&self, req: Request<MessagesAfterRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_after(&req.chat,
                                             MessageInternalId(req.message_internal_id),
                                             req.limit as usize)?
            })
        })
    }

    async fn messages_slice(&self, req: Request<MessagesSliceRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_slice(&req.chat,
                                             MessageInternalId(req.message_internal_id_1),
                                             MessageInternalId(req.message_internal_id_2))?
            })
        })
    }

    async fn messages_abbreviated_slice(&self, req: Request<MessagesAbbreviatedSliceRequest>) -> TonicResult<MessagesAbbreviatedSliceResponse> {
        with_dao_by_key!(self, req, dao, {
            let (left_messages, in_between, right_messages) =
                dao.messages_abbreviated_slice(&req.chat,
                                               MessageInternalId(req.message_internal_id_1),
                                               MessageInternalId(req.message_internal_id_2),
                                               req.combined_limit as usize,
                                               req.abbreviated_limit as usize)?;
            Ok(MessagesAbbreviatedSliceResponse { left_messages, in_between: in_between as i32, right_messages })
        })
    }

    async fn messages_slice_len(&self, req: Request<MessagesSliceRequest>) -> TonicResult<CountMessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(CountMessagesResponse {
                messages_count: dao.messages_slice_len(&req.chat,
                                                       MessageInternalId(req.message_internal_id_1),
                                                       MessageInternalId(req.message_internal_id_2))? as i32
            })
        })
    }

    async fn message_option(&self, req: Request<MessageOptionRequest>) -> TonicResult<MessageOptionResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessageOptionResponse {
                message: dao.message_option(&req.chat, MessageSourceId(req.source_id))?
            })
        })
    }

    async fn is_loaded(&self, req: Request<IsLoadedRequest>) -> TonicResult<IsLoadedResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(IsLoadedResponse {
                is_loaded: dao.is_loaded(Path::new(&req.storage_path))
            })
        })
    }

    //
    // Mutable DAO endpoints
    //

    async fn backup(&self, req: Request<BackupRequest>) -> TonicResult<Empty> {
        with_dao_by_key!(self, req, dao, {
            // If DAO does not support backups, silently ignore the call
            if let Ok(dao_m) = dao.as_mutable() {
                dao_m.backup()?;
            }
            Ok(Empty {})
        })
    }

    async fn update_dataset(&self, req: Request<UpdateDatasetRequest>) -> TonicResult<UpdateDatasetResponse> {
        with_dao_by_key!(self, req, dao, {
            let dataset = req.dataset.clone();
            let dataset = dao.as_mutable()?.update_dataset(dataset.uuid.clone(), dataset)?;
            Ok(UpdateDatasetResponse { dataset })
        })
    }

    async fn delete_dataset(&self, req: Request<DeleteDatasetRequest>) -> TonicResult<Empty> {
        with_dao_by_key!(self, req, dao, {
            let uuid = req.uuid.clone();
            dao.as_mutable()?.delete_dataset(uuid)?;
            Ok(Empty {})
        })
    }

    async fn shift_dataset_time(&self, req: Request<ShiftDatasetTimeRequest>) -> TonicResult<Empty> {
        with_dao_by_key!(self, req, dao, {
            let uuid = req.uuid.clone();
            dao.as_shiftable()?.shift_dataset_time(&uuid, req.hours_shift)?;
            Ok(Empty {})
        })
    }

    async fn update_user(&self, req: Request<UpdateUserRequest>) -> TonicResult<UpdateUserResponse> {
        with_dao_by_key!(self, req, dao, {
            let user = req.user.clone();
            let user = dao.as_mutable()?.update_user(user.id(), user)?;
            Ok(UpdateUserResponse { user })
        })
    }

    async fn update_chat(&self, req: Request<UpdateChatRequest>) -> TonicResult<UpdateChatResponse> {
        with_dao_by_key!(self, req, dao, {
            let uuid = req.uuid.clone();
            let old_cwd = dao.chat_option(&uuid, req.old_id)?.context("Chat not found")?;
            let chat = Chat { id: req.new_id, ..old_cwd.chat };
            let chat = dao.as_mutable()?.update_chat(ChatId(req.old_id), chat)?;
            Ok(UpdateChatResponse { chat })
        })
    }

    async fn delete_chat(&self, req: Request<DeleteChatRequest>) -> TonicResult<Empty> {
        with_dao_by_key!(self, req, dao, {
            let chat = req.chat.clone();
            dao.as_mutable()?.delete_chat(chat)?;
            Ok(Empty {})
        })
    }

    async fn combine_chats(&self, req: Request<CombineChatsRequest>) -> TonicResult<Empty> {
        with_dao_by_key!(self, req, dao, {
            let master_chat = req.master_chat.clone();
            let slave_chat = req.slave_chat.clone();
            dao.as_mutable()?.combine_chats(master_chat, slave_chat)?;
            Ok(Empty {})
        })
    }
}
