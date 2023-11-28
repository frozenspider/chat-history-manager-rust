use std::sync::{Arc, Mutex};

use tonic::Request;

use crate::*;
use crate::protobuf::history::history_dao_service_server::HistoryDaoService;

use super::*;

macro_rules! with_dao_by_key {
    ($self:ident, $req:ident, $dao:ident, $code:block) => {
        $self.process_request_with_dao(&$req, &$req.get_ref().key, |#[allow(unused)] $req, $dao| { $code })
    };
}

macro_rules! uuid_from_req { ($req:ident) => { from_req!($req.ds_uuid) }; }
macro_rules! chat_from_req { ($req:ident) => { from_req!($req.chat) }; }

#[tonic::async_trait]
impl HistoryDaoService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn save_as(&self, req: Request<SaveAsRequest>) -> TonicResult<LoadedFile> {
        let mut new_dao: Option<DaoRefCell> = None;
        let mut new_key: String = String::new();

        let res = with_dao_by_key!(self, req, dao, {
            let new_storage_path =
                dao.storage_path().parent().map(|p| p.join(&req.new_folder_name)).context("Cannot resolve new folder")?;
            if !new_storage_path.exists() {
                bail!("Path does not exist!")
            }
            for entry in fs::read_dir(&new_storage_path)? {
                let file_name = path_file_name(&entry?.path())?.to_owned();
                if !file_name.starts_with(".") {
                    bail!("Directory is not empty! Found {file_name} there")
                }
            }
            let new_db_file = new_storage_path.join(SqliteDao::FILENAME);
            let sqlite_dao = SqliteDao::create(&new_db_file)?;
            sqlite_dao.copy_all_from(dao)?;
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
                path: dao.dataset_root(uuid_from_req!(req))?.0.to_str().unwrap().to_owned()
            })
        })
    }

    async fn myself(&self, req: Request<MyselfRequest>) -> TonicResult<MyselfResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MyselfResponse { myself: Some(dao.myself(uuid_from_req!(req))?) })
        })
    }

    async fn users(&self, req: Request<UsersRequest>) -> TonicResult<UsersResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(UsersResponse { users: dao.users(uuid_from_req!(req))? })
        })
    }

    async fn chats(&self, req: Request<ChatsRequest>) -> TonicResult<ChatsResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(ChatsResponse {
                cwds: dao.chats(uuid_from_req!(req))?
                    .into_iter()
                    .map(|cwd| cwd.into())
                    .collect_vec()
            })
        })
    }

    async fn scroll_messages(&self, req: Request<ScrollMessagesRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.scroll_messages(chat_from_req!(req), req.offset as usize, req.limit as usize)?
            })
        })
    }

    async fn last_messages(&self, req: Request<LastMessagesRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.last_messages(chat_from_req!(req), req.limit as usize)?
            })
        })
    }

    async fn messages_before(&self, req: Request<MessagesBeforeRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_before(chat_from_req!(req),
                                              MessageInternalId(req.message_internal_id),
                                              req.limit as usize)?
            })
        })
    }

    async fn messages_after(&self, req: Request<MessagesAfterRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_after(chat_from_req!(req),
                                             MessageInternalId(req.message_internal_id),
                                             req.limit as usize)?
            })
        })
    }

    async fn messages_slice(&self, req: Request<MessagesSliceRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_slice(chat_from_req!(req),
                                             MessageInternalId(req.message_internal_id_1),
                                             MessageInternalId(req.message_internal_id_2))?
            })
        })
    }

    async fn messages_slice_len(&self, req: Request<MessagesSliceRequest>) -> TonicResult<CountMessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(CountMessagesResponse {
                messages_count: dao.messages_slice_len(chat_from_req!(req),
                                                       MessageInternalId(req.message_internal_id_1),
                                                       MessageInternalId(req.message_internal_id_2))? as i32
            })
        })
    }

    async fn message_option(&self, req: Request<MessageOptionRequest>) -> TonicResult<MessageOptionResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessageOptionResponse {
                message: dao.message_option(chat_from_req!(req), MessageSourceId(req.source_id))?
            })
        })
    }

    async fn message_option_by_internal_id(&self, req: Request<MessageOptionByInternalIdRequest>) -> TonicResult<MessageOptionResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessageOptionResponse {
                message: dao.message_option_by_internal_id(chat_from_req!(req), MessageInternalId(req.internal_id))?
            })
        })
    }

    async fn is_loaded(&self, req: Request<IsLoadedRequest>) -> TonicResult<IsLoadedResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(IsLoadedResponse {
                is_loaded: dao.is_loaded(&Path::new(&req.storage_path))
            })
        })
    }
}
