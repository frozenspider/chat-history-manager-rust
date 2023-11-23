use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt};
use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::*;
use crate::dao::*;
use crate::protobuf::history::history_loader_service_client::HistoryLoaderServiceClient;

type Client = HistoryLoaderServiceClient<Channel>;

pub struct GrpcRemoteDao {
    name: String,
    key: String,
    storage_path: PathBuf,
    runtime_handle: Handle,
    cache: DaoCache,
    client: Arc<Mutex<Client>>,
}

impl GrpcRemoteDao {
    pub fn create(key: String, storage_path: PathBuf, runtime_handle: Handle, client: Client) -> Result<Self> {
        let path = key.clone();
        let client = Arc::new(Mutex::new(client));
        let client_copy = client.clone();
        let handle = runtime_handle.clone();
        let response = std::thread::spawn(move || {
            let mut client = client_copy.lock().map_err(|_| anyhow!("Mutex is poisoned!"))?;
            let client = client.deref_mut();
            let future = client.load(ParseLoadRequest { path });
            handle.block_on(future).map(|w| w.into_inner()).map_err(|status| anyhow!("Request failed: {:?}", status))
        }).join().unwrap()?;
        require!(response.file.map(|f| f.key).as_ref() == Some(&key),
                 "Remote load returned unexpected result");
        let name = format!("{} database", path_file_name(&storage_path)?);
        Ok(GrpcRemoteDao {
            name,
            key,
            storage_path,
            runtime_handle,
            cache: DaoCache::new(),
            client,
        })
    }

    fn wrap_request<ReqFn, Res>(&self, do_request: ReqFn) -> Result<Res>
        where ReqFn: for<'a> FnOnce(&mut Client) -> BoxFuture<'_, StdResult<tonic::Response<Res>, tonic::Status>>,
              ReqFn: Send + Sync + 'static,
              Res: Send + Sync + 'static
    {
        let client = self.client.clone();
        let handle = self.runtime_handle.clone();
        std::thread::spawn(move || {
            let mut client = client.lock().map_err(|_| anyhow!("Mutex is poisoned!"))?;
            let client = client.deref_mut();
            let future = do_request(client);
            handle.block_on(future).map(|w| w.into_inner()).map_err(|status| anyhow!("Request failed: {:?}", status))
        }).join().unwrap()
    }
}

impl WithCache for GrpcRemoteDao {
    fn get_cache_unchecked(&self) -> &DaoCache { &self.cache }

    fn init_cache(&self, inner: &mut DaoCacheInner) -> EmptyRes {
        let key = self.key.clone();
        inner.datasets = self.wrap_request(move |client| {
            client.datasets(DatasetsRequest { key: key.clone() })
        }.boxed())?.datasets;

        let ds_uuids = inner.datasets.iter().map(|ds| ds.uuid.clone().unwrap()).collect_vec();
        for ds_uuid in ds_uuids {
            let key = self.key.clone();
            let ds_uuid_instance = Some(ds_uuid.clone());
            // Can't do both requests in parallel since ChatHistoryDaoServiceClient requires mutable self reference.
            let users = self.wrap_request(move |client| {
                client.users(UsersRequest { key, ds_uuid: ds_uuid_instance })
            }.boxed())?.users;

            let key = self.key.clone();
            let ds_uuid_instance = Some(ds_uuid.clone());
            let myself = self.wrap_request(move |client| {
                client.myself(MyselfRequest { key, ds_uuid: ds_uuid_instance })
            }.boxed())?.myself.context("Myself was empty!")?;

            // Sanity check
            require!(myself == users[0], "Users first element wasn't myself!");

            inner.users.insert(ds_uuid, UserCacheForDataset {
                myself,
                user_by_id: users.into_iter().map(|u| (u.id(), u)).collect(),
            });
        }

        Ok(())
    }
}

impl ChatHistoryDao for GrpcRemoteDao {
    fn name(&self) -> &str { &self.name }

    fn storage_path(&self) -> &Path { &self.storage_path }

    fn dataset_root(&self, ds_uuid: &PbUuid) -> Result<DatasetRoot> {
        let key = self.key.clone();
        let ds_uuid = ds_uuid.clone();
        let res = self.wrap_request(move |client| {
            client.dataset_root(DatasetRootRequest { key, ds_uuid: Some(ds_uuid) })
        }.boxed())?;
        Ok(DatasetRoot(PathBuf::from(res.path)))
    }

    fn chats_inner(&self, ds_uuid: &PbUuid) -> Result<Vec<ChatWithDetails>> {
        let key = self.key.clone();
        let ds_uuid = ds_uuid.clone();
        let res = self.wrap_request(move |client| {
            client.chats(ChatsRequest { key, ds_uuid: Some(ds_uuid) })
        }.boxed())?;
        res.cwds.into_iter().map(|cwd_pb| cwd_pb.try_into()).try_collect()
    }

    fn scroll_messages(&self, chat: &Chat, offset: usize, limit: usize) -> Result<Vec<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.scroll_messages(ScrollMessagesRequest {
                key,
                chat,
                offset: offset as i64,
                limit: limit as i64,
            })
        }.boxed())?;
        Ok(res.messages)
    }

    fn last_messages(&self, chat: &Chat, limit: usize) -> Result<Vec<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.last_messages(LastMessagesRequest {
                key,
                chat,
                limit: limit as i64,
            })
        }.boxed())?;
        Ok(res.messages)
    }

    fn messages_before_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.messages_before(MessagesBeforeRequest {
                key,
                chat,
                message_internal_id: *msg_id,
                limit: limit as i64,
            })
        }.boxed())?;
        Ok(res.messages)
    }

    fn messages_after_impl(&self, chat: &Chat, msg_id: MessageInternalId, limit: usize) -> Result<Vec<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.messages_after(MessagesAfterRequest {
                key,
                chat,
                message_internal_id: *msg_id,
                limit: limit as i64,
            })
        }.boxed())?;
        Ok(res.messages)
    }

    fn messages_slice(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<Vec<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.messages_slice(MessagesSliceRequest {
                key,
                chat,
                message_internal_id_1: *msg1_id,
                message_internal_id_2: *msg2_id,
            })
        }.boxed())?;
        Ok(res.messages)
    }

    fn messages_slice_len(&self, chat: &Chat, msg1_id: MessageInternalId, msg2_id: MessageInternalId) -> Result<usize> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.messages_slice_len(MessagesSliceRequest {
                key,
                chat,
                message_internal_id_1: *msg1_id,
                message_internal_id_2: *msg2_id,
            })
        }.boxed())?;
        Ok(res.messages_count as usize)
    }

    fn messages_around_date(&self, _chat: &Chat, _date_ts: Timestamp, _limit: usize) -> Result<(Vec<Message>, Vec<Message>)> {
        todo!()
    }

    fn message_option(&self, chat: &Chat, source_id: MessageSourceId) -> Result<Option<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.message_option(MessageOptionRequest {
                key,
                chat,
                source_id: *source_id,
            })
        }.boxed())?;
        Ok(res.message)
    }

    fn message_option_by_internal_id(&self, chat: &Chat, internal_id: MessageInternalId) -> Result<Option<Message>> {
        let key = self.key.clone();
        let chat = Some(chat.clone());
        let res = self.wrap_request(move |client| {
            client.message_option_by_internal_id(MessageOptionByInternalIdRequest {
                key,
                chat,
                internal_id: *internal_id,
            })
        }.boxed())?;
        Ok(res.message)
    }
}
