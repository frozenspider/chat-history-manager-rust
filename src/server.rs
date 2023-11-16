use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::sync::Arc;

use itertools::Itertools;
use tokio::runtime::Handle;
use tonic::{Code, Request, Response, Status, transport::Server};
use unicode_segmentation::UnicodeSegmentation;

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::loader::Loader;
use crate::protobuf::history::*;
use crate::protobuf::history::chat_history_dao_service_server::*;
use crate::protobuf::history::choose_myself_service_client::ChooseMyselfServiceClient;
use crate::protobuf::history::history_loader_service_server::*;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("grpc_reflection_descriptor");

macro_rules! truncate_to {
    ($str:expr, $maxlen:expr) => {$str.graphemes(true).take($maxlen).collect::<String>()};
}

type StdRes<T, E> = std::result::Result<T, E>;
type StatusResult<T> = StdRes<T, Status>;
type TonicResult<T> = StatusResult<Response<T>>;

type DaoKey = String;
// Abosulte path to data source
type DaoMap = HashMap<DaoKey, RefCell<Box<dyn ChatHistoryDao>>>;

pub struct ChatHistoryManagerServer<MC: MyselfChooser> {
    loader: Arc<Loader<MC>>,
    loaded_daos: Arc<Mutex<DaoMap>>,
}

impl<MC: MyselfChooser> ChatHistoryManagerServer<MC> {
    fn process_request<Q, P, L>(&self, req: &Request<Q>, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q) -> Result<P> {
        log::info!(">>> Request:  {:?}", req.get_ref());
        let response_result = logic(req.get_ref())
            .map(Response::new);
        log::info!("{}", truncate_to!(format!("<<< Response: {:?}", response_result), 150));
        response_result.map_err(|err| {
            eprintln!("Request failed!\n{:?}", err);
            Status::new(Code::Internal, error_to_string(&err))
        })
    }

    fn lock_dao_map(&self) -> StatusResult<MutexGuard<DaoMap>> {
        self.loaded_daos.lock()
            .map_err(|e| Status::new(Code::Internal,
                                     format!("Cannot obtain a DAO mutex lock: {}", e)))
    }

    fn process_request_with_dao<Q, P, L>(&self, req: &Request<Q>, key: &DaoKey, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q, &mut dyn ChatHistoryDao) -> Result<P> {
        let dao_map_lock = self.lock_dao_map()?;
        let dao = dao_map_lock.get(key)
            .ok_or_else(|| Status::new(Code::FailedPrecondition,
                                       format!("Database {key} is not loaded!")))?;
        let mut dao = (*dao).borrow_mut();
        let dao = dao.deref_mut().as_mut();

        self.process_request(req, |req| logic(req, dao))
    }
}

#[tonic::async_trait]
impl<MC: MyselfChooser + 'static> HistoryLoaderService for Arc<ChatHistoryManagerServer<MC>> {
    async fn parse_return_full(&self, req: Request<ParseRequest>) -> TonicResult<ParseReturnFullResponse> {
        let loader = self.loader.clone();
        self.process_request(&req, move |req| {
            let path = Path::new(&req.path);
            let dao = loader.load(path)?;
            Ok(ParseReturnFullResponse {
                ds: Some(dao.dataset),
                root_file: String::from(dao.ds_root.to_str().unwrap()),
                myself: Some(dao.myself),
                users: dao.users,
                cwms: dao.cwms,
            })
        })
    }

    async fn parse_return_handle(&self, req: Request<ParseRequest>) -> TonicResult<ParseReturnHandleResponse> {
        let loader = self.loader.clone();
        let dao_map = self.loaded_daos.clone();
        self.process_request(&req, move |req| {
            let mut lock = dao_map.lock().unwrap();
            let map = lock.deref_mut();
            let path = fs::canonicalize(&req.path)?;
            let path_string = path_to_str(&path)?.to_owned();

            if let Some(dao) = map.get(&path_string) {
                let dao = dao.borrow();
                return Ok(ParseReturnHandleResponse {
                    file: Some(LoadedFile { key: path_string, name: dao.name().to_owned() })
                });
            }

            let dao = loader.load(&path)?;
            let response = ParseReturnHandleResponse {
                file: Some(LoadedFile { key: path_string.clone(), name: dao.name().to_owned() })
            };
            map.insert(path_string, RefCell::new(dao));
            Ok(response)
        })
    }
}

macro_rules! with_dao_by_key {
    ($self:ident, $req:ident, $dao:ident, $code:block) => {
        $self.process_request_with_dao(&$req, &$req.get_ref().key, |#[allow(unused)] $req, $dao| { $code })
    };
}

macro_rules! uuid_from_req { ($req:ident) => { $req.ds_uuid.as_ref().context("Request has no ds_uuid")? }; }
macro_rules! chat_from_req { ($req:ident) => { $req.chat   .as_ref().context("Request has no chat")? }; }
macro_rules! msg_from_req { ($req:ident.$msg:ident) => { $req.$msg.as_ref().context("Request has no message")? }; }

#[tonic::async_trait]
impl<MC: MyselfChooser + 'static> ChatHistoryDaoService for Arc<ChatHistoryManagerServer<MC>> {
    async fn get_loaded_files(&self, req: Request<GetLoadedFilesRequest>) -> TonicResult<GetLoadedFilesResponse> {
        self.process_request(&req, |_| {
            let dao_map_lock = self.lock_dao_map()?;
            let files = dao_map_lock.iter()
                .map(|(k, dao)| LoadedFile { key: k.clone(), name: dao.borrow().name().to_owned() })
                .collect_vec();
            Ok(GetLoadedFilesResponse { files })
        })
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
                path: dao.dataset_root(uuid_from_req!(req)).0.to_str().unwrap().to_owned()
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
                    .map(|cwd| ChatWithDetailsPb {
                        chat: Some(cwd.chat),
                        last_msg_option: cwd.last_msg_option,
                        members: cwd.members,
                    })
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
                messages: dao.messages_before(chat_from_req!(req), msg_from_req!(req.message), req.limit as usize)?
            })
        })
    }

    async fn messages_after(&self, req: Request<MessagesAfterRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_after(chat_from_req!(req), msg_from_req!(req.message), req.limit as usize)?
            })
        })
    }

    async fn messages_between(&self, req: Request<MessagesBetweenRequest>) -> TonicResult<MessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(MessagesResponse {
                messages: dao.messages_between(
                    chat_from_req!(req), msg_from_req!(req.message1), msg_from_req!(req.message2))?
            })
        })
    }

    async fn count_messages_between(&self, req: Request<MessagesBetweenRequest>) -> TonicResult<CountMessagesResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(CountMessagesResponse {
                messages_count: dao.count_messages_between(
                    chat_from_req!(req), msg_from_req!(req.message1), msg_from_req!(req.message2))? as i32
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

    async fn is_loaded(&self, req: Request<IsLoadedRequest>) -> TonicResult<IsLoadedResponse> {
        with_dao_by_key!(self, req, dao, {
            Ok(IsLoadedResponse {
                is_loaded: dao.is_loaded(&Path::new(&req.storage_path))
            })
        })
    }
}

async fn choose_myself_async(port: u16, users: Vec<User>) -> Result<usize> {
    log::info!("Connecting to myself chooser at port {}", port);
    let mut client =
        ChooseMyselfServiceClient::connect(format!("http://127.0.0.1:{}", port))
            .await?;
    log::info!("Sending ChooseMyselfRequest");
    let len = users.len();
    let request = ChooseMyselfRequest { users };
    let response = client.choose_myself(request).await
        .map_err(|status| anyhow!("{}", status.message()))?;
    log::info!("Got response");
    let response = response.get_ref().picked_option;
    if response < 0 {
        err!("Choice aborted!")
    } else if response as usize >= len {
        err!("Choice out of range!")
    } else {
        Ok(response as usize)
    }
}

struct ChooseMyselfImpl {
    runtime_handle: Arc<Handle>,
    myself_chooser_port: u16,
}

impl MyselfChooser for ChooseMyselfImpl {
    fn choose_myself(&self, users: &[&User]) -> Result<usize> {
        let async_chooser =
            choose_myself_async(self.myself_chooser_port,
                                users.iter().map(|&u| u.clone()).collect_vec());
        let handle = self.runtime_handle.clone();

        // We cannot use the current thread since when called via RPC, current thread is already used for async tasks.
        // We're unwrapping join() to propagate panic.
        std::thread::spawn(move || {
            let spawned = handle.spawn(async_chooser);
            handle.block_on(spawned)
        }).join().unwrap()?
    }
}

// https://betterprogramming.pub/building-a-grpc-server-with-rust-be2c52f0860e
#[tokio::main]
pub async fn start_server<H: HttpClient>(port: u16, http_client: &'static H) -> EmptyRes {
    let addr = format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap();

    let myself_chooser_port = port + 1;
    let runtime_handle = Arc::new(Handle::current());
    let myself_chooser = ChooseMyselfImpl {
        runtime_handle,
        myself_chooser_port,
    };
    let loader = Arc::new(Loader::new(http_client, myself_chooser));

    let chm_server = Arc::new(ChatHistoryManagerServer {
        loaded_daos: Arc::new(Mutex::new(HashMap::new())),
        loader,
    });

    log::info!("Server listening on {}", addr);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(HistoryLoaderServiceServer::new(chm_server.clone()))
        .add_service(ChatHistoryDaoServiceServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
pub async fn debug_request_myself(port: u16) -> Result<usize> {
    let chooser = ChooseMyselfImpl {
        runtime_handle: Arc::new(Handle::current()),
        myself_chooser_port: port,
    };

    let ds_uuid = PbUuid { value: "00000000-0000-0000-0000-000000000000".to_owned() };
    let chosen = chooser.choose_myself(&[
        &User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 100,
            first_name_option: Some("User 100 FN".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        },
        &User {
            ds_uuid: Some(ds_uuid),
            id: 200,
            first_name_option: None,
            last_name_option: Some("User 200 LN".to_owned()),
            username_option: None,
            phone_number_option: None,
        },
    ])?;
    Ok(chosen)
}
