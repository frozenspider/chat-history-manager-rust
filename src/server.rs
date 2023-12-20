use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::{Mutex, MutexGuard};
use std::sync::Arc;

use tokio::runtime::Handle;
use tonic::{Code, Request, Response, Status, transport::Server};
use tonic::transport::Endpoint;

use myself_chooser::MyselfChooserImpl;

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::dao::sqlite_dao::SqliteDao;
use crate::loader::Loader;
use crate::protobuf::history::*;
use crate::protobuf::history::choose_myself_service_client::ChooseMyselfServiceClient;
use crate::protobuf::history::history_dao_service_server::HistoryDaoServiceServer;
use crate::protobuf::history::history_loader_service_server::HistoryLoaderServiceServer;
use crate::protobuf::history::merge_service_server::MergeServiceServer;

mod myself_chooser;
mod history_loader_service;
mod history_dao_service;
mod merge_service;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("grpc_reflection_descriptor");

type StatusResult<T> = StdResult<T, Status>;
type TonicResult<T> = StatusResult<Response<T>>;

// Abosulte path to data source
type DaoKey = String;
type DaoRefCell = RefCell<Box<dyn ChatHistoryDao>>;

type ChmLock<'a> = MutexGuard<'a, ChatHistoryManagerServer>;

#[macro_export]
macro_rules! from_req {
    ($req:ident.$field:ident) => {
        $req.$field.as_ref().context(concat!("Request has no ", stringify!($field)))?
    };
}

// Should be used wrapped in Arc<Mutex<Self>>
pub struct ChatHistoryManagerServer {
    loader: Loader,
    loaded_daos: HashMap<DaoKey, DaoRefCell>,
}

trait ChatHistoryManagerServerTrait {
    fn process_request<Q, P, L>(&self, req: &Request<Q>, logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q, &mut ChmLock<'_>) -> Result<P>;

    fn process_request_with_dao<Q, P, L>(&self, req: &Request<Q>, key: &DaoKey, logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q, &mut dyn ChatHistoryDao) -> Result<P>;

    fn process_request_inner<Q, P, L>(&self, req: &Request<Q>, logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q) -> Result<P>;
}

impl ChatHistoryManagerServerTrait for Arc<Mutex<ChatHistoryManagerServer>> {
    fn process_request<Q, P, L>(&self, req: &Request<Q>, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q, &mut ChmLock<'_>) -> Result<P> {
        let mut self_lock = lock_or_status(self)?;
        self.process_request_inner(req, |req| logic(req, &mut self_lock))
    }

    fn process_request_with_dao<Q, P, L>(&self, req: &Request<Q>, key: &DaoKey, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q, &mut dyn ChatHistoryDao) -> Result<P> {
        let self_lock = lock_or_status(self)?;
        let dao = self_lock.loaded_daos.get(key)
            .ok_or_else(|| Status::new(Code::FailedPrecondition,
                                       format!("Database with key {key} is not loaded!")))?;
        let mut dao = (*dao).borrow_mut();
        let dao = dao.deref_mut().as_mut();

        self.process_request_inner(req, |req| logic(req, dao))
    }

    fn process_request_inner<Q, P, L>(&self, req: &Request<Q>, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q) -> Result<P> {
        log::debug!(">>> Request:  {}", truncate_to(format!("{:?}", req.get_ref()), 150));
        let response_result = logic(req.get_ref())
            .map(Response::new);
        log::debug!("<<< Response: {}", truncate_to(format!("{:?}", response_result), 150));
        response_result.map_err(|err| {
            eprintln!("Request failed! Error was:\n{:?}", err);
            Status::new(Code::Internal, error_to_string(&err))
        })
    }
}

// https://betterprogramming.pub/building-a-grpc-server-with-rust-be2c52f0860e
#[tokio::main]
pub async fn start_server<H: HttpClient>(port: u16, http_client: &'static H) -> EmptyRes {
    let addr = format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap();

    let remote_port = port + 1;
    let runtime_handle = Handle::current();
    let lazy_channel = Endpoint::new(format!("http://127.0.0.1:{remote_port}"))?.connect_lazy();
    let myself_chooser = Box::new(MyselfChooserImpl { runtime_handle, channel: lazy_channel });
    let loader = Loader::new(http_client, myself_chooser);

    let chm_server = Arc::new(Mutex::new(ChatHistoryManagerServer {
        loader,
        loaded_daos: HashMap::new(),
    }));

    log::info!("Server listening on {}", addr);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(HistoryLoaderServiceServer::new(chm_server.clone()))
        .add_service(HistoryDaoServiceServer::new(chm_server.clone()))
        .add_service(MergeServiceServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
pub async fn debug_request_myself(port: u16) -> Result<usize> {
    let conn_port = port + 1;
    let runtime_handle = Handle::current();
    let lazy_channel = Endpoint::new(format!("http://127.0.0.1:{conn_port}"))?.connect_lazy();
    let chooser = MyselfChooserImpl {
        runtime_handle,
        channel: lazy_channel,
    };

    let ds_uuid = PbUuid { value: "00000000-0000-0000-0000-000000000000".to_owned() };
    let chosen = chooser.choose_myself(&[
        User {
            ds_uuid: Some(ds_uuid.clone()),
            id: 100,
            first_name_option: Some("User 100 FN".to_owned()),
            last_name_option: None,
            username_option: None,
            phone_number_option: None,
        },
        User {
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

fn lock_or_status<T>(target: &Arc<Mutex<T>>) -> StatusResult<MutexGuard<'_, T>> {
    target.lock().map_err(|_| Status::new(Code::Internal, "Mutex is poisoned!"))
}
