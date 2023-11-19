use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;
use tokio::runtime::Handle;
use tonic::{Code, Request, Response, Status, transport::Server};
use unicode_segmentation::UnicodeSegmentation;

use crate::*;
use crate::loader::Loader;
use crate::protobuf::history::*;
use crate::protobuf::history::history_loader_service_server::*;
use crate::protobuf::history::choose_myself_service_client::ChooseMyselfServiceClient;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("grpc_reflection_descriptor");

macro_rules! truncate_to {
    ($str:expr, $maxlen:expr) => {$str.graphemes(true).take($maxlen).collect::<String>()};
}

type StdRes<T, E> = std::result::Result<T, E>;
type StatusResult<T> = StdRes<T, Status>;
type TonicResult<T> = StatusResult<Response<T>>;

pub struct ChatHistoryManagerServer<MC: MyselfChooser> {
    loader: Arc<Loader<MC>>,
}

impl<MC: MyselfChooser> ChatHistoryManagerServer<MC> {
    fn process_request<Q, P, L>(&self, req: &Request<Q>, mut logic: L) -> TonicResult<P>
        where Q: Debug,
              P: Debug,
              L: FnMut(&Q) -> StatusResult<P> {
        log::info!(">>> Request:  {:?}", req.get_ref());
        let response_result = logic(req.get_ref())
            .map(Response::new);
        log::info!("{}", truncate_to!(format!("<<< Response: {:?}", response_result), 150));
        response_result
    }
}

#[tonic::async_trait]
impl<MC: MyselfChooser + 'static> HistoryLoaderService for ChatHistoryManagerServer<MC> {
    async fn parse_history_file(&self, req: Request<ParseHistoryFileRequest>) -> TonicResult<ParseHistoryFileResponse> {
        let loader = self.loader.clone();
        self.process_request(&req, move |req| {
            let path = Path::new(&req.path);
            loader.load(path)
                .map_err(|err| {
                    eprintln!("Load failed!\n{:?}", err);
                    Status::new(Code::Internal, error_to_string(&err))
                })
                .map(|in_mem_dao|
                    ParseHistoryFileResponse {
                        ds: Some(in_mem_dao.dataset),
                        root_file: String::from(in_mem_dao.ds_root.to_str().unwrap()),
                        myself: Some(in_mem_dao.myself),
                        users: in_mem_dao.users,
                        cwms: in_mem_dao.cwms,
                    }
                )
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
    myself_chooser_port: u16,
}

impl MyselfChooser for ChooseMyselfImpl {
    fn choose_myself(&self, users: &[&User]) -> Result<usize> {
        // let mut pool = LocalPool::new();
        // let spawner = pool.spawner();

        let async_chooser =
            choose_myself_async(self.myself_chooser_port,
                                users.iter().map(|&u| u.clone()).collect_vec());
        // let handle =
        //     spawner.spawn_local_with_handle(async_chooser).map_err(error_to_string)?;
        // Ok(pool.run_until(handle)?)
        let handle = Handle::current();
        // let spawned = handle.spawn_blocking(|| {
        //     choose_myself_async(self.myself_chooser_port,
        //                         users.iter().map(|&u| u.clone()).collect_vec())
        // });

        let spawned = handle.spawn(async_chooser);

        handle.block_on(spawned)?
    }
}

// https://betterprogramming.pub/building-a-grpc-server-with-rust-be2c52f0860e
#[tokio::main]
pub async fn start_server<H: HttpClient>(port: u16, http_client: &'static H) -> EmptyRes {
    let addr = format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap();

    let myself_chooser_port = port + 1;
    let myself_chooser = ChooseMyselfImpl { myself_chooser_port };
    let loader = Arc::new(Loader::new(http_client, myself_chooser));

    let chm_server = ChatHistoryManagerServer {
        loader,
    };

    log::info!("Server listening on {}", addr);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(HistoryLoaderServiceServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
pub async fn debug_request_myself(port: u16) -> Result<usize> {
    let chooser = ChooseMyselfImpl {
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
