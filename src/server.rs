use std::net::SocketAddr;
use std::path::Path;

use itertools::Itertools;
use tokio::runtime::Handle;
use tonic::{Code, Request, Response, Status, transport::Server};
use unicode_segmentation::UnicodeSegmentation;

use crate::*;
use crate::protobuf::history::{ChooseMyselfRequest, ParseHistoryFileRequest, ParseHistoryFileResponse, PbUuid, User};
use crate::protobuf::history::history_loader_server::*;
use crate::protobuf::history::myself_chooser_client::MyselfChooserClient;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("grpc_reflection_descriptor");

macro_rules! truncate_to {
    ($str:expr, $maxlen:expr) => {$str.graphemes(true).take($maxlen).collect::<String>()};
}

pub struct ChatHistoryManagerServer {
    myself_chooser_port: u16,
//   db: Option<InMemoryDb>,
}

#[tonic::async_trait]
impl HistoryLoader for ChatHistoryManagerServer {
    async fn parse_history_file(
        &self,
        request: Request<ParseHistoryFileRequest>,
    ) -> std::result::Result<Response<ParseHistoryFileResponse>, Status> {
        log::info!(">>> Request:  {:?}", request.get_ref());
        let myself_chooser_port = self.myself_chooser_port;

        let blocking_task = tokio::task::spawn_blocking(move || {
            let myself_chooser = ChooseMyselfImpl { myself_chooser_port };
            let path = Path::new(&request.get_ref().path);
            let response =
                loader::load(path, &myself_chooser)
                    .map_err(|err| Status::new(Code::Internal, error_to_string(&err)))
                    .map(|dao|
                        ParseHistoryFileResponse {
                            ds: Some(dao.dataset),
                            root_file: String::from(dao.ds_root.to_str().unwrap()),
                            myself: Some(dao.myself),
                            users: dao.users,
                            cwms: dao.cwms,
                        }
                    )
                    .map(Response::new);
            log::info!("{}", truncate_to!(format!("<<< Response: {:?}", response), 200));
            response
        });

        let response =
            blocking_task.await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        response
    }
}

async fn choose_myself_async(port: u16, users: Vec<User>) -> Result<usize> {
    log::info!("Connecting to myself chooser at port {}", port);
    let mut client =
        MyselfChooserClient::connect(format!("http://127.0.0.1:{}", port))
            .await?;
    log::info!("Sending ChooseMyselfRequest");
    let len = users.len();
    let request = ChooseMyselfRequest { users };
    let response = client.choose_myself(request).await
        .map_err(|status| Error::from(status.message()))?;
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

        Ok(handle.block_on(spawned)??)
    }
}

// https://betterprogramming.pub/building-a-grpc-server-with-rust-be2c52f0860e
#[tokio::main]
pub async fn start_server(port: u16) -> EmptyRes {
    let addr = format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap();
    let chm_server = ChatHistoryManagerServer {
        myself_chooser_port: port + 1,
    };

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    log::info!("JsonServer server listening on {}", addr);

    Server::builder()
        .add_service(HistoryLoaderServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await
        .map_err(|e| format!("{:?}", e))?;
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
            ds_uuid: Some(ds_uuid.clone()),
            id: 200,
            first_name_option: None,
            last_name_option: Some("User 200 LN".to_owned()),
            username_option: None,
            phone_number_option: None,
        },
    ])?;
    Ok(chosen)
}
