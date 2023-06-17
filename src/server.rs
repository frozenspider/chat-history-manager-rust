use std::net::SocketAddr;

use itertools::Itertools;
use tokio::runtime::Handle;
use tonic::{Code, Request, Response, Status, transport::Server};
use unicode_segmentation::UnicodeSegmentation;

use crate::*;
use crate::protobuf::history::{ChooseMyselfRequest, ParseHistoryFileRequest, ParseHistoryFileResponse, PbUuid};
use crate::protobuf::history::history_loader_server::*;
use crate::protobuf::history::myself_chooser_client::MyselfChooserClient;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("greeter_descriptor");

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
    ) -> Result<Response<ParseHistoryFileResponse>, Status> {
        println!(">>> Request:  {:?}", request.get_ref());
        let myself_chooser_port = self.myself_chooser_port;

        let blocking_task = tokio::task::spawn_blocking(move || {
            let myself_chooser = ChooseMyselfImpl { myself_chooser_port };
            let response =
                json::parse_file(request.get_ref().path.as_str(), &myself_chooser)
                    .map_err(|s| Status::new(Code::Internal, s))
                    .map(|pr| ParseHistoryFileResponse {
                        ds: Some(pr.dataset),
                        root_file: String::from(pr.ds_root.to_str().unwrap()),
                        myself: Some(pr.myself),
                        users: pr.users,
                        cwm: pr.cwm,
                    })
                    .map(Response::new);
            println!("{}", truncate_to!(format!("<<< Response: {:?}", response), 200));
            response
        });

        let response =
            blocking_task.await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        response
    }
}

async fn choose_myself_async(port: u16, users: Vec<User>) -> Res<usize> {
    println!("Connecting to myself chooser at port {}", port);
    let mut client =
        MyselfChooserClient::connect(format!("http://127.0.0.1:{}", port))
            .await
            .map_err(error_to_string)?;
    println!("Sending ChooseMyselfRequest");
    let len = users.len();
    let request = ChooseMyselfRequest { users };
    let response = client.choose_myself(request).await
        .map_err(error_to_string)?;
    println!("Got response");
    let response = response.get_ref().picked_option;
    if response < 0 {
        Err("Choice aborted!".to_owned())
    } else if response as usize >= len {
        Err("Choice out of range!".to_owned())
    } else {
        Ok(response as usize)
    }
}

struct ChooseMyselfImpl {
    myself_chooser_port: u16,
}

impl ChooseMyselfTrait for ChooseMyselfImpl {
    fn choose_myself(&self, users: &Vec<&User>) -> Res<usize> {
        // let mut pool = LocalPool::new();
        // let spawner = pool.spawner();

        let async_chooser =
            choose_myself_async(self.myself_chooser_port,
                                users.iter().map(|&u| u.clone()).collect_vec());
        // let handle =
        //     spawner.spawn_local_with_handle(async_chooser).map_err(error_to_string)?;
        // Ok(pool.run_until(handle)?)
        let handle = Handle::current();
        let spawned = handle.spawn(async_chooser);

        Ok(handle.block_on(spawned).map_err(error_to_string)??)
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

    println!("JsonServer server listening on {}", addr);

    Server::builder()
        .add_service(HistoryLoaderServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await
        .map_err(|e| format!("{:?}", e))?;
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
pub async fn make_choose_myself_request(port: u16) -> EmptyRes {
    let chooser = ChooseMyselfImpl {
        myself_chooser_port: port,
    };

    let ds_uuid = PbUuid { value: "00000000-0000-0000-0000-000000000000".to_owned() };
    let chosen = chooser.choose_myself(&vec![
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
    println!("Picked: {}", chosen);
    Ok(())
}
