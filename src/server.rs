use tonic::{Code, Request, Response, Status, transport::Server};
use unicode_segmentation::UnicodeSegmentation;

use crate::{EmptyRes, InMemoryDb, json};
use crate::proto::history::{ParseJsonFileRequest, ParseJsonFileResponse};
use crate::proto::history::json_loader_server::*;

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("greeter_descriptor");

macro_rules! truncate_to {
    ($str:expr, $maxlen:expr) => {$str.graphemes(true).take($maxlen).collect::<String>()};
}

#[derive(Default)]
pub struct JsonServer {
    db: Option<InMemoryDb>,
}

impl JsonServer {}

#[tonic::async_trait]
impl JsonLoader for JsonServer {
    async fn parse_json_file(
        &self,
        request: Request<ParseJsonFileRequest>,
    ) -> Result<Response<ParseJsonFileResponse>, Status> {
        println!(">>> Request:  {:?}", request.get_ref());
        let response =
            json::parse_file(request.get_ref().path.as_str())
                .map_err(|s| Status::new(Code::Internal, s))
                .map(|pr| ParseJsonFileResponse {
                    ds: Some(pr.dataset),
                    myself: Some(pr.myself),
                    users: pr.users,
                    cwm: pr.cwm,
                })
                .map(Response::new);
        println!("{}", truncate_to!(format!("<<< Response: {:?}", response), 200));
        response
    }
}

// https://betterprogramming.pub/building-a-grpc-server-with-rust-be2c52f0860e
#[tokio::main]
pub async fn start_server() -> EmptyRes {
    let addr = "[::1]:50051".parse().unwrap();
    let chm_server = JsonServer::default();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    println!("JsonServer server listening on {}", addr);

    Server::builder()
        .add_service(JsonLoaderServer::new(chm_server))
        .add_service(reflection_service)
        .serve(addr)
        .await
        .map_err(|e| format!("{:?}", e))?;
    Ok(())
}
