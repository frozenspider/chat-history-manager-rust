use std::sync::{Arc, Mutex};

use tonic::Request;

use crate::*;
use crate::protobuf::history::history_parser_service_server::*;

use super::*;

#[tonic::async_trait]
impl HistoryParserService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn parse(&self, req: Request<ParseLoadRequest>) -> TonicResult<ParseResponse> {
        self.process_request(&req, move |req, self_lock| {
            let path = Path::new(&req.path);
            let dao = self_lock.loader.parse(path)?;
            Ok(ParseResponse {
                ds: Some(dao.in_mem_dataset()),
                root_file: String::from(dao.ds_root.to_str().unwrap()),
                myself: Some(dao.in_mem_myself()),
                users: (dao.in_mem_users()),
                cwms: dao.cwms,
            })
        })
    }
}
