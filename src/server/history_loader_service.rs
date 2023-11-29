use std::sync::{Arc, Mutex};

use tonic::Request;

use crate::*;
use crate::protobuf::history::history_parser_service_server::*;
use crate::protobuf::history::history_loader_service_server::*;

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

#[tonic::async_trait]
impl HistoryLoaderService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn load(&self, req: Request<ParseLoadRequest>) -> TonicResult<LoadResponse> {
        self.process_request(&req, move |req, self_lock| {
            let path = fs::canonicalize(&req.path)?;
            let path_string = path_to_str(&path)?.to_owned();

            if let Some(dao) = self_lock.loaded_daos.get(&path_string) {
                let dao = dao.borrow();
                return Ok(LoadResponse {
                    file: Some(LoadedFile { key: path_string, name: dao.name().to_owned() })
                });
            }

            let dao = self_lock.loader.load(&path)?;
            let response = LoadResponse {
                file: Some(LoadedFile { key: path_string.clone(), name: dao.name().to_owned() })
            };
            self_lock.loaded_daos.insert(path_string, RefCell::new(dao));
            Ok(response)
        })
    }

    async fn get_loaded_files(&self, req: Request<GetLoadedFilesRequest>) -> TonicResult<GetLoadedFilesResponse> {
        self.process_request(&req, |_, self_lock| {
            let files = self_lock.loaded_daos.iter()
                .map(|(k, dao)| LoadedFile { key: k.clone(), name: dao.borrow().name().to_owned() })
                .collect_vec();
            Ok(GetLoadedFilesResponse { files })
        })
    }
}
