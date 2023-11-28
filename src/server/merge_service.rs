use std::sync::{Arc, Mutex};

use tonic::Request;

use crate::*;
use crate::merge::analyzer::{DatasetDiffAnalyzer, MergeAnalysisSection};
use crate::protobuf::history::merge_service_server::*;

use super::*;

#[tonic::async_trait]
impl MergeService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn analyze(&self, req: Request<AnalyzeRequest>) -> TonicResult<AnalyzeResponse> {
        self.process_request(&req, move |req, self_lock| {
            let m_dao = self_lock.loaded_daos.get(&req.master_dao_key).context("Master DAO not found")?;
            let s_dao = self_lock.loaded_daos.get(&req.slave_dao_key).context("Slave DAO not found")?;

            let m_dao = (*m_dao).borrow();
            let s_dao = (*s_dao).borrow();

            let m_ds_uuid = from_req!(req.master_ds_uuid);
            let s_ds_uuid = from_req!(req.slave_ds_uuid);

            let m_ds = m_dao.datasets()?.into_iter().find(|ds| ds.uuid() == m_ds_uuid)
                .context("Master dataset not found!")?;
            let s_ds = s_dao.datasets()?.into_iter().find(|ds| ds.uuid() == s_ds_uuid)
                .context("Slave dataset not found!")?;

            let analyzer = DatasetDiffAnalyzer::create(m_dao.as_ref(), &m_ds, s_dao.as_ref(), &s_ds)?;
            let mut analysis = Vec::with_capacity(req.chat_ids.len());
            for chat_id in req.chat_ids.iter() {
                let chat_id = *chat_id;
                let m_cwd = m_dao.chat_option(m_ds_uuid, chat_id)?
                    .with_context(|| format!("Source chat {} not found!", chat_id))?;
                let s_cwd = s_dao.chat_option(s_ds_uuid, chat_id)?
                    .with_context(|| format!("Source chat {} not found!", chat_id))?;
                let analyzed =
                    analyzer.analyze(&m_cwd, &s_cwd,
                                     &name_or_unnamed(&s_cwd.chat.name_option))?;
                let sections = analyzed.into_iter().map(|a| {
                    let mut res = AnalysisSection {
                        tpe: 0,
                        first_master_msg_id: *NO_INTERNAL_ID,
                        last_master_msg_id: *NO_INTERNAL_ID,
                        first_slave_msg_id: *NO_INTERNAL_ID,
                        last_slave_msg_id: *NO_INTERNAL_ID,
                    };
                    macro_rules! set { ($from:ident.$k:ident) => { res.$k = *$from.$k }; }
                    match a {
                        MergeAnalysisSection::Match(v) => {
                            res.tpe = AnalysisSectionType::Match as i32;
                            set!(v.first_master_msg_id);
                            set!(v.last_master_msg_id);
                            set!(v.first_slave_msg_id);
                            set!(v.last_slave_msg_id);
                        }
                        MergeAnalysisSection::Retention(v) => {
                            res.tpe = AnalysisSectionType::Retention as i32;
                            set!(v.first_master_msg_id);
                            set!(v.last_master_msg_id);
                        }
                        MergeAnalysisSection::Addition(v) => {
                            res.tpe = AnalysisSectionType::Addition as i32;
                            set!(v.first_slave_msg_id);
                            set!(v.last_slave_msg_id);
                        }
                        MergeAnalysisSection::Conflict(v) => {
                            res.tpe = AnalysisSectionType::Conflict as i32;
                            set!(v.first_master_msg_id);
                            set!(v.last_master_msg_id);
                            set!(v.first_slave_msg_id);
                            set!(v.last_slave_msg_id);
                        }
                    };
                    res
                }).collect_vec();
                analysis.push(ChatAnalysis { chat_id, sections })
            }
            Ok(AnalyzeResponse { analysis })
        })
    }
}
