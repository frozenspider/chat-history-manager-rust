use std::cell::Ref;
use std::sync::{Arc, Mutex};
use itertools::Itertools;

use tonic::Request;

use crate::merge::analyzer::*;
use crate::merge::merger;
use crate::merge::merger::{ChatMergeDecision, MessagesMergeDecision, UserMergeDecision};
use crate::protobuf::history::merge_service_server::*;

use super::*;

#[tonic::async_trait]
impl MergeService for Arc<Mutex<ChatHistoryManagerServer>> {
    async fn analyze(&self, req: Request<AnalyzeRequest>) -> TonicResult<AnalyzeResponse> {
        self.process_merge_service_request(&req, |req, m_dao, m_ds, s_dao, s_ds| {
            let analyzer = DatasetDiffAnalyzer::create(m_dao.as_ref(), &m_ds, s_dao.as_ref(), &s_ds)?;
            let mut analysis = Vec::with_capacity(req.chat_id_pairs.len());
            for pair @ ChatIdPair { master_chat_id, slave_chat_id } in req.chat_id_pairs.iter() {
                let m_cwd = m_dao.chat_option(&m_ds.uuid, *master_chat_id)?
                    .with_context(|| format!("Master chat {} not found!", *master_chat_id))?;
                let s_cwd = s_dao.chat_option(&s_ds.uuid, *slave_chat_id)?
                    .with_context(|| format!("Slave chat {} not found!", *slave_chat_id))?;
                let analyzed =
                    analyzer.analyze(&m_cwd, &s_cwd, &s_cwd.chat.qualified_name(), req.force_conflicts)?;
                let sections = analyzed.into_iter().map(|a| {
                    let mut res = AnalysisSection {
                        tpe: 0,
                        range: MessageMergeSectionRange {
                            first_master_msg_id: *NO_INTERNAL_ID,
                            last_master_msg_id: *NO_INTERNAL_ID,
                            first_slave_msg_id: *NO_INTERNAL_ID,
                            last_slave_msg_id: *NO_INTERNAL_ID,
                        },
                    };
                    macro_rules! set { ($from:ident.$k:ident) => { res.range.$k = *$from.$k }; }
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
                analysis.push(ChatAnalysis { chat_ids: pair.clone(), sections })
            }
            Ok(analysis)
        }, |analysis, _self_lock| Ok(AnalyzeResponse { analysis }))
    }

    async fn merge(&self, req: Request<MergeRequest>) -> TonicResult<MergeResponse> {
        self.process_merge_service_request(&req, |req, m_dao, m_ds, s_dao, s_ds| {
            let sqlite_dao_dir = Path::new(&req.new_database_dir);
            let user_merges = req.user_merges.iter().map(|um|
                ok(match UserMergeType::try_from(um.tpe)? {
                    UserMergeType::Retain => UserMergeDecision::Retain(UserId(um.user_id)),
                    UserMergeType::Add => UserMergeDecision::Add(UserId(um.user_id)),
                    UserMergeType::DontAdd => UserMergeDecision::DontAdd(UserId(um.user_id)),
                    UserMergeType::Replace => UserMergeDecision::Replace(UserId(um.user_id)),
                    UserMergeType::MatchOrDontReplace => UserMergeDecision::MatchOrDontReplace(UserId(um.user_id)),
                })
            ).try_collect()?;
            let chat_merges = req.chat_merges.iter().map(|cm|
                ok(match ChatMergeType::try_from(cm.tpe)? {
                    ChatMergeType::Retain => ChatMergeDecision::Retain { master_chat_id: ChatId(cm.chat_id) },
                    ChatMergeType::DontMerge => ChatMergeDecision::DontMerge { chat_id: ChatId(cm.chat_id) },
                    ChatMergeType::Add => ChatMergeDecision::Add { slave_chat_id: ChatId(cm.chat_id) },
                    ChatMergeType::DontAdd => ChatMergeDecision::DontAdd { slave_chat_id: ChatId(cm.chat_id) },
                    ChatMergeType::Merge => {
                        use MessageMergeType as MMT;
                        use MessagesMergeDecision as MMD;
                        let message_merges = cm.message_merges.iter().map(|mm| {
                            let range = &mm.range;
                            ok(match MessageMergeType::try_from(mm.tpe)? {
                                MMT::Match => MMD::Match(MergeAnalysisSectionMatch {
                                    first_master_msg_id: MasterInternalId(range.first_master_msg_id),
                                    last_master_msg_id: MasterInternalId(range.last_master_msg_id),
                                    first_slave_msg_id: SlaveInternalId(range.first_slave_msg_id),
                                    last_slave_msg_id: SlaveInternalId(range.last_slave_msg_id),
                                }),
                                MMT::Retain => MMD::Retain(MergeAnalysisSectionRetention {
                                    first_master_msg_id: MasterInternalId(range.first_master_msg_id),
                                    last_master_msg_id: MasterInternalId(range.last_master_msg_id),
                                }),
                                MMT::Add => MMD::Add(MergeAnalysisSectionAddition {
                                    first_slave_msg_id: SlaveInternalId(range.first_slave_msg_id),
                                    last_slave_msg_id: SlaveInternalId(range.last_slave_msg_id),
                                }),
                                MMT::DontAdd => MMD::DontAdd(MergeAnalysisSectionAddition {
                                    first_slave_msg_id: SlaveInternalId(range.first_slave_msg_id),
                                    last_slave_msg_id: SlaveInternalId(range.last_slave_msg_id),
                                }),
                                MMT::Replace => MMD::Replace(MergeAnalysisSectionConflict {
                                    first_master_msg_id: MasterInternalId(range.first_master_msg_id),
                                    last_master_msg_id: MasterInternalId(range.last_master_msg_id),
                                    first_slave_msg_id: SlaveInternalId(range.first_slave_msg_id),
                                    last_slave_msg_id: SlaveInternalId(range.last_slave_msg_id),
                                }),
                                MMT::DontReplace => MMD::DontReplace(MergeAnalysisSectionConflict {
                                    first_master_msg_id: MasterInternalId(range.first_master_msg_id),
                                    last_master_msg_id: MasterInternalId(range.last_master_msg_id),
                                    first_slave_msg_id: SlaveInternalId(range.first_slave_msg_id),
                                    last_slave_msg_id: SlaveInternalId(range.last_slave_msg_id),
                                }),
                            })
                        }).try_collect()?;
                        ChatMergeDecision::Merge { chat_id: ChatId(cm.chat_id), message_merges }
                    }
                })
            ).try_collect()?;
            let (dao, ds) = merger::merge_datasets(sqlite_dao_dir,
                                                   &**m_dao, &m_ds,
                                                   &**s_dao, &s_ds,
                                                   user_merges, chat_merges)?;
            let key = path_to_str(&dao.db_file)?.to_owned();
            Ok((key, DaoRefCell::new(Box::new(dao)), ds))
        }, |(key, dao, ds): (DaoKey, DaoRefCell, Dataset), self_lock| {
            let name = dao.borrow().name().to_owned();
            self_lock.loaded_daos.insert(key.clone(), dao);
            Ok(MergeResponse {
                new_file: LoadedFile { key, name },
                new_ds_uuid: ds.uuid.clone(),
            })
        })
    }
}

trait MergeServiceHelper {
    fn process_merge_service_request<Q, R1, R2, Process, Finalize>(&self,
                                                                   req: &Request<Q>,
                                                                   process: Process,
                                                                   finalize: Finalize) -> TonicResult<R2>
        where Q: MergeServiceRequest + Debug,
              R2: Debug,
              Process: FnMut(
                  &Q,
                  Ref<Box<dyn ChatHistoryDao>>, Dataset,
                  Ref<Box<dyn ChatHistoryDao>>, Dataset,
              ) -> Result<R1>,
              Finalize: FnMut(R1, &mut ChmLock<'_>) -> Result<R2>;
}

impl MergeServiceHelper for Arc<Mutex<ChatHistoryManagerServer>> {
    fn process_merge_service_request<Q, R1, R2, Process, Finalize>(&self,
                                                                   req: &Request<Q>,
                                                                   mut process: Process,
                                                                   mut finalize: Finalize) -> TonicResult<R2>
        where Q: MergeServiceRequest + Debug,
              R2: Debug,
              Process: FnMut(
                  &Q,
                  Ref<Box<dyn ChatHistoryDao>>, Dataset,
                  Ref<Box<dyn ChatHistoryDao>>, Dataset,
              ) -> Result<R1>,
              Finalize: FnMut(R1, &mut ChmLock<'_>) -> Result<R2> {
        self.process_request(req, move |req, self_lock| {
            let m_dao = self_lock.loaded_daos.get(req.master_dao_key()).context("Master DAO not found")?;
            let s_dao = self_lock.loaded_daos.get(req.slave_dao_key()).context("Slave DAO not found")?;

            let m_dao = (*m_dao).borrow();
            let s_dao = (*s_dao).borrow();

            let m_ds_uuid = req.master_ds_uuid();
            let s_ds_uuid = req.slave_ds_uuid();

            let m_ds = m_dao.datasets()?.into_iter().find(|ds| &ds.uuid == m_ds_uuid)
                .context("Master dataset not found!")?;
            let s_ds = s_dao.datasets()?.into_iter().find(|ds| &ds.uuid == s_ds_uuid)
                .context("Slave dataset not found!")?;

            let pre_res = process(req, m_dao, m_ds, s_dao, s_ds)?;
            finalize(pre_res, self_lock)
        })
    }
}

trait MergeServiceRequest {
    fn master_dao_key(&self) -> &String;
    fn master_ds_uuid(&self) -> &PbUuid;
    fn slave_dao_key(&self) -> &String;
    fn slave_ds_uuid(&self) -> &PbUuid;
}
macro_rules! merge_req_impl {
    ($class:ident) => {
        impl MergeServiceRequest for $class {
            fn master_dao_key(&self) -> &String { &self.master_dao_key }
            fn master_ds_uuid(&self) -> &PbUuid { &self.master_ds_uuid }
            fn slave_dao_key(&self) -> &String { &self.slave_dao_key }
            fn slave_ds_uuid(&self) -> &PbUuid { &self.slave_ds_uuid }
        }
    };
}
merge_req_impl!(AnalyzeRequest);
merge_req_impl!(MergeRequest);
