#![allow(dead_code)]

use std::cmp::Ordering;

use crate::*;
use crate::dao::ChatHistoryDao;
use crate::protobuf::history::*;

#[cfg(test)]
#[path = "analyzer_tests.rs"]
mod tests;

struct DatasetDiffAnalyzer<'a> {
    m_dao: &'a dyn ChatHistoryDao,
    m_root: DatasetRoot,

    s_dao: &'a dyn ChatHistoryDao,
    s_root: DatasetRoot,
}

impl<'a> DatasetDiffAnalyzer<'a> {
    pub fn new(
        m_dao: &'a dyn ChatHistoryDao,
        m_ds: &'a Dataset,
        s_dao: &'a dyn ChatHistoryDao,
        s_ds: &'a Dataset,
    ) -> Self {
        let m_root = m_dao.dataset_root(m_ds.uuid());
        let s_root = s_dao.dataset_root(s_ds.uuid());
        DatasetDiffAnalyzer { m_dao, m_root, s_dao, s_root }
    }

    /** Note that we can only detect conflicts if data source supports source IDs. */
    pub fn analyze(
        &self,
        master_cwd: &ChatWithDetails,
        slave_cwd: &ChatWithDetails,
        title: &str,
    ) -> Result<Vec<MergeAnalysisSection>> {
        measure(|| {
            self.analyze_inner(
                AnalysContext {
                    mm_stream: messages_stream(self.m_dao, &master_cwd.chat, MasterMessage, |m| m)?,
                    m_cwd: master_cwd,
                    sm_stream: messages_stream(self.s_dao, &slave_cwd.chat, SlaveMessage, |m| m)?,
                    s_cwd: slave_cwd,
                }
            )
        }, |_, t| log::info!("Chat {title} analyzed in {t} ms"))
    }

    fn analyze_inner(&self, mut cx: AnalysContext) -> Result<Vec<MergeAnalysisSection>> {
        use AnalysisState::*;
        use InProgressState::*;

        let mut state = NoState;
        let mut acc: Vec<MergeAnalysisSection> = vec![];

        let matches = |mm: &MasterMessage, sm: &SlaveMessage|
            equals_with_no_mismatching_content(PracticalEqTuple::<MasterMessage>::new(mm, &self.m_root, cx.m_cwd),
                                               PracticalEqTuple::<SlaveMessage>::new(sm, &self.s_root, cx.s_cwd));
        loop {
            match (cx.peek(), &state) {
                //
                // NoState
                //

                ((Some(mm), Some(sm)), NoState) if matches(mm, sm)? => {
                    let (mm, sm) = cx.advance_both();
                    let mm_internal_id = mm.typed_id();
                    let sm_internal_id = sm.typed_id();

                    // Matching subsequence starts
                    state = InProgress(Match {
                        first_master_msg_id: mm_internal_id,
                        first_slave_msg_id: sm_internal_id,
                    });
                }

                // (Some(mm), Some(sm), NoState)
                // if mm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
                //     sm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
                //     mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption &&
                //     mm.fromId < 0x100000000L && sm.fromId > 0x100000000L &&
                //     (mm.copy(fromId = sm.fromId), masterRoot, cxt.mCwd) =~ = (sm, slaveRoot, cxt.sCwd) =>
                //
                // // // Special handling for a service message mismatch which is expected when merging Telegram after 2020-10
                // // // We register this one conflict and proceed in clean state.
                // // // This is dirty but relatively easy to do.
                // // val singleConflictState = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
                // // onDiffEnd(concludeDiff(cxt.advanceBoth(), singleConflictState))
                // // iterate(cxt.advanceBoth(), NoState, onDiffEnd)

                ((Some(mm), Some(sm)), NoState) if mm.source_id_option.is_some() && mm.source_id_option == sm.source_id_option => {
                    // Checking if there's a timestamp shift
                    {
                        let mut mm = mm.clone();
                        mm.0.timestamp = sm.timestamp;
                        if matches(&mm, sm)? {
                            let (ahead_behind, diff_sec) = {
                                let ts_diff = sm.timestamp - mm.timestamp;
                                assert!(ts_diff != 0);
                                if ts_diff > 0 {
                                    ("ahead of", ts_diff)
                                } else {
                                    ("behind", -ts_diff)
                                }
                            };
                            let diff_hrs = diff_sec / 3600;

                            bail!("Time shift detected between datasets! Slave is {} master by {} sec ({} hrs)",
                                ahead_behind, diff_sec, diff_hrs);
                        }
                    }

                    // Conflict started
                    // (Conflicts are only detectable if data source supply source IDs)

                    let (mm, sm) = cx.advance_both();
                    let mm_internal_id = mm.typed_id();
                    let sm_internal_id = sm.typed_id();
                    state = InProgress(Conflict {
                        first_master_msg_id: mm_internal_id,
                        first_slave_msg_id: sm_internal_id,
                    });
                }

                ((_, Some(_sm)), NoState) if cx.cmp_master_slave().is_gt() => {
                    // Addition started
                    let sm = cx.advance_slave();
                    let sm_internal_id = sm.typed_id();
                    state = InProgress(Addition {
                        first_slave_msg_id: sm_internal_id,
                    });
                }

                ((Some(_mm), _), NoState) if cx.cmp_master_slave().is_lt() => {
                    // Retention started
                    let mm = cx.advance_master();
                    let mm_internal_id = mm.typed_id();
                    state = InProgress(Retention {
                        first_master_msg_id: mm_internal_id,
                    });
                }

                //
                // Match continues
                //

                ((Some(mm), Some(sm)), InProgress(Match { .. })) if matches(mm, sm)? => {
                    cx.advance_both();
                }

                //
                // Addition continues
                //

                ((_, Some(_sm)), InProgress(Addition { .. }))
                if /*state.prev_master_msg_option == cx.prev_mm &&*/ cx.cmp_master_slave().is_gt() => {
                    cx.advance_slave();
                }


                //
                // Retention continues
                //

                ((Some(_mm), _), _state @ InProgress(Retention { .. }))
                if /*cx.prev_sm == prevSlaveMsgOption &&*/ cx.cmp_master_slave().is_lt() => {
                    cx.advance_master();
                }

                //
                // Conflict continues
                //

                ((Some(mm), Some(sm)), InProgress(Conflict { .. })) if !matches(mm, sm)? => {
                    cx.advance_both();
                }

                //
                // Section ended
                //

                ((_, _), InProgress(inner_state)) => {
                    acc.push(inner_state.make_section(
                        cx.mm_stream.last_id_option, cx.sm_stream.last_id_option));
                    state = NoState;
                }

                //
                // Streams ended
                //

                ((None, None), NoState) =>
                    break,

                ((mm, sm), NoState) =>
                    panic!("Unexpected state! ({:?}, {:?}, NoState)", mm, sm),
            }
        };

        Ok(acc)
    }
}

#[derive(Debug)]
enum AnalysisState {
    NoState,
    InProgress(InProgressState),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum InProgressState {
    Match {
        first_master_msg_id: MasterInternalId,
        first_slave_msg_id: SlaveInternalId,
    },
    Retention {
        first_master_msg_id: MasterInternalId,
    },
    Addition {
        first_slave_msg_id: SlaveInternalId,
    },
    Conflict {
        first_master_msg_id: MasterInternalId,
        first_slave_msg_id: SlaveInternalId,
    },
}

impl InProgressState {
    fn make_section(&self, mm_id: Option<MasterInternalId>, sm_id: Option<SlaveInternalId>) -> MergeAnalysisSection {
        match *self {
            InProgressState::Match { first_master_msg_id, first_slave_msg_id } => MergeAnalysisSection::Match {
                first_master_msg_id,
                last_master_msg_id: mm_id.unwrap(),
                first_slave_msg_id,
                last_slave_msg_id: sm_id.unwrap(),
            },
            InProgressState::Retention { first_master_msg_id } => MergeAnalysisSection::Retention {
                first_master_msg_id,
                last_master_msg_id: mm_id.unwrap(),
            },
            InProgressState::Addition { first_slave_msg_id } => MergeAnalysisSection::Addition {
                first_slave_msg_id,
                last_slave_msg_id: sm_id.unwrap(),
            },
            InProgressState::Conflict { first_master_msg_id, first_slave_msg_id } => MergeAnalysisSection::Conflict {
                first_master_msg_id,
                last_master_msg_id: mm_id.unwrap(),
                first_slave_msg_id,
                last_slave_msg_id: sm_id.unwrap(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeAnalysisSection {
    Match {
        first_master_msg_id: MasterInternalId,
        last_master_msg_id: MasterInternalId,
        first_slave_msg_id: SlaveInternalId,
        last_slave_msg_id: SlaveInternalId,
    },
    Retention {
        first_master_msg_id: MasterInternalId,
        last_master_msg_id: MasterInternalId,
    },
    Addition {
        first_slave_msg_id: SlaveInternalId,
        last_slave_msg_id: SlaveInternalId,
    },
    Conflict {
        first_master_msg_id: MasterInternalId,
        last_master_msg_id: MasterInternalId,
        first_slave_msg_id: SlaveInternalId,
        last_slave_msg_id: SlaveInternalId,
    },
}

struct AnalysContext<'a> {
    mm_stream: BatchedMessageIterator<'a, MasterMessage>,
    m_cwd: &'a ChatWithDetails,

    sm_stream: BatchedMessageIterator<'a, SlaveMessage>,
    s_cwd: &'a ChatWithDetails,
}

impl AnalysContext<'_> {
    fn cmp_master_slave(&self) -> Ordering {
        match self.peek() {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
            (Some(mm), Some(sm)) => {
                if mm.timestamp != sm.timestamp {
                    mm.timestamp.cmp(&sm.timestamp)
                } else if let (Some(msrcid), Some(ssrcid)) = (mm.source_id_option, sm.source_id_option) {
                    msrcid.cmp(&ssrcid)
                } else if mm.searchable_string == sm.searchable_string {
                    Ordering::Equal
                } else {
                    panic!("Cannot compare messages {:?} and {:?}!", mm.0, sm.0)
                }
            }
        }
    }

    fn peek(&self) -> (Option<&MasterMessage>, Option<&SlaveMessage>) {
        (self.mm_stream.peek(), self.sm_stream.peek())
    }

    fn advance_both(&mut self) -> (MasterMessage, SlaveMessage) {
        (self.advance_master(), self.advance_slave())
    }

    fn advance_master(&mut self) -> MasterMessage {
        let next = self.mm_stream.next().expect("Empty master stream advanced! This should've been checked");
        assert!(next.internal_id != *NO_INTERNAL_ID);
        next
    }

    fn advance_slave(&mut self) -> SlaveMessage {
        let next = self.sm_stream.next().expect("Empty slave stream advanced! This should've been checked");
        assert!(next.internal_id != *NO_INTERNAL_ID);
        next
    }
}

const BATCH_SIZE: usize = 1000;

fn messages_stream<'a, T: WithTypedId>(
    dao: &'a dyn ChatHistoryDao,
    chat: &'a Chat,
    wrap: fn(Message) -> T,
    unwrap_ref: fn(&T) -> &Message,
) -> Result<BatchedMessageIterator<'a, T>> {
    let mut res = BatchedMessageIterator {
        dao,
        chat,
        wrap,
        unwrap_ref,
        saved_batch: dao.first_messages(chat, BATCH_SIZE)?.into_iter(),
        next_option: None,
        last_id_option: None,
    };
    res.next_option = res.saved_batch.next().map(res.wrap);
    Ok(res)
}

struct BatchedMessageIterator<'a, T: WithTypedId> {
    dao: &'a dyn ChatHistoryDao,
    chat: &'a Chat,
    wrap: fn(Message) -> T,
    unwrap_ref: fn(&T) -> &Message,
    saved_batch: std::vec::IntoIter<Message>,
    next_option: Option<T>,
    last_id_option: Option<T::Item>,
}

impl<'a, T: WithTypedId> BatchedMessageIterator<'a, T> {
    fn peek(&self) -> Option<&T> {
        self.next_option.as_ref()
    }
}

impl<'a, T: WithTypedId> Iterator for BatchedMessageIterator<'a, T> {
    type Item = T; // TODO: Should it be Result<T>?

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next_option.take();
        if let Some(ref current) = current {
            match self.saved_batch.next() {
                Some(next) => {
                    // Iterator still has elements, cache it and be happy.
                    self.next_option = Some((self.wrap)(next));
                }
                None => {
                    // Iterator exhausted, time to preload next batch.
                    self.saved_batch = self.dao.messages_after(self.chat, (self.unwrap_ref)(current), BATCH_SIZE + 1)
                        .expect("Iterator errored out!").into_iter();
                    self.next_option = self.saved_batch.next().map(self.wrap);
                }
            }
        } // Otherwise iterator ended, no more elements.
        self.last_id_option = current.as_ref().map(|m| m.typed_id());
        current
    }
}

/**
 * Equality test, but treats master and slave messages as equal if either of them has content - unless they both do
 * and it's mismatching.
 * Also ignores edit timestamp if nothing else is changed.
 */
fn equals_with_no_mismatching_content(mm_eq: PracticalEqTuple<MasterMessage>,
                                      sm_eq: PracticalEqTuple<SlaveMessage>) -> Result<bool> {
    use message::Typed::*;
    use message_service::SealedValueOptional::*;

    fn has_content(c: &Option<Content>, root: &DatasetRoot) -> bool {
        c.as_ref().and_then(|c| c.path_file_option(root))
            .map(|p| p.exists())
            .unwrap_or(false)
    }
    fn photo_has_content(c: &Option<ContentPhoto>, root: &DatasetRoot) -> bool {
        c.as_ref().and_then(|photo| photo.path_option.as_ref())
            .map(|path| root.to_absolute(path).exists())
            .unwrap_or(false)
    }
    let mm_eq_sm = || mm_eq.apply(|m| &m.0).practically_equals(&sm_eq.apply(|m| &m.0));

    match (mm_eq.v.0.typed.as_ref(), sm_eq.v.0.typed.as_ref()) {
        (Some(Regular(mm_regular)), Some(Regular(sm_regular))) => {
            let mm_copy = Message {
                typed: Some(Regular(MessageRegular {
                    content_option: None,
                    edit_timestamp_option: None,
                    ..mm_regular.clone()
                })),
                ..mm_eq.v.0.clone()
            };
            let sm_copy = Message {
                typed: Some(Regular(MessageRegular {
                    content_option: None,
                    edit_timestamp_option: None,
                    ..sm_regular.clone()
                })),
                ..sm_eq.v.0.clone()
            };

            if !mm_eq.with(&mm_copy).practically_equals(&sm_eq.with(&sm_copy))? {
                return Ok(false);
            }

            if !has_content(&mm_regular.content_option, mm_eq.ds_root) ||
                !has_content(&sm_regular.content_option, sm_eq.ds_root) {
                return Ok(true);
            }

            mm_eq.with(&mm_regular.content_option).practically_equals(&sm_eq.with(&sm_regular.content_option))
        }
        (Some(Service(MessageService { sealed_value_optional: Some(GroupEditPhoto(MessageServiceGroupEditPhoto { photo: mm_photo })) })),
            Some(Service(MessageService { sealed_value_optional: Some(GroupEditPhoto(MessageServiceGroupEditPhoto { photo: sm_photo })) }))) => {
            if !photo_has_content(mm_photo, mm_eq.ds_root) || !photo_has_content(sm_photo, sm_eq.ds_root) {
                return Ok(true);
            }
            mm_eq_sm()
        }
        (Some(Service(MessageService { sealed_value_optional: Some(SuggestProfilePhoto(MessageServiceSuggestProfilePhoto { photo: mm_photo })) })),
            Some(Service(MessageService { sealed_value_optional: Some(SuggestProfilePhoto(MessageServiceSuggestProfilePhoto { photo: sm_photo })) }))) => {
            if !photo_has_content(mm_photo, mm_eq.ds_root) || !photo_has_content(sm_photo, sm_eq.ds_root) {
                return Ok(true);
            }
            mm_eq_sm()
        }
        _ => mm_eq_sm()
    }
}
