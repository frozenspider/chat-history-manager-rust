package org.fs.chm.dao.merge

import java.io.File

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.loader.H2DataManager
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._
import org.fs.utility.StopWatch

class DatasetMergerLocal(
    val masterDao: ChatHistoryDao,
    val masterDs: Dataset,
    val slaveDao: ChatHistoryDao,
    val slaveDs: Dataset,
    createDao: File => MutableChatHistoryDao,
) extends DatasetMerger {
  import DatasetMergerLocal._

  private val masterRoot = masterDao.datasetRoot(masterDs.uuid)
  private val slaveRoot  = slaveDao.datasetRoot(slaveDs.uuid)

  /**
   * Analyze dataset mergeability, amending `ChatMergeOption.Combine` with mismatches in order.
   * Other `ChatMergeOption`s are returned unchanged.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  override def analyze(masterCwd: ChatWithDetails, slaveCwd: ChatWithDetails, title: String): IndexedSeq[MessagesMergeDiff] ={
    StopWatch.measureAndCall {
      var diffs = ArrayBuffer.empty[MessagesMergeDiff]
      iterate(
        MsgIterationContext(
          mmStream = messagesStream(masterDao, masterCwd.chat, None),
          prevMm   = None,
          mCwd     = masterCwd,
          smStream = messagesStream(slaveDao, slaveCwd.chat, None),
          prevSm   = None,
          sCwd     = slaveCwd,
        ),
        IterationState.NoState,
        (mm => diffs += mm)
      )
      diffs.toIndexedSeq
    }((_, t) => log.info(s"Chat $title analyzed in $t ms"))
  }

  /** Stream messages, either from the beginning or from the given one (exclusive) */
  protected[merge] def messagesStream[TM <: TaggedMessage, TMId <: TaggedMessageId](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageIdOption: Option[TMId]
  ): Stream[TM] = {
    messageBatchesStream[TM, TMId](dao, chat, fromMessageIdOption).flatten
  }

  /** Stream messages, either from the beginning or from the given one (exclusive) */
  protected[merge] def messageBatchesStream[TM <: TaggedMessage, TMId <: TaggedMessageId](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageIdOption: Option[TMId]
  ): Stream[IndexedSeq[TM]] = {
    val batch = fromMessageIdOption
      .map(fromId => dao.messagesAfter(chat, fromId, BatchSize + 1).drop(1))
      .getOrElse(dao.firstMessages(chat, BatchSize))
      .asInstanceOf[IndexedSeq[TM]]
    if (batch.isEmpty) {
      Stream.empty
    } else if (batch.size < BatchSize) {
      Stream(batch)
    } else {
      Stream(batch) #::: messageBatchesStream[TM, TMId](dao, chat, Some(batch.last.internalId.asInstanceOf[TMId]))
    }
  }

  private def concludeDiff(cxt: MsgIterationContext,
                           state: IterationState.StateInProgress): MessagesMergeDiff = {
    import IterationState._
    state match {
      case MatchInProgress(_, startMasterMsg, _, startSlaveMsg) =>
        MessagesMergeDiff.Match(
          firstMasterMsgId = startMasterMsg.taggedId,
          lastMasterMsgId  = cxt.prevMm.get.taggedId,
          firstSlaveMsgId  = startSlaveMsg.taggedId,
          lastSlaveMsgId   = cxt.prevSm.get.taggedId
        )
      case RetentionInProgress(_, startMasterMsg, _) =>
        MessagesMergeDiff.Retain(
          firstMasterMsgId = startMasterMsg.taggedId,
          lastMasterMsgId  = cxt.prevMm.get.taggedId,
        )
      case AdditionInProgress(prevMasterMsgOption, prevSlaveMsgOption, startSlaveMsg) =>
        assert(cxt.prevMm == prevMasterMsgOption) // Master stream hasn't advanced
        assert(cxt.prevSm.isDefined)
        MessagesMergeDiff.Add(
          firstSlaveMsgId = startSlaveMsg.taggedId,
          lastSlaveMsgId  = cxt.prevSm.get.taggedId
        )
      case ConflictInProgress(prevMasterMsgOption, startMasterMsg, prevSlaveMsgOption, startSlaveMsg) =>
        assert(cxt.prevMm.isDefined && cxt.prevSm.isDefined)
        MessagesMergeDiff.Replace(
          firstMasterMsgId = startMasterMsg.taggedId,
          lastMasterMsgId  = cxt.prevMm.get.taggedId,
          firstSlaveMsgId  = startSlaveMsg.taggedId,
          lastSlaveMsgId   = cxt.prevSm.get.taggedId
        )
    }
  }

  /** Iterate through both master and slave streams using state machine like approach */
  @tailrec
  private def iterate(
      cxt: MsgIterationContext,
      state: IterationState,
      onDiffEnd: MessagesMergeDiff => Unit
  ): Unit = {
    import IterationState._

    if (Thread.interrupted()) {
      throw new InterruptedException()
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {

      //
      // Streams ended
      //

      case (None, None, NoState) =>
        ()
      case (None, None, state: StateInProgress) =>
        onDiffEnd(concludeDiff(cxt, state))

      //
      // NoState
      //

      case (Some(mm), Some(sm), NoState) if equalsWithNoMismatchingContent(mm, cxt.mCwd, sm, cxt.sCwd) =>
        // Matching subsequence starts
        val state2 = MatchInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onDiffEnd)
      case (Some(mm), Some(sm), NoState)
        if mm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
           sm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
           mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption &&
           mm.fromId < 0x100000000L && sm.fromId > 0x100000000L &&
           (mm.copy(fromId = sm.fromId), masterRoot, cxt.mCwd) =~= (sm, slaveRoot, cxt.sCwd) =>

        // Special handling for a service message mismatch which is expected when merging Telegram after 2020-10
        // We register this one conflict and proceed in clean state.
        // This is dirty but relatively easy to do.
        val singleConflictState = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        onDiffEnd(concludeDiff(cxt.advanceBoth(), singleConflictState))
        iterate(cxt.advanceBoth(), NoState, onDiffEnd)
      case (Some(mm), Some(sm), NoState) if mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption =>
        // Checking if there's a timestamp shift
        if (equalsWithNoMismatchingContent(mm.copy(timestamp = sm.timestamp).asInstanceOf[TaggedMessage.M], cxt.mCwd, sm, cxt.sCwd)) {
          val (aheadBehind, diffSec) = {
            val tsDiff = sm.timestamp - mm.timestamp
            assert(tsDiff != 0)
            if (tsDiff > 0) {
              ("ahead of", tsDiff)
            } else {
              ("behind", -tsDiff)
            }
          }
          val diffHrs = diffSec / 3600

          throw new IllegalStateException("Time shift detected between datasets! " +
            s"Slave is ${aheadBehind} master by ${diffSec} sec (${diffHrs} hrs)")
        }
        // Conflict started
        // (Conflicts are only detectable if data source supply source IDs)
        val state2 = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onDiffEnd)
      case (_, Some(sm), NoState) if cxt.cmpMasterSlave() > 0 =>
        // Addition started
        val state2 = AdditionInProgress(cxt.prevMm, cxt.prevSm, sm)
        iterate(cxt.advanceSlave(), state2, onDiffEnd)
      case (Some(mm), _, NoState) if cxt.cmpMasterSlave() < 0 =>
        // Retention started
        val state2 = RetentionInProgress(cxt.prevMm, mm, cxt.prevSm)
        iterate(cxt.advanceMaster(), state2, onDiffEnd)

      //
      // AdditionInProgress
      //

      case (_, Some(sm), state: AdditionInProgress)
          if state.prevMasterMsgOption == cxt.prevMm && cxt.cmpMasterSlave() > 0 =>
        // Addition continues
        iterate(cxt.advanceSlave(), state, onDiffEnd)
      case (_, _, state: AdditionInProgress) =>
        // Addition ended
        onDiffEnd(concludeDiff(cxt, state))
        iterate(cxt, NoState, onDiffEnd)

      //
      // MatchInProgress
      //

      case (Some(mm), Some(sm), state: MatchInProgress) if equalsWithNoMismatchingContent(mm, cxt.mCwd, sm, cxt.sCwd) =>
        // Matching subsequence continues
        iterate(cxt.advanceBoth(), state, onDiffEnd)
      case (_, _, state: MatchInProgress) =>
        // Matching subsequence ends
        onDiffEnd(concludeDiff(cxt, state))
        iterate(cxt, NoState, onDiffEnd)

      //
      // RetentionInProgress
      //

      case (Some(mm), _, RetentionInProgress(_, _, prevSlaveMsgOption))
          if (cxt.prevSm == prevSlaveMsgOption) && cxt.cmpMasterSlave() < 0 =>
        // Retention continues
        iterate(cxt.advanceMaster(), state, onDiffEnd)
      case (_, _, state: RetentionInProgress) =>
        // Retention ended
        onDiffEnd(concludeDiff(cxt, state))
        iterate(cxt, NoState, onDiffEnd)

      //
      // ConflictInProgress
      //

      case (Some(mm), Some(sm), state: ConflictInProgress)
          if !equalsWithNoMismatchingContent(mm, cxt.mCwd, sm, cxt.sCwd) =>
        // Conflict continues
        iterate(cxt.advanceBoth(), state, onDiffEnd)
      case (_, _, state: ConflictInProgress) =>
        // Conflict ended
        onDiffEnd(concludeDiff(cxt, state))
        iterate(cxt, NoState, onDiffEnd)

      case other => unexpectedCase(other)
    }
  }

  override def merge(
      usersToMerge: Seq[UserMergeOption],
      chatsToMerge: Seq[ResolvedChatMergeOption],
      newDbPath: File
  ): (ChatHistoryDao, Dataset) = {
    val newDao = createDao(newDbPath)
    StopWatch.measureAndCall {
      try {
        if (newDao.datasets.nonEmpty) {
          newDao.backup()
        }
        newDao.disableBackups()
        val newDs = Dataset(
          uuid       = randomUuid,
          alias      = masterDs.alias + " (merged)",
        )
        newDao.insertDataset(newDs)

        // Sanity check
        for {
          firstMasterChat <- chatsToMerge.find(_.masterCwdOption.isDefined)
          masterCwd       <- firstMasterChat.masterCwdOption
        } require(masterDao.users(masterCwd.chat.dsUuid).size <= usersToMerge.size, "Not enough user merges!")

        // Users
        val masterSelf = masterDao.myself(masterDs.uuid)
        require(
          usersToMerge.map(_.userToInsertOption).yieldDefined.count(_.id == masterSelf.id) == 1,
          "User merges should contain exactly one self user!"
        )
        for {
          sourceUser <- usersToMerge
          userToInsert <- sourceUser.userToInsertOption
        } {
          val user2 = userToInsert.copy(dsUuid = newDs.uuid)
          newDao.insertUser(user2, user2.id == masterSelf.id)
        }
        val finalUsers = newDao.users(newDs.uuid)

        // Chats
        for (cmo <- chatsToMerge if !cmo.isInstanceOf[ChatMergeOption.DontAdd]) {
          val (dsRoot, chat) = {
            Seq(
              cmo.slaveCwdOption.map(cwd => (slaveDao.datasetRoot(cwd.dsUuid), cwd.chat)),
              cmo.masterCwdOption.map(cwd => (masterDao.datasetRoot(cwd.dsUuid), cwd.chat))
            ).yieldDefined.head match {
              case (f, c) =>
                val c2 = (c.tpe, c.memberIds.find(_ != masterSelf.id)) match {
                  case (ChatType.Personal, Some(userId)) =>
                    // For merged personal chats, name should match whatever user name was chosen
                    val user = finalUsers.find(_.id == userId).get
                    c.copy(nameOption = user.prettyNameOption)
                  case _ =>
                    c
                }
                (f, c2.copy(dsUuid = newDs.uuid))
            }
          }
          newDao.insertChat(dsRoot, chat)

          // Messages
          val messageBatches: Stream[(DatasetRoot, IndexedSeq[Message])] = cmo match {
            case ChatMergeOption.Keep(mcwd) =>
              messageBatchesStream[TaggedMessage.M, TaggedMessageId.M](masterDao, mcwd.chat, None)
                .map(_.map(fixupMessageWithMembers(mcwd, finalUsers)))
                .map(mb => (masterDao.datasetRoot(masterDs.uuid), mb))
            case ChatMergeOption.DontAdd(_) =>
              Stream.empty
            case ChatMergeOption.Add(scwd) =>
              messageBatchesStream[TaggedMessage.S, TaggedMessageId.S](slaveDao, scwd.chat, None)
                .map(_.map(fixupMessageWithMembers(scwd, finalUsers)))
                .map(mb => (slaveDao.datasetRoot(slaveDs.uuid), mb))
            case ChatMergeOption.ResolvedCombine(mc, sc, resolution) =>
              val res: Seq[Stream[(DatasetRoot, IndexedSeq[Message])]] =
                resolution.map {
                  case MessagesMergeDiff.Retain(firstMasterMsg, lastMasterMsg) =>
                    batchLoadMsgsUntilInc(finalUsers, masterDao, masterDs, cmo.masterCwdOption.get, firstMasterMsg, lastMasterMsg)
                  case MessagesMergeDiff.Add(firstSlaveMsg, lastSlaveMsg) =>
                    batchLoadMsgsUntilInc(finalUsers, slaveDao, slaveDs, cmo.slaveCwdOption.get, firstSlaveMsg, lastSlaveMsg)
                  case MessagesMergeDiff.DontAdd(_, _) =>
                    Stream.empty
                  case MessagesMergeDiff.Replace(firstMasterMsg, lastMasterMsg, firstSlaveMsg, lastSlaveMsg) =>
                    // Treat exactly as Add
                    // TODO: Should we analyze content and make sure nothing is lost?
                    batchLoadMsgsUntilInc(finalUsers, slaveDao, slaveDs, cmo.slaveCwdOption.get, firstSlaveMsg, lastSlaveMsg)
                  case MessagesMergeDiff.DontReplace(firstMasterMsg, lastMasterMsg, firstSlaveMsg, lastSlaveMsg) =>
                    // Treat exactly as Retain
                    // TODO: Should we analyze content and make sure nothing is lost?
                    batchLoadMsgsUntilInc(finalUsers, masterDao, masterDs, cmo.masterCwdOption.get, firstMasterMsg, lastMasterMsg)
                  case MessagesMergeDiff.Match(firstMasterMsg, lastMasterMsg, firstSlaveMsg, lastSlaveMsg) =>
                    // Note: while messages to match, our matching rules allow either master or slave to have missing content.
                    // We keep master messages unless slave has new content.
                    val masterStream =
                      batchLoadMsgsUntilInc(finalUsers, masterDao, masterDs, cmo.masterCwdOption.get, firstMasterMsg, lastMasterMsg)
                    val slaveStream =
                      batchLoadMsgsUntilInc(finalUsers, slaveDao, slaveDs, cmo.slaveCwdOption.get, firstSlaveMsg, lastSlaveMsg)
                    val mixedFlatStream: Stream[(DatasetRoot, Message)] =
                      masterStream.zip(slaveStream).flatMap { case ((mDsRoot, mMsgs), (sDsRoot, sMsgs)) =>
                        assert(mMsgs.length == sMsgs.length)
                        mMsgs.zip(sMsgs).map { case (mMsg, sMsg) =>
                          val mFiles = mMsg.files(mDsRoot).filter(_.exists())
                          val sFiles = sMsg.files(sDsRoot).filter(_.exists())
                          if (mFiles.size >= sFiles.size) {
                            (mDsRoot, mMsg)
                          } else {
                            (sDsRoot, sMsg)
                          }
                        }
                      }

                    def groupConsecutivePairs[A, B](stream: Stream[(A, B)]): Stream[(A, IndexedSeq[B])] =
                      if (stream.isEmpty) Stream.empty else {
                        val key = stream.head._1
                        val (matching, rest) = stream.span(_._1 == key)
                        val segment = matching.map(_._2).toIndexedSeq
                        (key, segment) #:: groupConsecutivePairs(rest)
                      }

                    groupConsecutivePairs(mixedFlatStream)
                }
              res.toStream.flatten
          }

          for ((srcDsRoot, mb) <- messageBatches) {
            // Also copies files
            newDao.insertMessages(srcDsRoot, chat, mb)
          }
        }

        (newDao, newDs)
      } finally {
        newDao.enableBackups()
      }
    } ((_, t) => log.info(s"Datasets merged in ${t} ms"))
  }

  //
  // Helpers
  //

  /**
   * Treats master and slave messages as equal if either of them has content - unless they both do and it's mismatching.
   * Also ignores edit timestamp if nothing else is changed.
   */
  private def equalsWithNoMismatchingContent(mm: TaggedMessage.M,
                                             mCwd: ChatWithDetails,
                                             sm: TaggedMessage.S,
                                             sCwd: ChatWithDetails): Boolean = {
    /**
     * Special case: Telegram 2023-11 started exporting double styles (bold+X)
     * as bold instead of an X. We want to ignore this change.
     */
    def textToComparable(text: Seq[RichTextElement]): Seq[RichTextElement] = {
      text map (t => t.`val` match {
        case rte: RichTextElement.Val.Italic        => RichText.makeBold(rte.value.text)
        case rte: RichTextElement.Val.Underline     => RichText.makeBold(rte.value.text)
        case rte: RichTextElement.Val.Strikethrough => RichText.makeBold(rte.value.text)
        case _                                      => t
      })
    }

    def toComparable(m: Message, mr: Message.Typed.Regular): Message = {
      m.copy(
        typed = Message.Typed.Regular(mr.value.copy(contentOption = None, editTimestampOption = None)),
        text  = textToComparable(m.text)
      )
    }

    def hasContent(c: WithPathFileOption, root: DatasetRoot): Boolean = {
      c.pathFileOption(root).exists(_.exists)
    }

    (mm.asInstanceOf[Message], masterRoot, mCwd) =~= (sm, slaveRoot, sCwd) ||
      ((mm.typed, sm.typed) match {
        case (mmRegular: Message.Typed.Regular, smRegular: Message.Typed.Regular) =>
          val contentEquals = (mmRegular.value.contentOption, smRegular.value.contentOption) match {
            case (Some(mc), Some(sc)) if mc.getClass == sc.getClass && mc.hasPath =>
              (!hasContent(mc, masterRoot) || !hasContent(sc, slaveRoot))
            case (None, None) => true
            case _            => false
          }
          contentEquals && {
            val mmCmp = toComparable(mm, mmRegular)
            val smCmp = toComparable(sm, smRegular)
            (mmCmp, masterRoot, mCwd) =~= (smCmp, slaveRoot, sCwd)
          }
        case (Message.Typed.Service(Some(MessageServiceGroupEditPhoto(mmPhoto, _))),
              Message.Typed.Service(Some(MessageServiceGroupEditPhoto(smPhoto, _)))) =>
          !hasContent(mmPhoto, masterRoot) || !hasContent(smPhoto, slaveRoot)
        case (Message.Typed.Service(Some(MessageServiceSuggestProfilePhoto(mmPhoto, _))),
              Message.Typed.Service(Some(MessageServiceSuggestProfilePhoto(smPhoto, _)))) =>
          !hasContent(mmPhoto, masterRoot) || !hasContent(smPhoto, slaveRoot)
        case _ => false
      })
  }

  /** If message dates and plain strings are equal, we consider this enough */
  private val msgOrdering = new Ordering[Message] {
    override def compare(x: Message, y: Message): Int = {
      (x, y) match {
        case _ if x.time != y.time =>
          x.time compareTo y.time
        case _ if x.sourceIdOption.isDefined && y.sourceIdOption.isDefined =>
          x.sourceIdOption.get compareTo y.sourceIdOption.get
        case _ if x.searchableString == y.searchableString =>
          0
        case _ =>
          throw new IllegalStateException(s"Cannot compare messages $x and $y!")
      }
    }
  }

  private val msgOptionOrdering = new Ordering[Option[Message]] {
    override def compare(xo: Option[Message], yo: Option[Message]): Int = {
      (xo, yo) match {
        case (None, None)       => 0
        case (None, _)          => 1
        case (_, None)          => -1
        case (Some(x), Some(y)) => msgOrdering.compare(x, y)
      }
    }
  }

  private case class MsgIterationContext(
    mmStream: Stream[TaggedMessage.M],
    prevMm:   Option[TaggedMessage.M],
    mCwd:     ChatWithDetails,
    smStream: Stream[TaggedMessage.S],
    prevSm:   Option[TaggedMessage.S],
    sCwd:     ChatWithDetails
  ) {
    def cmpMasterSlave(): Int = {
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption)
    }

    def advanceBoth(): MsgIterationContext =
      copy(
        mmStream = mmStream.tail,
        prevMm   = mmStream.headOption,
        smStream = smStream.tail,
        prevSm   = smStream.headOption
      )

    def advanceMaster(): MsgIterationContext =
      copy(
        mmStream = mmStream.tail,
        prevMm   = mmStream.headOption
      )

    def advanceSlave(): MsgIterationContext =
      copy(
        smStream = smStream.tail,
        prevSm   = smStream.headOption
      )
  }

  /** Fixup messages who have 'members' field, to make them comply with resolved/final user names. */
  def fixupMessageWithMembers(cwd: ChatWithDetails, finalUsers: Seq[User])(message: Message): Message = {
    def fixupMembers(members: Seq[String]): Seq[String] = {
      // Unresolved members are kept as-is.
      val resolvedUsers = cwd.resolveMembers(members)
      resolvedUsers.mapWithIndex((u, idx) =>
        finalUsers.find(fu => u.exists(_.id == fu.id)).map(_.prettyName).getOrElse(members(idx)))
    }

    def withTypedService(v: MessageService) = message.copy(typed = Message.Typed.Service(Some(v)))

    message.typed.service.flatten match {
      case Some(culprit: MessageServiceGroupCreate) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupInviteMembers) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupRemoveMembers) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupCall) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case _ =>
        message
    }
  }

  private def batchLoadMsgsUntilInc[TMId <: TaggedMessageId](
      finalUsers: Seq[User],
      dao: ChatHistoryDao,
      ds: Dataset,
      cwd: ChatWithDetails,
      firstMsgId: TMId,
      lastMsgId: TMId
  ): Stream[(DatasetRoot, IndexedSeq[Message])] = {
    // TODO: Inefficient!
    takeMsgsFromBatchUntilInc(
      IndexedSeq(dao.messageOptionByInternalId(cwd.chat, firstMsgId).get) #:: messageBatchesStream(dao, cwd.chat, Some(firstMsgId)),
      lastMsgId
    ) map (mb => (dao.datasetRoot(ds.uuid), mb.map(fixupMessageWithMembers(cwd, finalUsers))))
  }

  private def takeMsgsFromBatchUntilInc[TM <: TaggedMessage, TMId <: TaggedMessageId](
      stream: Stream[IndexedSeq[Message]],
      lastMsgId: TMId
  ): Stream[IndexedSeq[Message]] = {
    var lastFound = false
    stream.map { mb =>
      if (!lastFound) {
        mb.takeWhile { m2 =>
          val isLast = m2.internalId == lastMsgId
          lastFound |= isLast
          isLast || !lastFound
        }
      } else {
        IndexedSeq.empty
      }
    }.takeWhile(_.nonEmpty)
  }
}

object DatasetMergerLocal {
  protected[merge] val BatchSize = 1000

  private sealed trait IterationState
  private object IterationState {
    case object NoState extends IterationState

    sealed trait StateInProgress extends IterationState
    case class MatchInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
    case class RetentionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S]
    ) extends StateInProgress
    case class AdditionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
    case class ConflictInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
  }
}