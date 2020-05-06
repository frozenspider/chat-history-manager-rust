package org.fs.chm.dao.merge

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao._
import org.fs.chm.dao.merge.ChatHistoryMerger._
import org.fs.chm.utility.EntityUtils._

class ChatHistoryMerger(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset
) {

  /**
   * Analyze dataset mergeability, returning the map from slave chat to mismatches in order.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  def analyzeMergingChats(mc: Chat, sc: Chat): Seq[Mismatch] = {
    def messagesStream[T <: TaggedMessage](dao: ChatHistoryDao, chat: Chat, offset: Int): Stream[T] = {
      if (offset >= chat.msgCount) {
        Stream.empty
      } else {
        val batch = dao.scrollMessages(chat, offset, BatchSize).asInstanceOf[IndexedSeq[T]]
        batch.toStream #::: messagesStream[T](dao, chat, offset + batch.size)
      }
    }
    var mismatches = ArrayBuffer.empty[Mismatch]
    iterate(
      ((messagesStream(masterDao, mc, 0), None), (messagesStream(slaveDao, sc, 0), None)),
      IterationState.NoState,
      (mm => mismatches += mm)
    )
    mismatches.toVector
  }

  /** Iterate through both master and slave streams using state machine like approach */
  @tailrec
  private def iterate(
      cxt: IterationContext,
      state: IterationState,
      onMismatch: Mismatch => Unit
  ): Unit = {
    import IterationState._
    def mismatchOptionAfterConflictEnd(state: StateInProgress): Option[Mismatch] = {
      state match {
        case AdditionInProgress(prevMasterMsgOption, prevSlaveMsgOption, startSlaveMsg) =>
          assert(cxt.prevMm == prevMasterMsgOption) // Master stream hasn't advanced
          assert(cxt.prevSm.isDefined)
          Some(
            Mismatch.Addition(
              prevMasterMsgOption = prevMasterMsgOption,
              nextMasterMsgOption = cxt.mmStream.headOption,
              prevSlaveMsgOption  = prevSlaveMsgOption,
              slaveMsgs           = (startSlaveMsg, cxt.prevSm.get),
              nextSlaveMsgOption  = cxt.smStream.headOption
            )
          )
        case ConflictInProgress(prevMasterMsgOption, startMasterMsg, prevSlaveMsgOption, startSlaveMsg) =>
          assert(cxt.prevMm.isDefined && cxt.prevSm.isDefined)
          Some(
            Mismatch.Conflict(
              prevMasterMsgOption = prevMasterMsgOption,
              masterMsgs          = (startMasterMsg, cxt.prevMm.get),
              nextMasterMsgOption = cxt.mmStream.headOption,
              prevSlaveMsgOption  = prevSlaveMsgOption,
              slaveMsgs           = (startSlaveMsg, cxt.prevSm.get),
              nextSlaveMsgOption  = cxt.smStream.headOption
            )
          )
        case RetentionInProgress(_, _, prevSlaveMsgOption) =>
          assert(cxt.prevSm == prevSlaveMsgOption) // Slave stream hasn't advanced
          // We don't treat retention as a mismatch
          None
      }
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {

      //
      // Streams ended
      //

      case (None, None, NoState) =>
        ()
      case (None, None, state: StateInProgress) =>
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch

      //
      // NoState
      //

      case (Some(mm), Some(sm), NoState) if mm =~= sm =>
        // Matching subsequence continues
        iterate(cxt.advanceBoth(), NoState, onMismatch)
      case (Some(mm), Some(sm), NoState) if mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption =>
        // Conflict started
        // (Conflicts are only detectable if data source supply source IDs)
        val state2 = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onMismatch)
      case (_, Some(sm), NoState) if cxt.cmpMasterSlave() > 0 =>
        // Addition started
        val state2 = AdditionInProgress(cxt.prevMm, cxt.prevSm, sm)
        iterate(cxt.advanceSlave(), state2, onMismatch)
      case (Some(mm), _, NoState) if cxt.cmpMasterSlave() < 0 =>
        // Retention started
        val state2 = RetentionInProgress(cxt.prevMm, mm, cxt.prevSm)
        iterate(cxt.advanceMaster(), state2, onMismatch)

      //
      // AdditionInProgress
      //

      case (_, Some(sm), state: AdditionInProgress)
          if state.prevMasterMsgOption == cxt.prevMm && cxt.cmpMasterSlave() > 0 =>
        // Addition continues
        iterate(cxt.advanceSlave(), state, onMismatch)
      case (_, _, state: AdditionInProgress) =>
        // Addition ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)

      //
      // RetentionInProgress
      //

      case (Some(mm), _, RetentionInProgress(_, _, prevSlaveMsgOption))
          if (cxt.prevSm == prevSlaveMsgOption) && cxt.cmpMasterSlave() < 0 =>
        // Retention continues
        iterate(cxt.advanceMaster(), state, onMismatch)
      case (_, _, state: RetentionInProgress) =>
        // Retention ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)

      //
      // ConflictInProgress
      //

      case (Some(mm), Some(sm), state: ConflictInProgress) if mm !=~= sm =>
        // Conflict continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: ConflictInProgress) =>
        // Conflict ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)
    }
  }

  def mergeChats(
      newDs: Dataset,
      masterChat: Chat,
      slaveChat: Chat,
      resolutions: Map[Mismatch, MismatchResolution]
  ): Unit = {
    /*
     * Do the same as analyze, reuse as much as possible
     */
    ???
  }

  //
  // Helpers
  //

  /** If message dates and plain strings are equal, we consider this enough */
  private val msgOrdering = new Ordering[Message] {
    override def compare(x: Message, y: Message): Int = {
      (x, y) match {
        case _ if x.time != y.time =>
          x.time compareTo y.time
        case _ if x.sourceIdOption.isDefined && y.sourceIdOption.isDefined =>
          x.sourceIdOption.get compareTo y.sourceIdOption.get
        case _ if x.plainSearchableString == y.plainSearchableString =>
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

  private type IterationContext =
    ((Stream[TaggedMessage.M], Option[TaggedMessage.M]), (Stream[TaggedMessage.S], Option[TaggedMessage.S]))

  private implicit class RichIterationContext(cxt: IterationContext) {
    def mmStream: Stream[TaggedMessage.M] = cxt._1._1
    def prevMm:   Option[TaggedMessage.M] = cxt._1._2
    def smStream: Stream[TaggedMessage.S] = cxt._2._1
    def prevSm:   Option[TaggedMessage.S] = cxt._2._2

    def cmpMasterSlave(): Int = {
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption)
    }

    def advanceBoth(): IterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream.tail, smStream.headOption))
    }

    def advanceMaster(): IterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream, prevSm))
    }

    def advanceSlave(): IterationContext = {
      ((mmStream, prevMm), (smStream.tail, smStream.headOption))
    }
  }

  private sealed trait IterationState
  private object IterationState {
    case object NoState extends IterationState

    sealed trait StateInProgress extends IterationState
    case class AdditionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
    case class RetentionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S]
    ) extends StateInProgress
    case class ConflictInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
  }
}

object ChatHistoryMerger {

  protected[merge] val BatchSize = 1000

  // Message tagged types
  sealed trait TaggedMessage
  object TaggedMessage {
    sealed trait MasterMessageTag extends TaggedMessage
    sealed trait SlaveMessageTag  extends TaggedMessage

    type M = Message with MasterMessageTag
    type S = Message with SlaveMessageTag
  }

  /** Represents a single general merge option: a chat that should be added or merged (or skipped if no decision) */
  sealed trait ChatMergeOption
  sealed trait ChangedChatMergeOption extends ChatMergeOption
  object ChatMergeOption {
    case class Combine(masterChat: Chat, slaveChat: Chat) extends ChangedChatMergeOption
    case class Add(slaveChat: Chat)                       extends ChangedChatMergeOption
    case class Retain(masterChat: Chat)                   extends ChatMergeOption
  }

  sealed trait Mismatch {
    def prevMasterMsgOption: Option[TaggedMessage.M]
    def firstMasterMsgOption: Option[TaggedMessage.M]
    def lastMasterMsgOption: Option[TaggedMessage.M]
    def nextMasterMsgOption: Option[TaggedMessage.M]
    protected def slaveMsgs: (TaggedMessage.S, TaggedMessage.S)
    def prevSlaveMsgOption: Option[TaggedMessage.S]
    def firstSlaveMsg: TaggedMessage.S = slaveMsgs._1
    def lastSlaveMsg:  TaggedMessage.S = slaveMsgs._2
    def nextSlaveMsgOption: Option[TaggedMessage.S]
  }
  object Mismatch {
    case class Addition(
        prevMasterMsgOption: Option[TaggedMessage.M],
        nextMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        /** First and last */
        slaveMsgs: (TaggedMessage.S, TaggedMessage.S),
        nextSlaveMsgOption: Option[TaggedMessage.S]
    ) extends Mismatch {
      override def firstMasterMsgOption = None
      override def lastMasterMsgOption = None
    }

    case class Conflict(
        prevMasterMsgOption: Option[TaggedMessage.M],
        masterMsgs: (TaggedMessage.M, TaggedMessage.M),
        nextMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        /** First and last */
        slaveMsgs: (TaggedMessage.S, TaggedMessage.S),
        nextSlaveMsgOption: Option[TaggedMessage.S]
    ) extends Mismatch {
      def firstMasterMsg: TaggedMessage.M = masterMsgs._1
      def lastMasterMsg:  TaggedMessage.M = masterMsgs._2
      override def firstMasterMsgOption = Some(firstMasterMsg)
      override def lastMasterMsgOption = Some(lastMasterMsg)
    }
  }

  sealed trait MismatchResolution
  object MismatchResolution {
    case object Apply  extends MismatchResolution
    case object Reject extends MismatchResolution
  }
}
