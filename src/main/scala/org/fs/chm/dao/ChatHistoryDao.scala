package org.fs.chm.dao

import java.io.{File => JFile}

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.LangUtils._

/**
 * Everything except for messages should be pre-cached and readily available.
 * Should support equality.
 */
trait ChatHistoryDao extends AutoCloseable {
  sys.addShutdownHook(close())

  /** User-friendly name of a loaded data */
  def name: String

  /** Directory which stores eveything - including database itself at the root level. */
  def storagePath: JFile

  def datasets: Seq[Dataset]

  /** Directory which stores eveything in the dataset. All files are guaranteed to have this as a prefix */
  def datasetRoot(dsUuid: PbUuid): DatasetRoot

  /** List all files referenced by entities of this dataset. Some might not exist. */
  def datasetFiles(dsUuid: PbUuid): Set[JFile]

  def myself(dsUuid: PbUuid): User

  /** Contains myself as the first element. Order must be stable. Method is expected to be fast. */
  def users(dsUuid: PbUuid): Seq[User]

  def userOption(dsUuid: PbUuid, id: Long): Option[User]

  def chats(dsUuid: PbUuid): Seq[ChatWithDetails]

  def chatOption(dsUuid: PbUuid, id: Long): Option[ChatWithDetails]

  /** Return N messages after skipping first M of them. Trivial pagination in a nutshell. */
  def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message]

  def firstMessages(chat: Chat, limit: Int): IndexedSeq[Message] =
    scrollMessages(chat, 0, limit)

  def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages before the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesBefore(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] =
    messagesBeforeImpl(chat, msgId, limit).ensuring(seq =>
      seq.nonEmpty && seq.size <= limit && seq.last.internalId == msgId)

  protected def messagesBeforeImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages after the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesAfter(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] =
    messagesAfterImpl(chat, msgId, limit).ensuring(seq =>
      seq.nonEmpty && seq.size <= limit && seq.head.internalId == msgId)

  protected def messagesAfterImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages between the given ones (inclusive).
   * Messages must be present, so the result would contain at least one element (if both are the same message).
   */
  final def messagesSlice(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message] =
    messagesSliceImpl(chat, msgId1, msgId2).ensuring(seq =>
      seq.nonEmpty && seq.head.internalId == msgId1 && seq.last.internalId == msgId2)

  protected def messagesSliceImpl(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message]

  /**
   * Count messages between the given ones (inclusive).
   * Messages must be present.
   */
  def messagesSliceLength(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): Int

  /** Returns N messages before and N at-or-after the given date */
  def messagesAroundDate(chat: Chat, date: DateTime, limit: Int): (IndexedSeq[Message], IndexedSeq[Message])

  def messageOption(chat: Chat, source_id: MessageSourceId): Option[Message]

  def messageOptionByInternalId(chat: Chat, internal_id: MessageInternalId): Option[Message]

  def isMutable: Boolean = this.isInstanceOf[MutableChatHistoryDao]

  override def close(): Unit = {}

  /** Whether given data path is the one loaded in this DAO */
  def isLoaded(storagePath: JFile): Boolean
}

trait MutableChatHistoryDao extends ChatHistoryDao {
  def insertDataset(ds: Dataset): Unit

  def renameDataset(dsUuid: PbUuid, newName: String): Dataset

  def deleteDataset(dsUuid: PbUuid): Unit

  /** Shift time of all timestamps in the dataset to accommodate timezone differences */
  def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit

  /** Insert a new user. It should not yet exist */
  def insertUser(user: User, isMyself: Boolean): Unit

  /** Sets the data (names and phone only) for a user with the given `id` and `dsUuid` to the given state */
  def updateUser(user: User): Unit

  /**
   * Merge absorbed user into base user, moving its personal chat messages into base user personal chat.
   */
  def mergeUsers(baseUser: User, absorbedUser: User): Unit

  /**
   * Insert a new chat.
   * It should have a proper DS UUID set, should not yet exist, and all users must already be inserted.
   * Content will be resolved based on the given dataset root and copied accordingly.
   */
  def insertChat(srcDsRoot: DatasetRoot, chat: Chat): Unit

  def deleteChat(chat: Chat): Unit

  /**
   * Insert a new message for the given chat.
   * Internal ID will be ignored.
   * Content will be resolved based on the given dataset root and copied accordingly.
   */
  def insertMessages(srcDsRoot: DatasetRoot, chat: Chat, msgs: Seq[Message]): Unit

  /** Don't do automatic backups on data changes until re-enabled */
  def disableBackups(): Unit

  /** Start doing backups automatically once again */
  def enableBackups(): Unit

  /** Create a backup, if enabled, otherwise do nothing */
  def backup(): Unit
}
