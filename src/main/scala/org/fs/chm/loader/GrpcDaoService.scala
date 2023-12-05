package org.fs.chm.loader

import java.io.File

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.language.implicitConversions

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities.MessageInternalId
import org.fs.chm.dao.Entities.MessageSourceId
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.Logging

/** Acts as server API for a local history DAO */
class GrpcDaoService(doLoadH2: File => ChatHistoryDao)
  extends HistoryDaoServiceGrpc.HistoryDaoService
    with Logging {
  implicit private val ec: ExecutionContextExecutor = ExecutionContext.global

  // Only hash map access is syncronized, DAO themselves should be thread-safe already
  private val Lock = new Object

  private var DaoMap: Map[String, ChatHistoryDao] = Map.empty

  val loader: GrpcHistoryLoaderService = new GrpcHistoryLoaderService()

  def getDao(path: File): Option[ChatHistoryDao] = {
    Lock.synchronized {
      DaoMap.values.find(_.storagePath == path)
    }
  }

  /** For testing usage only! */
  def addDao(key: String, dao: ChatHistoryDao): Unit = {
    Lock.synchronized {
      DaoMap = DaoMap + (key -> dao)
    }
  }

  override def saveAs(request: SaveAsRequest): Future[LoadedFile] = {
    throw new UnsupportedOperationException("GrpcDaoService.saveAs is not supported!")
  }

  override def name(request: NameRequest): Future[NameResponse] =
    withDao(request, request.key)(dao => NameResponse(dao.name))

  override def storagePath(request: StoragePathRequest): Future[StoragePathResponse] =
    withDao(request, request.key)(dao => StoragePathResponse(dao.storagePath.getAbsolutePath))

  override def datasets(request: DatasetsRequest): Future[DatasetsResponse] =
    withDao(request, request.key)(dao => DatasetsResponse(dao.datasets))

  override def datasetRoot(request: DatasetRootRequest): Future[DatasetRootResponse] =
    withDao(request, request.key)(dao => DatasetRootResponse(dao.datasetRoot(request.dsUuid).getAbsolutePath))

  override def myself(request: MyselfRequest): Future[MyselfResponse] =
    withDao(request, request.key)(dao => MyselfResponse(dao.myself(request.dsUuid)))

  override def users(request: UsersRequest): Future[UsersResponse] =
    withDao(request, request.key)(dao => UsersResponse(dao.users(request.dsUuid)))

  override def chats(request: ChatsRequest): Future[ChatsResponse] =
    withDao(request, request.key)(dao => ChatsResponse(
      dao.chats(request.dsUuid)
        .map(cwd => ChatWithDetailsPB(cwd.chat, cwd.lastMsgOption, cwd.members))))

  override def scrollMessages(request: ScrollMessagesRequest): Future[MessagesResponse] =
    withDao(request, request.key)(dao => MessagesResponse(
      dao.scrollMessages(request.chat, request.offset.toInt, request.limit.toInt)))

  override def lastMessages(request: LastMessagesRequest): Future[MessagesResponse] =
    withDao(request, request.key)(dao => MessagesResponse(
      dao.lastMessages(request.chat, request.limit.toInt)))

  /** Return N messages before the given one (exclusive). Message must be present. */
  override def messagesBefore(request: MessagesBeforeRequest): Future[MessagesResponse] =
    withDao(request, request.key)(dao => MessagesResponse(
      dao.messagesBefore(request.chat, request.messageInternalId, request.limit.toInt).dropRight(1)))

  /** Return N messages after the given one (exclusive). Message must be present. */
  override def messagesAfter(request: MessagesAfterRequest): Future[MessagesResponse] =
    withDao(request, request.key)(dao => MessagesResponse(
      dao.messagesAfter(request.chat, request.messageInternalId, request.limit.toInt).tail))

  /** Return N messages between the given ones (inclusive). Messages must be present. */
  override def messagesSlice(request: MessagesSliceRequest): Future[MessagesResponse] =
    withDao(request, request.key)(dao => MessagesResponse(
      dao.messagesSlice(request.chat, request.messageInternalId1, request.messageInternalId2)))

  /** Count messages between the given ones (inclusive). Messages must be present. */
  override def messagesSliceLen(request: MessagesSliceRequest): Future[CountMessagesResponse] =
    withDao(request, request.key)(dao => CountMessagesResponse(
      dao.messagesSliceLength(request.chat, request.messageInternalId1, request.messageInternalId2)))

  override def messageOption(request: MessageOptionRequest): Future[MessageOptionResponse] =
    withDao(request, request.key)(dao => MessageOptionResponse(
      dao.messageOption(request.chat, request.sourceId.asInstanceOf[MessageSourceId])))

  override def messageOptionByInternalId(request: MessageOptionByInternalIdRequest): Future[MessageOptionResponse] =
    withDao(request, request.key)(dao => MessageOptionResponse(
      dao.messageOptionByInternalId(request.chat, request.internalId)))

  /** Whether given data path is the one loaded in this DAO. */
  override def isLoaded(request: IsLoadedRequest): Future[IsLoadedResponse] =
    withDao(request, request.key)(dao => IsLoadedResponse(
      dao.isLoaded(new File(request.storagePath))))

  //
  // Mutable DAO endpoints
  //

  override def backup(request: BackupRequest): Future[Empty] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].backup()
      Empty()
    })
  }

  override def updateDataset(request: UpdateDatasetRequest): Future[UpdateDatasetResponse] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].renameDataset(request.dataset.uuid, request.dataset.alias)
      UpdateDatasetResponse(dao.datasets.find(_.uuid == request.dataset.uuid).get)
    })
  }

  override def deleteDataset(request: DeleteDatasetRequest): Future[Empty] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].deleteDataset(request.uuid)
      Empty()
    })
  }

  /** Shift time of all timestamps in the dataset to accommodate timezone differences */
  override def shiftDatasetTime(request: ShiftDatasetTimeRequest): Future[Empty] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].shiftDatasetTime(request.uuid, request.hoursShift)
      Empty()
    })
  }

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].updateUser(request.user)
      UpdateUserResponse(dao.users(request.user.dsUuid).find(_.id == request.user.id).get)
    })
  }

  override def deleteChat(request: DeleteChatRequest): Future[Empty] = {
    withDao(request, request.key)(dao => {
      require(dao.isMutable, "Dao is immutable!")
      dao.asInstanceOf[MutableChatHistoryDao].deleteChat(request.chat)
      Empty()
    })
  }

  //
  // Helpers
  //

  private implicit def toInternal(l: Long): MessageInternalId = l.asInstanceOf[MessageInternalId]

  private def withDao[T](req: Object, key: String)(f: ChatHistoryDao => T): Future[T] = {
    Future {
      loggingRequest(req) {
        val dao = Lock.synchronized {
          DaoMap(key)
        }
        f(dao)
      }
    }
  }

  private def loggingRequest[T](req: Object)(logic: => T): T = {
    log.debug(s">>> Request:  ${req.toString.take(150)}")
    try {
      val res = logic
      log.debug(s"<<< Response: ${res.toString.linesIterator.next().take(150)}")
      res
    } catch {
      case th: Throwable =>
        log.debug(s"<<< Failure", th)
        throw th
    }
  }

  class GrpcHistoryLoaderService extends HistoryLoaderServiceGrpc.HistoryLoaderService {
    /** Parse/open a history file and return its DAO handle */
    override def load(request: LoadRequest): Future[LoadResponse] = {
      val file = new File(request.path)
      Future {
        loggingRequest(request) {
          val dao = doLoadH2(file)
          Lock.synchronized {
            DaoMap = DaoMap + (request.key -> dao)
          }
          LoadResponse(name = dao.name)
        }
      }
    }

    override def getLoadedFiles(request: Empty): Future[GetLoadedFilesResponse] = {
      Future.successful(loggingRequest(request) {
        val loaded = Lock.synchronized {
          for {
            (key, dao) <- DaoMap
          } yield LoadedFile(key, dao.name)
        }
        GetLoadedFilesResponse(loaded.toSeq)
      })
    }

    override def close(request: CloseRequest): Future[CloseResponse] = {
      Future {
        val key = request.key
        val dao = Lock.synchronized {
          val dao = DaoMap(key)
          DaoMap = DaoMap - key
          dao
        }
        dao.close();
        CloseResponse(success = true)
      }
    }
  }
}
