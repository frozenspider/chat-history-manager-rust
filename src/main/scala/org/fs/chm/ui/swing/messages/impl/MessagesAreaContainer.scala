package org.fs.chm.ui.swing.messages.impl

import scala.swing._
import javax.swing.text.DefaultCaret
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.ChatWithDetails
import org.fs.chm.dao.Message
import org.fs.chm.protobuf.Content
import org.fs.chm.protobuf.ContentLocation
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.messages.MessagesRenderingComponent
import org.fs.chm.ui.swing.messages.impl.MessagesService._

class MessagesAreaContainer(htmlKit: HTMLEditorKit) extends MessagesRenderingComponent[MessageDocument] {
  // TODO: This should really be private, but we're hacking into it for SelectMergeMessagesDialog
  val msgService = new MessagesService(htmlKit)

  //
  // Fields
  //

  val textPane: TextPane = {
    val ta = new TextPane()
    ta.peer.setEditorKit(htmlKit)
    ta.peer.setEditable(false)
    ta.peer.setSize(new Dimension(10, 10))
    ta
  }

  val scrollPane: ScrollPane = {
    new ScrollPane(textPane)
  }

  protected val viewport = scrollPane.peer.getViewport

  private val caret = textPane.peer.getCaret.asInstanceOf[DefaultCaret]

  private var viewPosSizeOption: Option[(Point, Dimension)] = None
  private var prepended:         Boolean                    = false
  private var appended:          Boolean                    = false

  private var _documentOption: Option[MessageDocument] = None

  // Workaround for https://github.com/scala/bug/issues/1938: Can't call super.x if x is a lazy val
  private lazy val _component: Component = new BorderPanel {
    import scala.swing.BorderPanel.Position._
    layout(scrollPane) = Center
  }

  override def component: Component = _component

  //
  // Methods
  //

  override def renderPleaseWait(): Unit = {
    checkEdt()
    document = msgService.pleaseWaitDoc
  }

  override def render(
      dao: ChatHistoryDao,
      cwd: ChatWithDetails,
      msgs: IndexedSeq[Message],
      beginReached: Boolean,
      showTop: Boolean
  ): MessageDocument = {
    checkEdt()
    val md = msgService.createStubDoc
    val sb = new StringBuilder
    if (beginReached) {
      sb.append(msgService.nothingNewerHtml)
    }
    for (m <- msgs) {
      sb.append(msgService.renderMessageHtml(dao, cwd, m))
    }
    md.insert(sb.toString, MessageInsertPosition.Leading)
    document = md
    if (showTop) {
      scrollToBegin()
    } else {
      scrollToEnd()
    }
    document
  }

  override def render(md: MessageDocument, showTop: Boolean): Unit = {
    checkEdt()
    document = md
    if (showTop) {
      scrollToBegin()
    } else {
      scrollToEnd()
    }
  }

  override def prependLoading(): MessageDocument = {
    checkEdt()
    document.insert(msgService.loadingHtml, MessageInsertPosition.Leading)
    document
  }

  override def appendLoading(): MessageDocument = {
    checkEdt()
    document.insert(msgService.loadingHtml, MessageInsertPosition.Trailing)
    document
  }

  override def prepend(
      dao: ChatHistoryDao,
      cwd: ChatWithDetails,
      msgs: IndexedSeq[Message],
      beginReached: Boolean
  ): MessageDocument = {
    checkEdt()
    require(viewPosSizeOption.isEmpty || !appended, "Prepend and append can't happen in a single update!")
    prepended = true
    // TODO: Prevent flickering
    // TODO: Preserve selection
    val sb = new StringBuilder
    if (beginReached) {
      sb.append(msgService.nothingNewerHtml)
    }
    for (m <- msgs) {
      sb.append(msgService.renderMessageHtml(dao, cwd, m))
    }
    document.removeLoading(true)
    document.insert(sb.toString, MessageInsertPosition.Leading)
    document
  }

  override def append(
      dao: ChatHistoryDao,
      cwd: ChatWithDetails,
      msgs: IndexedSeq[Message],
      endReached: Boolean
  ): MessageDocument = {
    checkEdt()
    require(viewPosSizeOption.isEmpty || !prepended, "Prepend and append can't happen in a single update!")
    appended = true
    // TODO: Prevent flickering
    val sb = new StringBuilder
    for (m <- msgs) {
      sb.append(msgService.renderMessageHtml(dao, cwd, m))
    }
    //    if (endReached) {
    //      sb.append(msgService.nothingNewerHtml)
    //    }
    document.removeLoading(false)
    document.insert(sb.toString, MessageInsertPosition.Trailing)
    document
  }

  override def updateStarted(): Unit = {
    scrollPane.validate()
    viewPosSizeOption = Some(currentViewPosSize)
    prepended        = false
    appended         = false
    // Disable message caret updates while messages are loading to avoid scrolling
    caret.setUpdatePolicy(DefaultCaret.NEVER_UPDATE)
  }

  override def updateFinished(): Unit = {
    require(viewPosSizeOption.isDefined, "updateStarted() wasn't called?")
    assert(!prepended || !appended)
    // TODO: Do it right after prepend?
    if (prepended) {
      val Some((pos1, size1)) = viewPosSizeOption
      val (_, size2)          = currentViewPosSize
      val heightDiff          = size2.height - size1.height
      show(pos1.x, pos1.y + heightDiff)
    }
    viewPosSizeOption = None
    prepended        = false
    appended         = false
    caret.setUpdatePolicy(DefaultCaret.UPDATE_WHEN_ON_EDT)
  }

  //
  // Helpers
  //

  protected def onDocumentChange(): Unit = {}

  protected def documentOption = _documentOption

  protected def document = _documentOption.get

  private def document_=(md: MessageDocument): Unit = {
    if (!_documentOption.contains(md)) {
      _documentOption = Some(md)
      textPane.peer.setStyledDocument(md.doc)
      onDocumentChange()
    }
  }

  private def currentViewPosSize = {
    (viewport.getViewPosition, viewport.getViewSize)
  }

  private def scrollToBegin(): Unit = {
    show(0, 0)
  }

  private def scrollToEnd(): Unit = {
    // FIXME: Doesn't always work!
    show(0, textPane.preferredHeight)
  }

  private def show(x: Int, y: Int): Unit = {
    viewport.setViewPosition(new Point(x, y))
  }
}

object MessagesAreaContainer {
  type MessageDocument = MessagesService.MessageDocument

  def main(args: Array[String]): Unit = {
    import java.awt.Desktop
    import java.nio.file.Files
    import java.util.UUID

    import scala.collection.immutable.ListMap

    import org.fs.chm.dao._
    import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
    import org.fs.chm.utility.TestUtils._

    val dao = {
      val ds = Dataset(
        uuid       = UUID.randomUUID(),
        alias      = "Dataset",
        sourceType = "test source"
      )
      val users = (1 to 2) map (createUser(ds.uuid, _))
      val msgs = IndexedSeq(
        Message.Service.Group.Call(
          internalId             = Message.NoInternalId,
          sourceIdOption         = Some(1L.asInstanceOf[Message.SourceId]),
          time                   = baseDate.plusMinutes(1),
          fromId                 = users.head.id,
          textOption             = Some(RichText(Seq(RichText.Plain(s"Join the call!")))),
          members                = users map (_.prettyName)
        ),
        Message.Regular(
          internalId             = Message.NoInternalId,
          sourceIdOption         = Some(2L.asInstanceOf[Message.SourceId]),
          time                   = baseDate.plusMinutes(2),
          editTimeOption         = Some(baseDate.plusMinutes(2).plusSeconds(5)),
          fromId                 = users.last.id,
          forwardFromNameOption  = Some("u" + users.head.id),
          replyToMessageIdOption = Some(1L.asInstanceOf[Message.SourceId]),
          textOption             = Some(RichText(Seq(RichText.Plain(s"Sharing my location")))),
          contentOption = Some(
            Content(Content.Val.Location(ContentLocation(
              title       = Some("My Brand New Place"),
              address     = Some("1 Caesar Ave"),
              latStr      = "11.11111",
              lonStr      = "22.22222",
              durationSec = Some(5)
            ))))
        )
      )

      val chat = createPersonalChat(ds.uuid, 1, users.head, users.map(_.id), msgs.size)
      val dataPathRoot = Files.createTempDirectory(null).toFile
      dataPathRoot.deleteOnExit()
      new EagerChatHistoryDao(
        name               = "Dao",
        _dataRootFile      = dataPathRoot,
        dataset            = ds,
        myself1            = users.head,
        users1             = users,
        _chatsWithMessages = ListMap(chat -> msgs)
      ) with EagerMutableDaoTrait
    }

    val (_, _, cwd, msgs) = getSimpleDaoEntities(dao)

    Swing.onEDTWait {
      val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
      val htmlKit = new ExtendedHtmlEditorKit(desktopOption)
      val container = new MessagesAreaContainer(htmlKit)
      container.render(dao, cwd, msgs.toIndexedSeq, false, false)
      container.component
      Dialog.showMessage(
        title       = classOf[MessagesAreaContainer].getSimpleName,
        message     = container.component.peer,
        messageType = Dialog.Message.Plain
      )
    }
  }
}
