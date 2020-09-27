package org.fs.chm.ui.swing

import java.io.File
import java.io.StringReader
import java.util.UUID

import javax.swing.text.Element
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.apache.commons.lang3.StringEscapeUtils
import org.fs.chm.dao.Message.Service
import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils
import org.fs.utility.Imports._

class MessagesService(htmlKit: HTMLEditorKit) {
  import org.fs.chm.ui.swing.MessagesService._

  def createStubDoc: MessageDocument = {
    // TODO: How to limit width and do word wraps?
    //       width, max-width acts weird,
    //       word-wrap, word-break, overflow-wrap are ignored
    val doc     = htmlKit.createDefaultDocument().asInstanceOf[HTMLDocument]
    val content = """<div id="messages"></div>"""
    htmlKit.read(new StringReader(content), doc, 0)
    val css = """|body {
                 |  font-family: arial,sans-serif;
                 |}
                 |.title {
                 |  padding-left: 5px;
                 |  padding-bottom: 5px;
                 |  font-size: 105%;
                 |}
                 |.forwarded-from {
                 |  padding-left: 5px;
                 |  padding-bottom: 5px;
                 |  font-size: 105%;
                 |  color: #909090;
                 |}
                 |.title-name {
                 |  font-weight: bold;
                 |}
                 |blockquote {
                 |  border-left: 1px solid #ccc;
                 |  margin: 5px 10px;
                 |  padding: 5px 10px;
                 |}
                 |.system-message {
                 |  border: 1px solid #A0A0A0;
                 |  margin: 5px 10px;
                 |  padding: 5px 10px;
                 |}""".stripMargin
    doc.getStyleSheet.addRule(css)
    doc.getStyleSheet.addRule("W3C_LENGTH_UNITS_ENABLE")
    val msgEl = doc.getElement("messages")
    MessageDocument(doc, msgEl)
  }

  lazy val pleaseWaitDoc: HTMLDocument = {
    val md = createStubDoc
    md.insert("<div><h1>Please wait...</h1></div>", MessageInsertPosition.Leading)
    md.doc
  }

  //
  // Renderers and helpers
  //

  def renderMessageHtml(cc: ChatWithDao, m: Message, isQuote: Boolean = false): String = {
    val msgHtml: String = m match {
      case m: Message.Regular =>
        val textHtmlOption = renderTextOption(m.textOption)
        val contentHtmlOption = m.contentOption map { ct =>
          s"""<div class="content">${ContentHtmlRenderer.render(cc, ct)}</div>"""
        }
        val fwdFromHtmlOption  = m.forwardFromNameOption map (n => renderFwdFrom(cc, n))
        val replySrcHtmlOption = if (isQuote) None else m.replyToMessageIdOption map (id => renderSourceMessage(cc, id))
        Seq(fwdFromHtmlOption, replySrcHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString
      case sm: Message.Service =>
        ServiceMessageHtmlRenderer.render(cc, sm)
    }
    val titleNameHtml = renderTitleName(cc, Some(m.fromId), None)
    val titleHtml =
      s"""$titleNameHtml (${m.time.toString("yyyy-MM-dd HH:mm")})"""
    val sourceIdAttr = m.sourceIdOption map (id => s"""message_source_id="$id"""") getOrElse ""
    s"""
       |<div class="message" ${sourceIdAttr}>
       |   <div class="title">${titleHtml}</div>
       |   <div class="body">${msgHtml}</div>
       |</div>
       |${if (!isQuote) "<p>" else ""}
    """.stripMargin // TODO: Remove <p>
  }

  private def renderTitleName(cc: ChatWithDao, idOption: Option[Long], nameOption: Option[String]): String = {
    val intl = cc.dao.interlocutors(cc.chat)
    val idx = {
      val idx1 = idOption map (id => intl indexWhere (_.id == id)) getOrElse -1
      val idx2 = intl indexWhere (u => nameOption contains u.prettyName)
      if (idx1 != -1) idx1 else idx2
    }
    val color = if (idx >= 0) Colors.stringForIdx(idx) else "#000000"
    val resolvedName = nameOption getOrElse {
      EntityUtils.getOrUnnamed(idOption flatMap (id => intl.find(_.id == id)) map (_.prettyName))
    }

    s"""<span class="title-name" style="color: $color;">${resolvedName}</span>"""
  }

  private def renderTextOption(textOption: Option[RichText]): Option[String] =
    textOption map { rt =>
      s"""<div class="text">${RichTextHtmlRenderer.render(rt)}</div>"""
    }

  private def renderFwdFrom(cc: ChatWithDao, fromName: String): String = {
    val titleNameHtml = renderTitleName(cc, None, Some(fromName))
    s"""<div class="forwarded-from">Forwarded from $titleNameHtml</div>"""
  }

  private def renderSourceMessage(cc: ChatWithDao, srcId: Message.SourceId): String = {
    val m2Option = cc.dao.messageOption(cc.chat, srcId)
    val html = m2Option match {
      case None     => "[Deleted message]"
      case Some(m2) => renderMessageHtml(cc, m2, true)
    }
    s"""<blockquote>$html</blockquote> """
  }

  private def renderPossiblyMissingContent(
      fileOption: Option[File],
      kindPrettyName: String,
      dao: ChatHistoryDao,
      dsUuid: UUID
  )(renderContentFromFile: File => String): String = {
    fileOption match {
      case Some(file) if file.exists => renderContentFromFile(file)
      case Some(file)                => s"[$kindPrettyName not found]"
      case None                      => s"[$kindPrettyName not downloaded]"
    }
  }

  private def renderImage(fileOption: Option[File],
                          widthOption: Option[Int],
                          heightOption: Option[Int],
                          altTextOption: Option[String],
                          imagePrettyType: String,
                          dao: ChatHistoryDao,
                          dsUuid: UUID): String = {
    renderPossiblyMissingContent(fileOption, imagePrettyType, dao, dsUuid)(file => {
      val srcAttr    = Some(s"""src="${fileToLocalUriString(file)}"""")
      val widthAttr  = widthOption map (w => s"""width="${w / 2}"""")
      val heightAttr = heightOption map (h => s"""height="${h / 2}"""")
      val altAttr    = altTextOption map (e => s"""alt="$e"""")
      "<img " + Seq(srcAttr, widthAttr, heightAttr, altAttr).yieldDefined.mkString(" ") + "/>"
    })
  }

  private def fileToLocalUriString(file: File): String = {
    "file:///" + file.getCanonicalPath.replace("\\", "/")
  }

  object ServiceMessageHtmlRenderer {
    def render(cc: ChatWithDao, sm: Message.Service): String = {
      val textHtmlOption = renderTextOption(sm.textOption)
      val content = sm match {
        case sm: Message.Service.PhoneCall         => renderPhoneCall(sm)
        case sm: Message.Service.PinMessage        => "Pinned message" + renderSourceMessage(cc, sm.messageId)
        case sm: Message.Service.MembershipChange  => renderMembershipChangeMessage(cc, sm)
        case sm: Message.Service.ClearHistory      => s"History cleared"
        case sm: Message.Service.Group.EditTitle   => renderEditTitleMessage(sm, cc.dao, cc.dsUuid)
        case sm: Message.Service.Group.EditPhoto   => renderEditPhotoMessage(sm, cc.dao, cc.dsUuid)
        case sm: Message.Service.Group.MigrateFrom => renderMigratedFrom(sm)
        case sm: Message.Service.Group.MigrateTo   => "Migrated to another group"
      }
      Seq(Some(s"""<div class="system-message">$content</div>"""), textHtmlOption).yieldDefined.mkString
    }

    private def renderPhoneCall(sm: Service.PhoneCall) = {
      Seq(
        Some("Phone call"),
        sm.durationSecOption map (d => s"($d sec)"),
        sm.discardReasonOption map (r => s"($r)")
      ).yieldDefined.mkString(" ")
    }

    private def renderEditTitleMessage(sm: Service.Group.EditTitle, dao: ChatHistoryDao, dsUuid: UUID) = {
      s"Changed group title to <b>${sm.title}</b>"
    }

    private def renderEditPhotoMessage(sm: Service.Group.EditPhoto, dao: ChatHistoryDao, dsUuid: UUID) = {
      val image = renderImage(sm.pathOption, sm.widthOption, sm.heightOption, None, "Photo", dao, dsUuid)
      s"Changed group photo<br>$image"
    }

    private def renderMigratedFrom(sm: Service.Group.MigrateFrom) = {
      ("Migrated" + sm.titleOption.map(s => s" from $s").getOrElse("")).trim
    }

    private def renderMembershipChangeMessage(cc: ChatWithDao, sm: Service.MembershipChange) = {
      val members = sm.members
        .map(name => renderTitleName(cc, None, Some(name)))
        .mkString("<ul><li>", "</li><li>", "</li></ul>")
      val content = sm match {
        case sm: Service.Group.Create        => s"Created group <b>${sm.title}</b>"
        case sm: Service.Group.InviteMembers => s"Invited"
        case sm: Service.Group.RemoveMembers => s"Removed"
      }
      s"$content$members"
    }
  }

  object RichTextHtmlRenderer {
    def render(rt: RichText): String = {
      val components = for (rtel <- rt.components) yield {
        renderComponent(rtel)
      }
      val hiddenLinkOption = rt.components collectFirst { case l: RichText.Link if l.hidden => l }
      val link = hiddenLinkOption map { l =>
        "<p> &gt; Link: " + renderComponent(l.copy(text = l.href, hidden = false))
      } getOrElse ""
      components.mkString + link
    }

    private def renderComponent(rtel: RichText.Element): String = {
      rtel match {
        case RichText.Plain(text)          => text replace ("\n", "<br>")
        case RichText.Bold(text)           => s"<b>${text replace ("\n", "<br>")}</b>"
        case RichText.Italic(text)         => s"<i>${text replace ("\n", "<br>")}</i>"
        case RichText.Underline(text)      => s"<u>${text replace ("\n", "<br>")}</u>"
        case RichText.Strikethrough(text)  => s"<strike>${text replace ("\n", "<br>")}</strike>"
        case link: RichText.Link           => renderLink(link)
        case RichText.PrefmtBlock(text, _) => s"""<pre>${text}</pre>"""
        case RichText.PrefmtInline(text)   => s"""<code>${text}</code>"""
      }
    }

    private def renderLink(rtel: RichText.Link) = {
      if (rtel.hidden) {
        rtel.plainSearchableString
      } else {
        // Space in the end is needed if link is followed by text
        val text = if (rtel.text.nonEmpty) rtel.text else rtel.href
        s"""<a href="${rtel.href}">${text}</a> """
      }
    }
  }

  object ContentHtmlRenderer {
    def render(cc: ChatWithDao, ct: Content): String = {
      ct match {
        case ct: Content.Sticker       => renderSticker(ct, cc.dao, cc.dsUuid)
        case ct: Content.Photo         => renderPhoto(ct, cc.dao, cc.dsUuid)
        case ct: Content.VoiceMsg      => renderVoiceMsg(ct, cc.dao, cc.dsUuid)
        case ct: Content.VideoMsg      => renderVideoMsg(ct, cc.dao, cc.dsUuid)
        case ct: Content.Animation     => renderAnimation(ct, cc.dao, cc.dsUuid)
        case ct: Content.File          => renderFile(ct, cc.dao, cc.dsUuid)
        case ct: Content.Location      => renderLocation(ct)
        case ct: Content.Poll          => renderPoll(ct)
        case ct: Content.SharedContact => renderSharedContact(cc, ct)
      }
    }

    private def renderVoiceMsg(ct: Content.VoiceMsg, dao: ChatHistoryDao, dsUuid: UUID) = {
      renderPossiblyMissingContent(ct.pathOption, "Voice message", dao, dsUuid)(file => {
        val mimeTypeOption = ct.mimeTypeOption map (mt => s"""type="$mt"""")
        val durationOption = ct.durationSecOption map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src=${fileToLocalUriString(file)}" ${mimeTypeOption getOrElse ""}>
           |</audio>""".stripMargin
      })
    }

    private def renderVideoMsg(ct: Content.VideoMsg, dao: ChatHistoryDao, dsUuid: UUID): String = {
      // TODO
      "[Video messages not supported yet]"
    }

    private def renderAnimation(ct: Content.Animation, dao: ChatHistoryDao, dsUuid: UUID): String = {
      // TODO
      "[Animations not supported yet]"
    }

    private def renderFile(ct: Content.File, dao: ChatHistoryDao, dsUuid: UUID): String = {
      // TODO
      "[Files not supported yet]"
    }

    def renderSticker(st: Content.Sticker, dao: ChatHistoryDao, dsUuid: UUID): String = {
      val pathOption = st.pathOption orElse st.thumbnailPathOption
      renderImage(pathOption, st.widthOption, st.heightOption, st.emojiOption, "Sticker", dao, dsUuid)
    }

    def renderPhoto(ct: Content.Photo, dao: ChatHistoryDao, dsUuid: UUID): String = {
      renderImage(ct.pathOption, Some(ct.width), Some(ct.height), None, "Photo", dao, dsUuid)
    }

    def renderLocation(ct: Content.Location): String = {
      val liveString = ct.durationSecOption map (s => s"(live for $s s)") getOrElse ""
      s"""<blockquote><i>Location:</i> <b>${ct.lat}, ${ct.lon}</b> $liveString<br></blockquote>"""
    }

    def renderPoll(ct: Content.Poll): String = {
      s"""<blockquote><i>Poll:</i> ${ct.question}</blockquote>"""
    }

    def renderSharedContact(cc: ChatWithDao, ct: Content.SharedContact): String = {
      val name  = renderTitleName(cc, None, Some(ct.prettyName))
      val phone = ct.phoneNumberOption map (pn => s"(phone: $pn)") getOrElse "(no phone number)"
      s"""<blockquote><i>Shared contact:</i> $name $phone</blockquote>"""
    }
  }
}

object MessagesService {
  case class MessageDocument(
      doc: HTMLDocument,
      contentParent: Element
  ) {

    // We need to account for "p-implied" parasitic element...
    private lazy val pImplied = contentParent.getElement(0)

    def removeFirst(): Unit = {
      doc.removeElement(contentParent.getElement(1))
    }

    def insert(htmlToInsert: String, pos: MessageInsertPosition): Unit = {
      pos match {
        case MessageInsertPosition.Leading  => doc.insertAfterEnd(pImplied, htmlToInsert)
        case MessageInsertPosition.Trailing => doc.insertBeforeEnd(contentParent, htmlToInsert)
      }
    }

    def clear(): Unit = {
      doc.remove(0, doc.getLength)
    }
  }

  sealed trait MessageInsertPosition
  object MessageInsertPosition {
    case object Leading  extends MessageInsertPosition
    case object Trailing extends MessageInsertPosition
  }

}
