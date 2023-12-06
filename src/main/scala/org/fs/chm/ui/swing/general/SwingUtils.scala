package org.fs.chm.ui.swing.general

import java.awt.Color
import java.awt.Dimension
import java.awt.EventQueue
import java.awt.Font
import java.awt.image.BufferedImage
import java.io.File
import java.nio.file.Files

import javax.imageio.ImageIO

import scala.swing._
import scala.swing.Font.Style
import javax.swing.JComponent
import javax.swing.{Box => JBox}
import javax.swing.filechooser.FileFilter

import com.twelvemonkeys.imageio.stream.ByteArrayImageInputStream
import org.fs.chm.dao.Entities.DatasetRoot
import org.fs.chm.utility.LangUtils.RichString
import org.slf4s.Logging

object SwingUtils extends Logging {
  implicit class RichUIElement(el: UIElement) {
    def width            = el.size.width
    def width_=(w: Int)  = el.peer.setSize(w, el.size.height)
    def height           = el.size.height
    def height_=(h: Int) = el.peer.setSize(el.size.width, h)

    def preferredWidth            = el.preferredSize.width
    def preferredWidth_=(w: Int)  = el.preferredSize = new Dimension(w, el.preferredSize.height)
    def preferredHeight           = el.preferredSize.height
    def preferredHeight_=(h: Int) = el.preferredSize = new Dimension(el.preferredSize.width, h)

    def minimumWidth            = el.minimumSize.width
    def minimumWidth_=(w: Int)  = el.minimumSize = new Dimension(w, el.minimumSize.height)
    def minimumHeight           = el.minimumSize.height
    def minimumHeight_=(h: Int) = el.minimumSize = new Dimension(el.minimumSize.width, h)

    def maximumWidth            = el.maximumSize.width
    def maximumWidth_=(w: Int)  = el.maximumSize = new Dimension(w, el.maximumSize.height)
    def maximumHeight           = el.maximumSize.height
    def maximumHeight_=(h: Int) = el.maximumSize = new Dimension(el.maximumSize.width, h)

    def fontSize                    = el.font.getSize
    def fontSize_=(s: Int)          = el.font = new Font(el.font.getName, el.font.getStyle, s)
    def fontStyle                   = Style.values.find(_.id == el.font.getStyle).get
    def fontStyle_=(s: Style.Value) = el.font = new Font(el.font.getName, s.id, el.font.getSize)
  }

  implicit class RichComponent(el: Component) {
    def wrapInScrollpaneAndAdjustWidth(): ScrollPane = {
      val sp = wrapInScrollpane()
      el.preferredWidth += sp.verticalScrollBar.preferredWidth
      sp.preferredWidth = el.preferredWidth
      sp
    }

    def wrapInScrollpane(): ScrollPane = {
      new ScrollPane(el) {
        verticalScrollBar.unitIncrement = ComfortableScrollSpeed
        verticalScrollBarPolicy         = ScrollPane.BarPolicy.Always
        horizontalScrollBarPolicy       = ScrollPane.BarPolicy.Never
      }
    }
  }

  def menuItem(title: String, enabled: Boolean = true)(action: => Any): MenuItem = {
    val e = enabled // To avoid name shadowing
    new MenuItem(new Action(title) { override def apply(): Unit = action }) {
      enabled = e
    }
  }

  def resolveImage(pathOption: Option[String], datasetRoot: DatasetRoot): Option[BufferedImage] = {
    pathOption.map(_.toFile(datasetRoot)) match {
      case Some(imgFile) if imgFile.exists =>
        val imageBytes = Files.readAllBytes(imgFile.toPath)
        Option(ImageIO.read(new ByteArrayImageInputStream(imageBytes)))
      case _ => None
    }
  }

  def easyFileFilter(desc: String)(filter: File => Boolean): FileFilter =
    new FileFilter() {
      override def accept(f: File): Boolean = f.isDirectory || filter(f)
      override def getDescription:  String  = desc
    }

  def showWarning(msg: String): Unit = {
    Dialog.showMessage(title = "Warining", message = msg, messageType = Dialog.Message.Warning)
  }

  def showError(msg: String): Unit = {
    Dialog.showMessage(title = "Error", message = msg, messageType = Dialog.Message.Error)
  }

  val ComfortableScrollSpeed: Int = 10

  def verticalListConstraint(p: GridBagPanel): p.Constraints =
    new p.Constraints {
      fill    = GridBagPanel.Fill.Horizontal
      weightx = 1
      gridx   = 0
    }

  def isEdt():Boolean = EventQueue.isDispatchThread

  def checkEdt() = {
    require(
      isEdt(), {
        log.error("Should be called from EDT!", new IllegalArgumentException("Should be called from EDT!"))
        "Should be called from EDT! "
      }
    )
  }

  /** Execute a code block on EDT, returning the result */
  def onEdtReturning[A](cb: => A) = {
    require(!isEdt(), "Shouldn't be called from EDT!")
    val holder = new ResultHolder[A]
    Swing.onEDTWait {
      holder.res = cb
    }
    holder.res
  }

  object Colors {

    /** Light green */
    val AdditionBg: Color = Color.decode("#E4FFE0")

    /** Light yellow */
    val CombineBg: Color = Color.decode("#F8F8CE")

    /** Light red */
    val ConflictBg: Color = Color.decode("#FFF0F0")

    /** White */
    val NoBg: Color = Color.WHITE

    val CyclingStrings = Seq(
      // User
      "#6495ED", // CornflowerBlue
      // First interlocutor
      "#B22222", // FireBrick
      "#008000", // Green
      "#DAA520", // GoldenRod
      "#BA55D3", // MediumOrchid
      "#FF69B4", // HotPink
      "#808000", // Olive
      "#008080", // Teal
      "#9ACD32", // YellowGreen
      "#FF8C00", // DarkOrange
      "#00D0D0", // Cyan-ish
      "#BDB76B" // DarkKhaki
    )

    val Cycling = CyclingStrings map Color.decode

    def forIdx(i: Int): Color = {
      Cycling(i % Cycling.size)
    }

    def stringForIdx(i: Int): String = {
      CyclingStrings(i % CyclingStrings.size)
    }
  }

  class FillerComponent(val horizontal: Boolean, val dim: Int) extends Component {
    override lazy val peer =
      if (horizontal)
        JBox.createHorizontalStrut(dim).asInstanceOf[JComponent]
      else
        JBox.createVerticalStrut(dim).asInstanceOf[JComponent]
  }

  /** Represents a "sublist" of menu items between two separators */
  class EmbeddedMenu(_menu: Menu, before: Separator, after: Separator) {
    private val menu = new MenuWrapper(_menu)

    require(menu.indexOf(before) > 0)
    require(menu.indexOf(after) > 0)

    def clear(): Unit = {
      val i1      = menu.indexOf(before)
      val i2      = menu.indexOf(after)
      val between = i2 - i1 - 1
      menu.remove(i1 + 1, between)
    }

    def append(item: Menu): Unit = {
      val i = menu.indexOf(after)
      menu.insert(i, item)
    }
  }

  /** Scala Swing "contents" wrapper for menu can't be used to get stuff back, so... */
  private class MenuWrapper(m: Menu) {
    def indexOf(c: Component): Int = {
      m.peer.getPopupMenu.getComponentZOrder(c.peer)
    } ensuring (_ >= 0)

    def remove(from: Int, len: Int): Unit = {
      val pm   = m.peer.getPopupMenu
      val cmps = (from until (from + len)) map pm.getComponent
      cmps foreach pm.remove
    }

    def insert(i: Int, item: Component): Unit = {
      m.contents.insert(i, item)
    }
  }

  /** To work around having to initialize local vars */
  private class ResultHolder[A] {
    var res: A = _
  }
}
