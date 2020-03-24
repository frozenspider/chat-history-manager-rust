package org.fs.chm.ui.swing.general

import java.awt.Color
import java.awt.Dimension
import java.awt.EventQueue
import java.awt.Font
import java.io.File

import scala.swing._
import scala.swing.Font.Style

import javax.swing.JComponent
import javax.swing.{ Box => JBox }
import javax.swing.filechooser.FileFilter
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

  def menuItem(title: String, enabled: Boolean = true)(action: => Any): MenuItem = {
    val e = enabled // To avoid name shadowing
    new MenuItem(new Action(title) { override def apply(): Unit = action }) {
      enabled = e
    }
  }

  def easyFileFilter(desc: String)(filter: File => Boolean): FileFilter =
    new FileFilter() {
      override def accept(f: File): Boolean = f.isDirectory || filter(f)
      override def getDescription:  String  = desc
    }

  def showWarning(msg: String): Unit = {
    log.warn(msg)
    Dialog.showMessage(title = "Warining", message = msg, messageType = Dialog.Message.Warning)
  }

  def showError(msg: String): Unit = {
    log.error(msg)
    Dialog.showMessage(title = "Error", message = msg, messageType = Dialog.Message.Error)
  }

  val ComfortableScrollSpeed: Int = 10

  def verticalListConstraint(p: GridBagPanel): p.Constraints =
    new p.Constraints {
      fill    = GridBagPanel.Fill.Horizontal
      weightx = 1
      gridx   = 0
    }

  def checkEdt() = {
    require(EventQueue.isDispatchThread, "Should be called from EDT")
  }

  object Colors {

    /** Light green */
    val AdditionBg: Color = Color.decode("#E4FFE0")

    /** Light yellow */
    val CombineBg: Color = Color.decode("#F8F8CE")

    /** Light red */
    val ConflictBg: Color = Color.decode("#F8CECE")

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
}