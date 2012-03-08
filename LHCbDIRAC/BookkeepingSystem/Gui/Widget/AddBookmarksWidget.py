########################################################################
# $HeadURL:  $
########################################################################

from PyQt4.QtCore  import *
from PyQt4.QtGui   import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_AddBookmarks           import Ui_AddBookmarks
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAddBookmarks  import ControlerAddBookmarks
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                       import Item

__RCSID__ = " $"


#############################################################################
class AddBookmarksWidget(QDialog, Ui_AddBookmarks):

  #############################################################################
  def __init__(self, parent = None):
    """
    Constructor

    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    self.setupUi(self)

    self.__controler = ControlerAddBookmarks(self, parent.getControler())

    self.connect(self.okButton, SIGNAL("clicked()"), self.__controler.okButton)
    self.connect(self.cancelButton, SIGNAL("clicked()"), self.__controler.cancelButton)
    self.__model = None


  #############################################################################
  def getControler(self):
    return self.__controler

  #############################################################################
  def getTitle(self):
    return str(self.titlelineEdit.text())

  #############################################################################
  def getPath(self):
    return str(self.pathlineEdit.text())

  #############################################################################
  def setTitle(self, str):
    self.titlelineEdit.setText(str)

  #############################################################################
  def setPath(self, str):
    self.pathlineEdit.setText(str)

  #############################################################################
  def waitCursor(self):
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    self.setCursor(Qt.ArrowCursor)
