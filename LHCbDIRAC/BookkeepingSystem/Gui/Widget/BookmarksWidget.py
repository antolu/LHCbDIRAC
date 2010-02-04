########################################################################
# $Id: $
########################################################################

from PyQt4.QtCore  import *
from PyQt4.QtGui   import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Bookmarks_ui              import Ui_BookmarksWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                       import Item
import DIRAC
__RCSID__ = " $"


#############################################################################  
class BookmarksWidget(QWidget, Ui_BookmarksWidget):
  
  #############################################################################  
  def __init__(self, parent = None):
    """
    Constructor
    
    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    self.setupUi(self)
    
  
    picturesPath = DIRAC.rootPath+'/LHCbDIRAC/BookkeepingSystem/Gui/Widget'
    closeIcon = QIcon(picturesPath+"/images/close.png")
    self.closeButton.setIcon(closeIcon)
    
    addIcon = QIcon(picturesPath+"/images/add.png")
    self.addButton.setIcon(addIcon)
    
    removeIcon = QIcon(picturesPath+"/images/remove.png")
    self.removeButton.setIcon(removeIcon)
    
    self.__controler = None
    self.connect(self.closeButton, SIGNAL("clicked()"), self.hidewidget)
    
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  def hidewidget(self):
    self.hide()
  