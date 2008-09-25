########################################################################
# $Id: TreeWidget.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

from PyQt4.QtCore  import *
from PyQt4.QtGui   import *

from DIRAC.BookkeepingSystem.Gui.Widget.TreeWidget_ui    import Ui_TreeWidget
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerTree import ControlerTree
from DIRAC.BookkeepingSystem.Gui.Widget.InfoDialog       import InfoDialog
from DIRAC.BookkeepingSystem.Gui.Widget.FileDialog       import FileDialog

from DIRAC.BookkeepingSystem.Gui.Basic.Item              import Item

__RCSID__ = "$Id: TreeWidget.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

#############################################################################  
class TreeWidget(QWidget, Ui_TreeWidget):
  
  #############################################################################  
  def __init__(self, parent = None):
    """
    Constructor
    
    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    self.__parent = parent.parentWidget()
    self.setupUi(self)
    self.configNameRadioButton.setChecked(True)
    
    self.__controler = ControlerTree(self, parent.parentWidget().getControler())
    self.connect(self.configNameRadioButton, SIGNAL("clicked()"), self.__controler.configButton)
    self.connect(self.radioButton_2, SIGNAL("clicked()"), self.__controler.eventTypeButton)
    self.tree.setupControler()
    
    self.__dialog = InfoDialog(self)
    self.__controler.addChild('InfoDialog',self.__dialog.getControler())
    
    self.__fileDialog = FileDialog(self)
    self.__controler.addChild('FileDialog',self.__fileDialog.getControler())
    #self.__dialog.show()
    #self.__dialog.hide()
    '''
    self.__bkClient = LHCB_BKKDBClient()
    item = self.__bkClient.get()
    items=Item(item,None)
    path = item['Value']['fullpath']
    for entity in self.__bkClient.list(path):
      childItem = Item(entity,items)
      items.addItem(childItem)
    self.tree.showTree(items)
    '''
  #############################################################################  
  def getTree(self):
    return self.tree
  
  #############################################################################  
  def headerItem(self):
    return self.tree.headerItem()
  
  #############################################################################  
  def getControler(self):
    return self.__controler
  