########################################################################
# $Id: MainWidget.py,v 1.4 2008/11/03 11:28:01 zmathe Exp $
########################################################################


from PyQt4.QtCore import *
from PyQt4.QtGui  import *

from DIRAC.BookkeepingSystem.Gui.Widget.MainWidget_ui                 import Ui_MainWidget
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerMain              import ControlerMain
from DIRAC.BookkeepingSystem.Gui.Basic.Item                           import Item
from DIRAC.BookkeepingSystem.Gui.Basic.Message                        import Message
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                  import LHCB_BKKDBClient

__RCSID__ = "$Id: MainWidget.py,v 1.4 2008/11/03 11:28:01 zmathe Exp $"

#from DIRAC.BookkeepingSystem.Gui.Widget.TreeWidget import TreeWidget

#############################################################################  
class MainWidget(QMainWindow, Ui_MainWidget):
  
  #############################################################################  
  def __init__(self, fileName,parent = None):
    super(MainWidget, self).__init__()

    """
    Constructor
    
    @param parent parent widget (QWidget)
    
    """
    #self.__bkClient = LHCB_BKKDBClient()
    self.__controler = ControlerMain(self, None)
    QMainWindow.__init__(self, parent)
    self.setupUi(self)
    
    self.__controler.addChild('TreeWidget', self.tree.getControler())
    self.connect(self.actionExit, SIGNAL("triggered()"),
                     self, SLOT("close()"))
    
    self.__controler.setFileName(fileName)
    #self.__controler.addChild('TableWidget', self.tableWidget.getControler())
    
        
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def start(self):
    self.__controler.start()
    '''
    item = self.__bkClient.get()
    items=Item(item,None)
    path = item['Value']['fullpath']
    for entity in self.__bkClient.list(path):
      childItem = Item(entity,items)
      items.addItem(childItem)
    message = Message({'action':'list','items':items})
    self.getControler().messageFromParent(message)
    '''
  
  #############################################################################  
  def waitCursor(self):
    self.setCursor(Qt.WaitCursor)
  
  #############################################################################  
  def arrowCursor(self):
    self.setCursor(Qt.ArrowCursor)
  
  