########################################################################
# $Id: MainWidget.py,v 1.7 2009/05/08 15:23:24 zmathe Exp $
########################################################################


from PyQt4.QtCore import *
from PyQt4.QtGui  import *

from DIRAC.BookkeepingSystem.Gui.Widget.MainWidget_ui                 import Ui_MainWidget
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerMain              import ControlerMain
from DIRAC.BookkeepingSystem.Gui.Basic.Item                           import Item
from DIRAC.BookkeepingSystem.Gui.Basic.Message                        import Message
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                  import LHCB_BKKDBClient
from DIRAC.BookkeepingSystem.Gui.Widget.ProductionLookup              import ProductionLookup
__RCSID__ = "$Id: MainWidget.py,v 1.7 2009/05/08 15:23:24 zmathe Exp $"

#from DIRAC.BookkeepingSystem.Gui.Widget.TreeWidget import TreeWidget

#############################################################################  
class MainWidget(QMainWindow, Ui_MainWidget):
  
  #############################################################################  
  def __init__(self, fileName, savepath = None, parent = None):
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
    
    self.connect(self.actionFile_dialog_paging_size, SIGNAL("triggered()"), self.tree.getControler().setPagesize)
      
    self.__productionLookup = ProductionLookup(data = None, parent = self)
    self.__controler.addChild('ProductionLookup', self.__productionLookup.getControler())
    
    self.__controler.setFileName(fileName)
    if savepath != '':
      self.__controler.setPathFileName(savepath)
    
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
  
  