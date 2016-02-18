# pylint: skip-file

"""
Main bookkeeping widget
"""

from PyQt4.QtCore import SIGNAL, SLOT, Qt
from PyQt4.QtGui  import QMainWindow

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_MainWidget                 import Ui_MainWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerMain              import ControlerMain
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.ProductionLookup              import ProductionLookup
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.DataQualityDialog             import DataQualityDialog

__RCSID__ = "$Id$"

#from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreeWidget import TreeWidget

#############################################################################
class MainWidget(QMainWindow, Ui_MainWidget):
  """
  MainWidget class
  Constructor

  @param parent parent widget (QWidget)

  """
  #############################################################################
  def __init__(self, fileName, savepath=None, parent=None):
    super(MainWidget, self).__init__()
    QMainWindow.__init__(self, parent)
    Ui_MainWidget.__init__(self)

    #self.__bkClient = LHCB_BKKDBClient()
    self.__controler = ControlerMain(self, None)
    self.setupUi(self)

    self.__controler.addChild('TreeWidget', self.tree.getControler())
    self.connect(self.actionExit, SIGNAL("triggered()"),
                     self, SLOT("close()"))

    self.connect(self.actionDataQuality, SIGNAL("triggered()"), self.__controler.dataQuality)

    self.__dataQuality = DataQualityDialog(self)
    self.__controler.addChild('DataQuality', self.__dataQuality.getControler())

    self.__productionLookup = ProductionLookup(data=None, parent=self)
    self.__controler.addChild('ProductionLookup', self.__productionLookup.getControler())

    self.__controler.setFileName(fileName)
    if savepath != '':
      self.__controler.setPathFileName(savepath)

    #self.__controler.addChild('TableWidget', self.tableWidget.getControler())


  #############################################################################
  def getControler(self):
    """returs the controller"""
    return self.__controler

  #############################################################################
  def start(self):
    """
    It start the bookkeeping gui.
    """
    self.__controler.start()

#    item = self.__bkClient.get()
#    items=Item(item,None)
#    path = item['Value']['fullpath']
#    for entity in self.__bkClient.list(path):
#      childItem = Item(entity,items)
#      items.addItem(childItem)
#    message = Message({'action':'list','items':items})
#    self.getControler().messageFromParent(message)
#

  #############################################################################
  def waitCursor(self):
    """shows the wait cursor"""
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    """shows the normal cursor"""
    self.setCursor(Qt.ArrowCursor)

