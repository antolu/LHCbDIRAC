########################################################################
# $Id$
########################################################################


from PyQt4.QtCore  import *
from PyQt4.QtGui   import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableWidget_ui     import Ui_TableWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerTable  import ControlerTable

__RCSID__ = "$Id$"

#############################################################################  
class TableWidget(QWidget, Ui_TableWidget):
  
  def __init__(self, parent = None):
    """
    Constructor
    
    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerTable(self, parent.parentWidget().getControler()) 
      
  #############################################################################  
  def clear(self):
    self.tableWidget.clear()
  
  #############################################################################  
  def setColumnCount(self, number):
    self.tableWidget.setColumnCount(number)
    
  #############################################################################  
  def setRowCount(self, row):
    self.tableWidget.setRowCount(row)
  
  #############################################################################  
  def setupControler(self, controler):
    print 'table widget'
  
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  