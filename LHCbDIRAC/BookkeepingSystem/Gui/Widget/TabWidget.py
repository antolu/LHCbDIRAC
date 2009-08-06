########################################################################
# $Id: TabWidget.py,v 1.2 2009/08/06 16:06:25 zmathe Exp $
########################################################################

from PyQt4.QtGui                                                import *
from PyQt4.QtCore                                               import *
from DIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from DIRAC                                                      import gLogger, S_OK, S_ERROR
__RCSID__ = "$Id: TabWidget.py,v 1.2 2009/08/06 16:06:25 zmathe Exp $"

#############################################################################  
class TabWidget(QWidget):
  
  #############################################################################  
  def __init__(self, data, parent = None):
    QWidget.__init__(self, parent)
    
    self.__data = data
    self.__tabs = []
  
  #############################################################################  
  def createTable(self, header, tabledata ):
    gridlayout2 = QGridLayout(self)
    gridlayout2.setObjectName("gridlayout2")

    tableView = QTableView(self)
    tableView.setObjectName("tableView")
    gridlayout2.addWidget(tableView,0,0,1,1)
       # set the table model
    tm = TableModel(tabledata, header, self) 
    
    tableView.setModel(tm)
    tableView.setAlternatingRowColors(True)

    # set the minimum size
    self.setMinimumSize(400, 300)
  
    # hide grid
    tableView.setShowGrid(True)
  
    # set the font
    #font = QFont("Courier New", 12)
    #self.tableView.setFont(font)
  
    # hide vertical header
    vh = tableView.verticalHeader()
    vh.setVisible(True)
  
    # set horizontal header properties
    hh = tableView.horizontalHeader()
    hh.setStretchLastSection(True)
  
    # set column width to fit contents
    tableView.resizeColumnsToContents()
    tableView.setSortingEnabled(True)
    tableView.sortByColumn (0, Qt.AscendingOrder)
  
    # set row height
    nrows = len(tabledata)
    for row in xrange(nrows):
        tableView.setRowHeight(row, 18)
  
  #############################################################################  
  def getGroupDesc(self):
    if self.__data == None:
      gLogger.error('Wrong tab!')
    else:
      return self.__data[0][2]
      
  