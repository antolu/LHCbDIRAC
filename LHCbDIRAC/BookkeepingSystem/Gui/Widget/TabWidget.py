# pylint: skip-file

"""
Tab widget
"""
########################################################################
# $Id$
########################################################################

from PyQt4.QtGui                                                    import  QWidget, QGridLayout, \
                                                                            QTableView, QMenu, QAction,\
                                                                            QCursor, QApplication
from PyQt4.QtCore                                                   import SIGNAL, Qt

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel

from DIRAC                                                          import gLogger

__RCSID__ = "$Id$"

#############################################################################
class TabWidget(QWidget):
  """
  TabWidget class
  """
  #############################################################################
  def __init__(self, data, parent=None):
    """initialize the widget"""
    QWidget.__init__(self, parent)

    self.__data = data
    self.__tabs = []
    self.__copyAction = None
    self.__popUp = None

  #############################################################################
  def createTable(self, header, tabledata):
    """creates a table"""
    gridlayout2 = QGridLayout(self)
    gridlayout2.setObjectName("gridlayout2")

    tableView = QTableView(self)
    tableView.setObjectName("tableView")
    gridlayout2.addWidget(tableView, 0, 0, 1, 1)

    self.__popUp = QMenu(tableView)

    self.__copyAction = QAction(self.tr("Copy data"), tableView)
    self.connect (self.__copyAction, SIGNAL("triggered()"), self.copy)
    self.__popUp.addAction(self.__copyAction)

    tableView.setContextMenuPolicy(Qt.CustomContextMenu)
    self.connect(tableView, SIGNAL('customContextMenuRequested(QPoint)'), self.popUpMenu)

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
    """retuns the description"""
    if self.__data == None:
      gLogger.error('Wrong tab!')
    else:
      return str(self.__data[1][1])

  #############################################################################
  def popUpMenu(self):
    """shows the popup menu"""
    self.__popUp.popup(QCursor.pos())

  #############################################################################
  def copy(self):
    """ copy data to the clipboard"""
    text = ''
    for i in self.__data:
      text += '%s \t %s \n' % (i[0], i[1])
    clipboard = QApplication.clipboard()
    clipboard.setText(text)


