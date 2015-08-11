# pylint: skip-file

"""
Bookmarks widget
"""
########################################################################

from PyQt4.QtCore  import SIGNAL, Qt
from PyQt4.QtGui   import QAbstractItemView, QWidget

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_BookmarksWidget                      import Ui_BookmarksWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerBookmarks                   import ControlerBookmarks
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel                              import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.AddBookmarksWidget                      import AddBookmarksWidget

__RCSID__ = " $"

#############################################################################
class BookmarksWidget(QWidget, Ui_BookmarksWidget):
  """
  BookmarksWidget
  """
  #############################################################################
  def __init__(self, parent=None):
    """
    Constructor

    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    Ui_BookmarksWidget.__init__(self)
    self.setupUi(self)

    self.__model = None
    self.__controler = None
    self.__addBookmarks = None

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  def setupControler(self, parent):
    """setup the controller"""
    self.__controler = ControlerBookmarks(self, parent.getControler())

    self.connect(self.removeButton, SIGNAL("clicked()"), self.__controler.removeBookmarks)
    self.connect(self.addButton, SIGNAL("clicked()"), self.__controler.addBookmarks)

    self.getControler().filltable()

    self.__addBookmarks = AddBookmarksWidget(self)
    self.__controler.addChild('AddBookmarks', self.__addBookmarks.getControler())
    self.connect(self.bookmarks, SIGNAL("doubleClicked ( const QModelIndex & )"), self.__controler.doubleclick)

  #############################################################################
  def hidewidget(self):
    """hides the widget"""
    self.hide()


  #############################################################################
  def filltable(self, header, tabledata):
    """fills the table"""
    # set the table model
    tm = TableModel(tabledata, header, self)

    self.bookmarks.setModel(tm)
    self.bookmarks.setSelectionBehavior(QAbstractItemView.SelectRows)
    self.bookmarks.setSelectionMode(QAbstractItemView.SingleSelection)

    self.bookmarks.setAlternatingRowColors(True)

    sm = self.bookmarks.selectionModel()
    self.connect(sm, SIGNAL("selectionChanged(QItemSelection, QItemSelection)"), self.__controler.selection)

    # set the minimum size
    self.setMinimumSize(400, 300)

    # hide grid
    self.bookmarks.setShowGrid(True)

    # set the font
    #font = QFont("Courier New", 12)
    #self.bookmarks.setFont(font)

    # hide vertical header
    vh = self.bookmarks.verticalHeader()
    vh.setVisible(True)

    # set horizontal header properties
    hh = self.bookmarks.horizontalHeader()
    hh.setStretchLastSection(True)

    # set column width to fit contents
    self.bookmarks.resizeColumnsToContents()
    self.bookmarks.setSortingEnabled(True)

    # set row height
    nrows = len(tabledata)
    for row in xrange(nrows):
      self.bookmarks.setRowHeight(row, 18)

    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)


  def getSelectedRow(self):
    """returns the selected rows"""
    for i in self.bookmarks.selectedIndexes():
      row = i.row()
      title = i.model().arraydata[row][0]
      path = i.model().arraydata[row][1]
    return {'Title':title, 'Path':path}

  #############################################################################
  def waitCursor(self):
    """wait cursor"""
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    """normal cursor"""
    self.setCursor(Qt.ArrowCursor)
