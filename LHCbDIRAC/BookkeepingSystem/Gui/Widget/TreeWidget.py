# pylint: skip-file

"""
Tree widget
"""
########################################################################
# $Id: TreeWidget.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################

from PyQt4.QtCore  import SIGNAL, Qt
from PyQt4.QtGui   import QWidget, QHeaderView

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_TreeWidget              import Ui_TreeWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerTree           import ControlerTree
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.InfoDialog                 import InfoDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.ProcessingPassDialog       import ProcessingPassDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FileDialog                 import FileDialog

__RCSID__ = "$Id$"

#from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreePanel    import TreePanel

#############################################################################
class TreeWidget(QWidget, Ui_TreeWidget):
  """
  TreeWidget class
  """
  #############################################################################
  def __init__(self, parent=None):
    """
    Constructor

    @param parent parent widget (QWidget)
    """
    self.__controler = None
    QWidget.__init__(self, parent)
    Ui_TreeWidget.__init__(self)
    self.__parent = parent.parentWidget()
    self.setupUi(self)
    self.configNameRadioButton.setChecked(True)
    self.Bookmarks.hide()
    self.__controler = ControlerTree(self, parent.parentWidget().getControler())
    self.connect(self.configNameRadioButton, SIGNAL("clicked()"), self.__controler.configButton)
    self.connect(self.radioButton_2, SIGNAL("clicked()"), self.__controler.eventTypeButton)
    self.connect(self.productionRadioButton, SIGNAL("clicked()"), self.__controler.productionRadioButton)
    self.connect(self.runLookup, SIGNAL("clicked()"), self.__controler.runRadioButton)
    self.connect(self.bookmarksButton, SIGNAL("clicked()"), self.__controler.bookmarksButtonPressed)
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.hidewidget)

    self.tree.setupControler()

    self.closeButton.hide()

    self.standardQuery.setChecked(True)
    self.connect(self.standardQuery, SIGNAL("clicked()"), self.__controler.standardQuery)
    self.connect(self.advancedQuery, SIGNAL("clicked()"), self.__controler.advancedQuery)

    self.__dialog = ProcessingPassDialog(self)
    self.__controler.addChild('ProcessingPassDialog', self.__dialog.getControler())

    self.__infodialog = InfoDialog(self)
    self.__controler.addChild('InfoDialog', self.__infodialog.getControler())

    self.__fileDialog = FileDialog(self)
    flags = Qt.Window
    flags |= Qt.WindowCloseButtonHint
    flags |= Qt.WindowMinimizeButtonHint
    flags |= Qt.WindowMaximizeButtonHint
    self.__fileDialog.setWindowFlags(flags)

    self.__controler.addChild('FileDialog', self.__fileDialog.getControler())
    self.Bookmarks.setupControler(self)
    self.__controler.addChild('Bookmarks', self.Bookmarks.getControler())

    #self.__dialog.show()
    #self.__dialog.hide()

#    self.__bkClient = LHCB_BKKDBClient()
#    item = self.__bkClient.get()
#    items=Item(item,None)
#    path = item['Value']['fullpath']
#    for entity in self.__bkClient.list(path):
#      childItem = Item(entity,items)
#      items.addItem(childItem)
#    self.tree.showTree(items)

    self.tree.header().setResizeMode(1, QHeaderView.ResizeToContents)
    self.tree.header().setResizeMode(0, QHeaderView.ResizeToContents)


  #############################################################################
  def showBookmarks(self):
    """shows the bookmarks"""
    self.bookmarksButton.hide()
    self.closeButton.show()
    self.Bookmarks.show()

  def hidewidget(self):
    """hides the bookmarks widget"""
    self.bookmarksButton.show()
    self.closeButton.hide()
    self.Bookmarks.hide()

  #############################################################################
  def getTree(self):
    """returns the tree"""
    return self.tree

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def setAdvancedQueryValue(self):
    """tick or un-tick the check box"""
    if self.advancedQuery.isChecked():
      self.advancedQuery.setChecked(False)
    else:
      self.advancedQuery.setChecked(True)

  #############################################################################
  def setStandardQueryValue(self):
    """tick or un-tick the check box"""
    if self.standardQuery.isChecked():
      self.standardQuery.setChecked(False)
    else:
      self.standardQuery.setChecked(True)

  #############################################################################
  def getPageSize(self):
    """returns the page size"""
    return self.pageSize.text()

  #############################################################################
  def runLookupRadioButtonIsChecked(self):
    """is the run lookup ticked"""
    return self.runLookup.isChecked()

  #############################################################################
  def productionLookupradiobuttonIschecked(self):
    """is the production lookup ticked"""
    return self.productionRadioButton.isChecked()

  #############################################################################
  def setSimRadioButton(self):
    """tick or un-tick the check box"""
    if self.configNameRadioButton.isChecked():
      self.configNameRadioButton.setChecked(False)
    else:
      self.configNameRadioButton.setChecked(True)

  #############################################################################
  def setEvtButton(self):
    """tick or un-tick the check box"""
    if self.radioButton_2.isChecked():
      self.radioButton_2.setChecked(False)
    else:
      self.radioButton_2.setChecked(True)

  #############################################################################
  def setProdButton(self):
    """tick or un-tick the check box"""
    if self.productionRadioButton.isChecked():
      self.productionRadioButton.setChecked(False)
    else:
      self.productionRadioButton.setChecked(True)

  #############################################################################
  def setRunButton(self):
    """tick or un-tick the check box"""
    if self.runLookup.isChecked():
      self.runLookup.setChecked(False)
    else:
      self.runLookup.setChecked(True)

  #############################################################################
  def headerItem(self):
    """returns the elements of the header"""
    return self.tree.headerItem()
