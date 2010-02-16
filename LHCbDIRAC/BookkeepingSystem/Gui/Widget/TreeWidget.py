########################################################################
# $Id$
########################################################################

from PyQt4.QtCore  import *
from PyQt4.QtGui   import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreeWidget_ui              import Ui_TreeWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerTree           import ControlerTree
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.InfoDialog                 import InfoDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.ProcessingPassDialog       import ProcessingPassDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FileDialog                 import FileDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                        import Item
import LHCbDIRAC

__RCSID__ = "$Id$"

#from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreePanel    import TreePanel

#############################################################################  
class TreeWidget(QWidget, Ui_TreeWidget):
  
  #############################################################################  
  def __init__(self, parent = None):
    """
    Constructor
    
    @param parent parent widget (QWidget)
    """
    self.__controler = None
    QWidget.__init__(self, parent)
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
    
    picturesPath = LHCbDIRAC.rootPath+'/LHCbDIRAC/BookkeepingSystem/Gui/Widget'
    bookmarksIcon = QIcon(picturesPath+"/images/bookmarks2.png")
    self.bookmarksButton.setIcon(bookmarksIcon)
    
    closeIcon = QIcon(picturesPath+"/images/reloadpage.png")
    self.closeButton.setIcon(closeIcon)
    self.closeButton.hide()
    
    self.standardQuery.setChecked(True)
    self.connect(self.standardQuery, SIGNAL("clicked()"), self.__controler.standardQuery)
    self.connect(self.advancedQuery, SIGNAL("clicked()"), self.__controler.advancedQuery)
    
    self.__dialog = ProcessingPassDialog(self)
    self.__controler.addChild('ProcessingPassDialog',self.__dialog.getControler())
    
    self.__infodialog = InfoDialog(self)
    self.__controler.addChild('InfoDialog', self.__infodialog.getControler())
    
    self.__fileDialog = FileDialog(self)
    self.__controler.addChild('FileDialog',self.__fileDialog.getControler())
    self.Bookmarks.setupControler(self)
    self.__controler.addChild('Bookmarks', self.Bookmarks.getControler())
    
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
    self.tree.header().setResizeMode(1, QHeaderView.ResizeToContents)
    self.tree.header().setResizeMode(0, QHeaderView.ResizeToContents)
    
  
  #############################################################################
  def showBookmarks(self):
    self.bookmarksButton.hide()
    self.closeButton.show()
    self.Bookmarks.show()
  
  def hidewidget(self):
    self.bookmarksButton.show()
    self.closeButton.hide()
    self.Bookmarks.hide()
  
  #############################################################################  
  def getTree(self):
    return self.tree
  
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def setAdvancedQueryValue(self):
    if self.advancedQuery.isChecked():
      self.advancedQuery.setChecked(False)
    else:
      self.advancedQuery.setChecked(True)
      
  #############################################################################  
  def setStandardQueryValue(self):
    if self.standardQuery.isChecked():
      self.standardQuery.setChecked(False)
    else:
      self.standardQuery.setChecked(True)
  
  #############################################################################
  def getPageSize(self):
    return self.pageSize.text()
  
  #############################################################################
  def runLookupRadioButtonIsChecked(self):
    return self.runLookup.isChecked()
  
  #############################################################################
  def productionLookupRadioButtonIsChecked(self):
    return self.productionRadioButton.isChecked()
  
  #############################################################################
  def setSimRadioButton(self):
    if self.configNameRadioButton.isChecked():
      self.configNameRadioButton.setChecked(False)
    else:
      self.configNameRadioButton.setChecked(True)
  
  #############################################################################
  def setEvtButton(self):
    if self.radioButton_2.isChecked():
      self.radioButton_2.setChecked(False)
    else:
      self.radioButton_2.setChecked(True)
  
  #############################################################################
  def setProdButton(self):
    if self.productionRadioButton.isChecked():
      self.productionRadioButton.setChecked(False)
    else:
      self.productionRadioButton.setChecked(True)
  
  #############################################################################
  def setRunButton(self):
    if self.runLookup.isChecked():
      self.runLookup.setChecked(False)
    else:
      self.runLookup.setChecked(True)