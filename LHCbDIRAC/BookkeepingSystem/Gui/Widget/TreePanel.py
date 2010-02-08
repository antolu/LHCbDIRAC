########################################################################
# $Id$
########################################################################

from PyQt4.QtCore import *
from PyQt4.QtGui import *

from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item              import Item
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreeNode         import TreeNode
import DIRAC, types

__RCSID__ = "$Id$"

#############################################################################  
class TreePanel(QTreeWidget):
  
  #############################################################################  
  def __init__(self, parent=None):
    QTreeWidget.__init__(self, parent)
  
    #labels = QStringList()
    #labels << self.tr("Title") << self.tr("Location")
  
    #self.header().setResizeMode(QHeaderView.Stretch)
    #self.setHeaderLabels(labels)
        
    self.folderIcon = QIcon()
    self.bookmarkIcon = QIcon()
   
    self.folderIcon.addPixmap(self.style().standardPixmap(QStyle.SP_DirClosedIcon),
                              QIcon.Normal, QIcon.Off)
    self.folderIcon.addPixmap(self.style().standardPixmap(QStyle.SP_DirOpenIcon),
                              QIcon.Normal, QIcon.On)
    self.bookmarkIcon.addPixmap(self.style().standardPixmap(QStyle.SP_FileIcon))
    
    '''
    self.connect(self, SIGNAL('itemExpanded(QTreeWidgetItem *)'),
            self._on_item_expanded)
    
    self.connect(self,
            SIGNAL('itemActivated(QTreeWidgetItem *, int)'),
            self._on_item_clicked)
    '''
    self.__controler = None
    self.setSelectionBehavior(QAbstractItemView.SelectRows)
    self.__currentItem = None
    picturesPath = DIRAC.rootPath+'/DIRAC/BookkeepingSystem/Gui/Widget'
    self.infoIcon_ = QIcon(picturesPath+"/images/info1.png")
    
    self.filesIcon_ = QIcon(picturesPath+"/images/files1.png")
    
    
      
  #############################################################################  
  def setupControler(self):
    self.__controler = self.parentWidget().getControler()
    
    self.connect(self, SIGNAL('itemExpanded(QTreeWidgetItem *)'),
            self.__controler._on_item_expanded)
    
    '''
    self.connect(self,
            SIGNAL('itemClicked(QTreeWidgetItem *, int)'),
            self.__controler._on_item_clicked)
    '''
    self.connect(self, SIGNAL('itemDoubleClicked(QTreeWidgetItem *, int)'),self.__controler._on_itemDuble_clicked)
    
    self.__createPopUpMenu()
    
    self.setContextMenuPolicy(Qt.CustomContextMenu)
    
    self.connect(self,SIGNAL('customContextMenuRequested(QPoint)'),
               self.popUpMenu)



  
  '''  
  #############################################################################  
  def _on_item_expanded(self,parentItem):
    controler = self.parentWidget().getControler()
    #path = parentItem.text(0)
    node = parentItem.getUserObject()
    if node <> None:
      path = node['fullpath']
      if parentItem.childCount() > 0:
        for i in range(parentItem.childCount()):
          parentItem.takeChild(0)
      #newPath = '/'+str(path)
      bkClient = self.parentWidget().getBkkClient()
      #print bkClient.list(str(path))
      items=Item({'fullpath':path},None)
      for entity in bkClient.list(str(path)):
        childItem = Item(entity,items)
        items.addItem(childItem)
      if parentItem <> None:
        self.showTree(items, parentItem)
      
  #############################################################################  
  def _on_item_clicked(self):
    print 'wqwqwq'
  '''
  #############################################################################  
  def showTree(self, item,parent=None):
    #self.clear()
  
    #self.disconnect(self, QtCore.SIGNAL("itemChanged(QTreeWidgetItem *, int)"),
     #               self.updateDomElement)
  
    children = item.getChildren()
    
    keys = children.keys()
    keys.sort()
    node = children[keys[0]]
    if node.has_key('level'):
      self.createdumyNode({'name':node['level']}, parent)
      
    for child in keys:
      self.parseFolderElement(children[child],parent)
    
    
  
    #self.connect(self, QtCore.SIGNAL("itemChanged(QTreeWidgetItem *, int)"),
    #             self.updateDomElement)
    self.repaint()    
    return True
  
  #############################################################################  
  def addLeaf(self, element, parentItem=None):
    item = self.createItem(element, parentItem)
    item.setUserObject(element)
    #print '!!!!!!!!!',parentItem.getUserObject()
    nbfiles = element['Number of files'] 
    nbevents = element['Nuber of Events']
    item.setIcon(0, self.filesIcon_)
    title = self.tr("Nb of Files/Events")
    item.setText(0, title)
    
    desc = self.tr(str(nbfiles)+'/'+str(nbevents))
    item.setText(1, desc)
    item.setExpanded(False)
    self.repaint()    
    item.setFlags(item.flags() | Qt.ItemIsEditable)

    
  def parseFolderElement(self, element, parentItem=None):
    item = self.createItem(element, parentItem)
    item.setUserObject(element)
    title = element.name()
    if element.has_key('level'):
      if element['level']=='Production(s)/Run(s)':
        if element.name() != 'ALL':
          title = str(abs(long(element.name()))) #['fullpath']
    
    #if title != '':
    #    title = QtCore.QObject.tr("Folder")
  
    
    item.setIcon(0, self.folderIcon)
    item.setText(0, title)
    #item.setFlags(item.flags() | Qt.ItemIsEditable)
    #self.setItemExpanded(item, False)
        
    userobj = item.getUserObject()
    if userobj.has_key('level'):
      if userobj['level']=='Event types':
        if userobj.has_key('Description'):
          item.setText(1, userobj['Description'])
        else:
          item.setText(1, '')
    dumy = self.createItem({'name':'Nb of Files/Events'},item)
    self.setItemExpanded(dumy, True)
    dumy.setFlags(item.flags() | Qt.ItemIsEnabled)
    
    self.repaint()    
    '''
    child = element.firstChildElement()
    while not child.isNull():
        if child.tagName() == "folder":
            self.parseFolderElement(child, item)
        elif child.tagName() == "bookmark":
            childItem = self.createItem(child, item)
  
            title = child.firstChildElement("title").text()
            if title.isEmpty():
                title = QtCore.QObject.tr("Folder")
  
            childItem.setFlags(item.flags() | QtCore.Qt.ItemIsEditable)
            childItem.setIcon(0, self.bookmarkIcon)
            childItem.setText(0, title)
            childItem.setText(1, child.attribute("href"))
        elif child.tagName() == "separator":
            childItem = self.createItem(child, item)
            childItem.setFlags(item.flags() & ~(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEditable))
            childItem.setText(0, QtCore.QString(30 * "\xB7"))
  
        child = child.nextSiblingElement()
  '''
  
  #############################################################################  
  def createdumyNode(self,element,parent):
    if parent != None:
      dumy = self.createItem(element,parent)
      dumy.setUserObject(None)
      self.setItemExpanded(dumy, False)
      title = element['name']
      dumy.setFlags(parent.flags() | Qt.ItemIsEditable)
      dumy.setIcon(0, self.infoIcon_)
      dumy.setText(0, title)
      return dumy
  
  #############################################################################  
  def createItem(self, element, parentItem=None):
    item = TreeNode()#QTreeWidgetItem()
    
    if parentItem is not None:
        item = TreeNode(parentItem)#QtGui.QTreeWidgetItem(parentItem)
    else:
        item = TreeNode(self)#QtGui.QTreeWidgetItem(self)
    return item
  
  #############################################################################  
  def clearTree(self):
    self.clear()
    self.repaint()
  
  #############################################################################  
  def popUpMenu(self, pos):
    item = self.itemAt(pos)
    if item:
      self.__currentItem = item
      self.__popUp.popup(QCursor.pos())
      

  
  #############################################################################  
  def __createPopUpMenu(self):
    self.__popUp = QMenu(self)
    
    self.__jobAction = QAction(self.tr("More Information"), self)
    self.connect (self.__jobAction, SIGNAL("triggered()"), self.__controler.moreInformations)
    self.__popUp.addAction(self.__jobAction)
    
  
    self.__bookmarksAction = QAction(self.tr("Bookmarks"), self)
    self.connect (self.__bookmarksAction, SIGNAL("triggered()"), self.__controler.bookmarks)
    self.__popUp.addAction(self.__bookmarksAction)
  
  
    '''
    self.__closeAction = QAction(self.tr("Close"), self)
    self.connect (self.__closeAction, SIGNAL("triggered()"), self.__controler.close)
    self.__popUp.addAction(self.__closeAction)
    '''
  
  #############################################################################  
  def getCurrentItem(self):
    return self.__currentItem
  