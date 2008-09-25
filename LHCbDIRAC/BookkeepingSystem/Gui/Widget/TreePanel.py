########################################################################
# $Id: TreePanel.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

from PyQt4.QtCore import *
from PyQt4.QtGui import *
from DIRAC.BookkeepingSystem.Gui.Basic.Item              import Item
from DIRAC.BookkeepingSystem.Gui.Widget.TreeNode         import TreeNode

__RCSID__ = "$Id: TreePanel.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

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
  
  #############################################################################  
  def setupControler(self):
    self.__controler = self.parentWidget().getControler()
    self.connect(self, SIGNAL('itemExpanded(QTreeWidgetItem *)'),
            self.__controler._on_item_expanded)
    
    self.connect(self,
            SIGNAL('itemClicked(QTreeWidgetItem *, int)'),
            self.__controler._on_item_clicked)
  
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
    for child in keys:
      self.parseFolderElement(children[child],parent)
  
    #self.connect(self, QtCore.SIGNAL("itemChanged(QTreeWidgetItem *, int)"),
    #             self.updateDomElement)
    
    return True
  
  #############################################################################  
  def parseFolderElement(self, element, parentItem=None):
    item = self.createItem(element, parentItem)
    item.setUserObject(element)
    title = element.name() #['fullpath']
    #if title != '':
    #    title = QtCore.QObject.tr("Folder")
  
    item.setFlags(item.flags() | Qt.ItemIsEditable)
    item.setIcon(0, self.folderIcon)
    item.setText(0, title)
  
    folded = True # (element.attribute("folded") != "no")
    self.setItemExpanded(item, False)
    
    userobj = item.getUserObject()
    if userobj.has_key('level'):
      self.createdumyNode({'name':userobj['level']},item)
      '''
      dumy = self.createItem({'name':userobj['level']},item)
      dumy.setUserObject(None)
      self.setItemExpanded(dumy, False)
      title = userobj['level']
      dumy.setFlags(item.flags() | Qt.ItemIsEditable)
      dumy.setIcon(0, self.bookmarkIcon)
      dumy.setText(0, title)
      '''
    else:
      dumy = self.createItem({'name':'dummy'},item)
      self.setItemExpanded(dumy, True)
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
    dumy = self.createItem(element,parent)
    dumy.setUserObject(None)
    self.setItemExpanded(dumy, False)
    title = element['name']
    dumy.setFlags(parent.flags() | Qt.ItemIsEditable)
    dumy.setIcon(0, self.bookmarkIcon)
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