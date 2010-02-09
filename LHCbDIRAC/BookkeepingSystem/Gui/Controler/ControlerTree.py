########################################################################
# $Id$
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC                                                               import gLogger, S_OK, S_ERROR
from PyQt4.QtCore import *
from PyQt4.QtGui import *

import types
__RCSID__ = "$Id$"

#############################################################################  
class ControlerTree(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerTree, self).__init__(widget, parent)
    self.__pagesize = 0
    self.__option = None
    self.__offset = 0
  #############################################################################  
  def messageFromParent(self, message):
    gLogger.debug(message)
    if message.action()=='list':
      self.getWidget().getTree().showTree(message['items'])
    elif message.action()=='removeTree':
      self.getWidget().getTree().clearTree()
      self.getWidget().getTree().showTree(message['items'])      
    elif message.action()=='showJobInfos':
      controlers = self.getChildren()
      controlers['InfoDialog'].messageFromParent(message)
    else:
      print 'Unknown message!',message.action()
   
    
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    if message.action() == 'getLimitedFiles':    
      message = Message({'action':'expande','node':message['path'], 'StartItem':self.__pagesize,'MaxItem':(self.__pagesize+self.__offset)})

      self.__pagesize += self.__offset
      feedback = self.getParent().messageFromChild(self, message)
      
      if feedback.action()=='showNode':
          controlers = self.getChildren()
          ct = controlers['FileDialog']  
          message = Message({'action':'listNextFiles','items':feedback['items']})
          ct.messageFromParent(message)
    elif message.action() =='PageSizeIsNull':
      self.__offset = 0
      self.__pagesize = 0
    elif message.action() == 'openPathLocation':
      path = message['Path']
      self.browsePath(path)
    else:
      return self.getParent().messageFromChild(self, message) # send to main controler!
  
  #############################################################################  
  def configButton(self):
    message = Message({'action':'configbuttonChanged'})
    self.getParent().messageFromChild(self, message)
   
  def productionRadioButton(self):
    message = Message({'action':'productionButtonChanged'})
    self.getParent().messageFromChild(self, message)
  
  #############################################################################  
  def runRadioButton(self):
    message = Message({'action':'runLookup'})
    self.getParent().messageFromChild(self, message)
  
  #############################################################################
  def bookmarksButtonPressed(self):
    self.getWidget().showBookmarks()
  
  def hidewidget(self):
    self.getWidget().hidewidget()
    
  #############################################################################  
  def standardQuery(self):
    widget = self.getWidget()
    
    if not widget.runLookupRadioButtonIsChecked() and not widget.productionLookupRadioButtonIsChecked():
      message = Message({'action':'StandardQuery'})
      self.getParent().messageFromChild(self, message)
      self.getWidget().setAdvancedQueryValue()
    else:
      self.getWidget().setAdvancedQueryValue()
    
  #############################################################################  
  def advancedQuery(self):
    widget = self.getWidget()
    
    if not widget.runLookupRadioButtonIsChecked() and not widget.productionLookupRadioButtonIsChecked():
      message = Message({'action':'AdvancedQuery'})
      self.getParent().messageFromChild(self, message)
      self.getWidget().setStandardQueryValue()
    else:
      self.getWidget().setStandardQueryValue()
    
  #############################################################################  
  def eventTypeButton(self):
    message = Message({'action':'eventbuttonChanged'})
    self.getParent().messageFromChild(self, message)
  
  #############################################################################  
  def _on_item_expanded(self,parentItem):
    gLogger.debug('On item expanded',parentItem.getUserObject())
    node = parentItem.getUserObject()
    if node <> None:
      path = node['fullpath']
      if parentItem.childCount() != 1: 
        return
        #parentItem.takeChild(0) # I have to remove the dumychildren!
      else:
        

        if parentItem.childCount() == 1:
          child = parentItem.child(0)
          gLogger.debug('Childcount:',child.getUserObject())
          parentItem.takeChild(0)           

        if node.has_key('showFiles'):
          message = Message({'action':'waitCursor','type':None})
          feeddback = self.getParent().messageFromChild(self, message)
          
          message = Message({'action':'getNbEventsAndFiles','node':path})
          feedback = self.getParent().messageFromChild(self, message)
          
          statistics = feedback['Extras']['GlobalStatistics']
          files = feedback['TotalRecords']
          nbev = statistics['Number of Events']
          show={'Number of files':files,'Nuber of Events':nbev}
          #show={'Number of files':feedback['TotalRecords'],'Nuber of Events':feedback['Extras']['Number of Events']}
          self.getWidget().getTree().addLeaf(show, parentItem)
          message = Message({'action':'arrowCursor','type':None})
          feeddback = self.getParent().messageFromChild(self, message)
        else:
          message = Message({'action':'expande','node':path})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback.action()=='showNode':
            if feedback['items'].childnum() > 0:
              child = feedback['items'].child(0)
              if not child['expandable']:
                message = Message({'action':'waitCursor','type':None})
                feeddback = self.getParent().messageFromChild(self, message)
                
                controlers = self.getChildren()
                ct = controlers['FileDialog']  
                message = Message({'action':'list','items':feedback['items'], 'StartItem':0,'MaxItem':self.getPageSize()})
                #res = ct.messageFromParent(message)
                res = True
                if res :
                  message = Message({'action':'arrowCursor','type':None})
                  feeddback = self.getParent().messageFromChild(self, message)
              else:        
                self.getWidget().getTree().showTree(feedback['items'], parentItem)
        
    
 
  #############################################################################  
  def _on_item_clicked(self,parentItem):
    node = parentItem.getUserObject()
    parentnode = parentItem.parent()
    if parentnode != None: 
      parent = parentnode.getUserObject()
      controlers = self.getChildren()
      if parent.has_key('level') and parent.has_key('showFiles'):  
        path = parent['fullpath']
        message = Message({'action':'expande','node':path, 'StartItem':0,'MaxItem':self.getPageSize()})
        self.__pagesize += self.__offset
        feedback = self.getParent().messageFromChild(self, message)
        if feedback.action()=='showNode':
          ct = controlers['FileDialog']  
          message = Message({'action':'list','items':feedback['items']})
          ct.messageFromParent(message)
  
  #############################################################################
  def __openPath(self, name, item):
    
    if item != None:
      for i in range(item.childCount()):
        node = item.child(i)
        userObject = node.getUserObject() 
        if userObject != None: 
          if userObject['name'] == name:
            self.getWidget().getTree().expandItem(node)
            return node
    else:
      message = 'The path is wrong!'
      gLogger.error(message)
      QMessageBox.critical(self.getWidget(), "ERROR", message,QMessageBox.Ok)
      
    
        
  def browsePath(self, path):
    info = path.split(':/')
    if len(info) > 1:
      prefix = info[0]
      path = info[1]
      message = Message({'action':'BookmarksPrefices'})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback['OK']:
        curentprefix = feedback['Value']
        if curentprefix != prefix:
          answer = QMessageBox.information(self.getWidget(), "Different query or query type", " Do you want to refresh the tree? If yes, Your selection will lost!!!", QMessageBox.Yes,  QMessageBox.No)
          if answer == QMessageBox.Yes:
            qType = prefix.split('+')
            if qType[0] == 'sim':
              self.configButton()
              self.getWidget().setSimRadioButton()
            elif qType[0] == 'evt':
              self.eventTypeButton()
              self.getWidget().setEvtButton()
              
            elif qType[0] == 'prod':
              self.productionRadioButton()
              self.getWidget().setProdButton()
              
            elif qType[0] == 'run':
              self.runRadioButton()
              self.getWidget().setRunButton()
              
            if qType[1] == 'std':
              self.standardQuery()
              self.getWidget().setStandardQueryValue()
              
            elif qType[1] == 'adv':
              self.advancedQuery()
              self.getWidget().setAdvancedQueryValue()
                   
            self.__openTreeNodes(path)
          elif answer == QMessageBox.No:
            return
        else:
          self.__openTreeNodes(path)  
      else:
        QMessageBox.critical(self.getWidget(), "Error", str(feedback['Message']),QMessageBox.Ok)
        gLoogger.error(feedback['Message'])
      
    else:
      QMessageBox.critical(self.getWidget(), "Error", 'Wrong path!',QMessageBox.Ok)
      gLogger.error('Wrong path!')      
  
  #############################################################################
  def __openTreeNodes(self, path):
      npath = path.split('/')
      n = self.getWidget().getTree().findItems(npath[1],Qt.MatchExactly,0)
      parentItem = n[0] 
      self.getWidget().getTree().expandItem(parentItem)
      node = '/'+npath[1]
      for i in range(2,len(npath)):
        if npath[i] != '':
            node = npath[i]
            item = self.__openPath(node, parentItem)
            parentItem = item
            
      if parentItem != None and parentItem.childCount() > 0:
        node = parentItem.child(0) # this two line open the File Dialog window
        self._on_item_clicked(node)
          
  #############################################################################  
  def _on_itemDuble_clicked(self, parentItem, column):
    self._on_item_clicked(parentItem)
     
  
  #############################################################################  
  def moreInformations(self):
    currentItem = self.getWidget().getTree().getCurrentItem()
    node = currentItem.getUserObject()
    
    controlers = self.getChildren()
    ct = controlers['InfoDialog']
    if node <> None:
      if node.has_key('level') and node['level'] == 'Processing Pass':
        ctproc = controlers['ProcessingPassDialog']
        message = Message({'action':'list','items':node})
        ctproc.messageFromParent(message)
      elif node.has_key('level') and node['level'] == 'Production(s)/Run(s)':
        message = Message({'action':'ProductionInformations','production':node['name']})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback != None:
          message = Message({'action':'list','items':feedback})
          ct.messageFromParent(message)
      elif type(node) != types.DictType and node.expandable() :
          message = Message({'action':'list','items':node})
          ct.messageFromParent(message)
  
  #############################################################################
  def bookmarks(self):
    currentItem = self.getWidget().getTree().getCurrentItem()
    curent = currentItem
    path = ''
    nodes = [ curent ]
    while curent != None:
      curent = curent.parent() 
      nodes +=  [curent]
      
    nodes.reverse()
    for i in  nodes:
      if i != None:
        node = i.getUserObject()
        if node != None and node.has_key('name'):
          path += '/'+ node['name'] 
    
    message = Message({'action':'BookmarksPrefices'})
    feedback = self.getParent().messageFromChild(self, message)
    if not feedback['OK']:
      gLogger.error(feedback['Message'])
    else:
      values = feedback['Value']
      controlers = self.getChildren()
      ct = controlers['Bookmarks']
      fullpath = values+':/'+path
      message = Message({'action':'showValues','paths':{'Title':path,'Path':fullpath}})
      f = ct.messageFromParent(message)
      if not f['OK']:
        gLogger.error(f['Message'])
      
    
    
  #############################################################################  
  def getPageSize(self):
    value = self.getWidget().getPageSize()
    if type(str(value)) == types.StringType:
      if value == 'ALL':
        self.__offset = 0
      elif type(int(value)) == types.IntType:
        self.__offset = int(value)
    return self.__offset
  
 