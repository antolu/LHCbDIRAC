########################################################################
# $Id: ControlerTree.py,v 1.14 2009/05/08 15:23:25 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.BookkeepingSystem.Gui.Widget.FileDialogOptions            import FileDialogOptions
from DIRAC                                                           import gLogger, S_OK, S_ERROR

import types
__RCSID__ = "$Id: ControlerTree.py,v 1.14 2009/05/08 15:23:25 zmathe Exp $"

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
    if message.action()=='list':
      self.getWidget().getTree().showTree(message['items'])
    elif message.action()=='removeTree':
      self.getWidget().getTree().clearTree()
      self.getWidget().getTree().showTree(message['items'])      
    elif message.action()=='showJobInfos':
      controlers = self.getChildren()
      controlers['InfoDialog'].messageFromParent(message)
    elif message.action()== 'showAncestors':
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
          message = Message({'action':'list','items':feedback['items']})
          ct.messageFromParent(message)
    elif message.action() =='PageSizeIsNull':
      self.__pagesize = 0
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
  def standardQuery(self):
    message = Message({'action':'StandardQuery'})
    self.getParent().messageFromChild(self, message)
    self.getWidget().setAdvancedQueryValue()
  
  #############################################################################  
  def advancedQuery(self):
    message = Message({'action':'AdvancedQuery'})
    self.getParent().messageFromChild(self, message)
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
          message = Message({'action':'getNbEventsAndFiles','node':path})
          feedback = self.getParent().messageFromChild(self, message)
          statistics = feedback['Extras']['GlobalStatistics']
          files = feedback['TotalRecords']
          nbev = statistics['Number of Events']
          show={'Number of files':files,'Nuber of Events':nbev}
          #show={'Number of files':feedback['TotalRecords'],'Nuber of Events':feedback['Extras']['Number of Events']}
          self.getWidget().getTree().addLeaf(show, parentItem)
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
  def setPagesize(self):
    self.__option = FileDialogOptions(self.getWidget())
    self.__option.show()
    
  #############################################################################  
  def ok(self):
    value = self.__option.pageSize.text()
    if type(str(value)) == types.StringType:
      if value == 'ALL':
        self.__offset = 0
      elif type(int(value)) == types.IntType:
        self.__offset = int(value)
    self.__option.close()
  
  #############################################################################  
  def cancel(self):
    self.__offset = 0
    self.__option.close()
    
  #############################################################################  
  def getPageSize(self):
    return self.__offset