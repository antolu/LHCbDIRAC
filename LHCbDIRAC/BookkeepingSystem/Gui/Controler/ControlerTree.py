########################################################################
# $Id: ControlerTree.py,v 1.4 2008/11/26 12:36:04 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: ControlerTree.py,v 1.4 2008/11/26 12:36:04 zmathe Exp $"

#############################################################################  
class ControlerTree(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerTree, self).__init__(widget, parent)
  
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
    else:
      print 'Unknown message!',message.action()
   
    
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    return self.getParent().messageFromChild(self, message) # send to main controler!
  
  #############################################################################  
  def configButton(self):
    message = Message({'action':'configbuttonChanged'})
    self.getParent().messageFromChild(self, message)
  
  #############################################################################  
  def eventTypeButton(self):
    message = Message({'action':'eventbuttonChanged'})
    self.getParent().messageFromChild(self, message)
  
  #############################################################################  
  def _on_item_expanded(self,parentItem):
    
    node = parentItem.getUserObject()
    if node <> None:
      path = node['fullpath']
      if parentItem.childCount() != 1: 
        return
        #parentItem.takeChild(0) # I have to remove the dumychildren!
      else:
        
        if parentItem.childCount() == 1:
          child = parentItem.child(0)
          if not child.getUserObject():
            parentItem.takeChild(0)  
        
        if node.has_key('level') and node['level']=='Program name and version':
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
                message = Message({'action':'list','items':feedback['items']})
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
      if parent.has_key('level') and parent['level'] == 'Program name and version':
        path = parent['fullpath']
        message = Message({'action':'expande','node':path})
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
      if node.expandable():
        message = Message({'action':'list','items':node})
        ct.messageFromParent(message)

  #############################################################################  