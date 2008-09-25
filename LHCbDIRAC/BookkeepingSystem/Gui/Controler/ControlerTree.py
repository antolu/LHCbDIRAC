########################################################################
# $Id: ControlerTree.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

__RCSID__ = "$Id: ControlerTree.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $"

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
      #  parentItem.takeChild(0) # I have to remove the dumychildren!
      else:
        message = Message({'action':'expande','node':path})
      
        feedback = self.getParent().messageFromChild(self, message)
        if feedback.action()=='showNode':
          if feedback['items'].childnum() > 0:
            child = feedback['items'].child(0)
            if not child['expandable']:
              controlers = self.getChildren()
              ct = controlers['FileDialog']  
              message = Message({'action':'list','items':feedback['items']})
              ct.messageFromParent(message)
            else:
              self.getWidget().getTree().showTree(feedback['items'], parentItem)
        
      
   
  #############################################################################  
  def _on_item_clicked(self,parentItem):
    node = parentItem.getUserObject()
    parentnode = parentItem.parent()
    if parentnode != None: 
      parent = parentnode.getUserObject()
      controlers = self.getChildren()
      if parent['level'] == 'List of files':
        path = parent['fullpath']
        message = Message({'action':'expande','node':path})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback.action()=='showNode':
          ct = controlers['FileDialog']  
          message = Message({'action':'list','items':feedback['items']})
          ct.messageFromParent(message)
      else:
        ct = controlers['InfoDialog']
        if node <> None:
          if node.expandable():
            message = Message({'action':'list','items':node})
            ct.messageFromParent(message)

  #############################################################################  