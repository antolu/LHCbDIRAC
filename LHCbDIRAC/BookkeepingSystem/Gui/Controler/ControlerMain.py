########################################################################
# $Id: ControlerMain.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $
########################################################################

from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                 import LHCB_BKKDBClient

__RCSID__ = "$Id: ControlerMain.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $"

#############################################################################  
class ControlerMain(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerMain, self).__init__(widget, parent)
    self.__bkClient = LHCB_BKKDBClient()
  
  #############################################################################  
  def messageFromParent(self, message):
    controlers = self.getChildren()
    for controler in controlers:
      ct = controlers[controler]
      ct.messageFromParent(message)
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    if sender.__class__.__name__=='ControlerTree':
      if message['action']=='expande':
        path = message['node']
        items=Item({'fullpath':path},None)
        for entity in self.__bkClient.list(str(path)):
          childItem = Item(entity,items)
          items.addItem(childItem)
        message = Message({'action':'showNode','items':items})
        return message
      
      elif message['action']=='configbuttonChanged':
        self.__bkClient.setParameter('Configuration')
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        items = self.root()
        message = Message({'action':'removeTree','items':items})
        ct.messageFromParent(message)
        
      elif message['action']=='eventbuttonChanged':
        self.__bkClient.setParameter('Event type')
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        items = self.root()
        message = Message({'action':'removeTree','items':items})
        ct.messageFromParent(message)
        
      elif message.action()=='SaveAs':
        fileName = message['fileName']
        lfns = message['lfns']
        self.__bkClient.writeJobOptions(lfns, str(fileName))
        return True
      elif message.action() == 'JobInfo':
        files = self.__bkClient.getJobInfo(message['fileName'])
        message = Message({'action':'showJobInfos','items':files})
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        feedback = ct.messageFromParent(message)
        return feedback
      else:        
        print 'Unknown message!',message.action()
      
  
  def root(self):
    item = self.__bkClient.get()
    items=Item(item,None)
    path = item['Value']['fullpath']
    for entity in self.__bkClient.list(path):
      childItem = Item(entity,items)
      items.addItem(childItem)
    return items
  
  def start(self):
    items = self.root()  
    message = Message({'action':'list','items':items})
    controlers = self.getChildren()
    for controler in controlers:
      if controler == 'TreeWidget':
        ct = controlers[controler]
        ct.messageFromParent(message)
    #message = Message({'action':'list','items':items})
    #self.getControler().messageFromParent(message)
  #############################################################################  
  
  