########################################################################
# $Id: ControlerMain.py,v 1.9 2008/12/01 17:18:56 zmathe Exp $
########################################################################

from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                 import LHCB_BKKDBClient
from DIRAC.BookkeepingSystem.Gui.ProgressBar.ProgressThread          import ProgressThread
from DIRAC                                                           import gLogger, S_OK, S_ERROR
import sys
__RCSID__ = "$Id: ControlerMain.py,v 1.9 2008/12/01 17:18:56 zmathe Exp $"

#############################################################################  
class ControlerMain(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerMain, self).__init__(widget, parent)
    self.__bkClient = LHCB_BKKDBClient()
    self.__fileName = ''
    #self.__progressBar = ProgressThread(False, 'Query on database...',self.getWidget())

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
        '''
        if self.__progressBar.isRunning():
          gLogger.info('2')
          self.__progressBar.stop()
          self.__progressBar.wait()
        gLogger.info('3')
        self.__progressBar.start()
        gLogger.info('4')
        '''
        self.getWidget().waitCursor()
        path = message['node']
        items=Item({'fullpath':path},None)
        for entity in self.__bkClient.list(str(path)):
          childItem = Item(entity,items)
          items.addItem(childItem)
        #self.__progressBar.stop()
        #self.__progressBar.wait()
        
        self.getWidget().arrowCursor()
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
        if self.__fileName <> '':
          fileName = self.__fileName
        else:
           fileName = message['fileName']
     
        lfns = message['lfns']
        self.__bkClient.writeJobOptions(lfns, str(fileName))
        return True
      elif message.action()=='SaveToTxt':
        if self.__fileName <> '':
          fileName = self.__fileName
          lfns = message['lfns']
          f = open(fileName,'w')
          for file in lfns:
            f.write(file+'\n')       
          sys.exit(0)
        else:
          fileName = message['fileName']
          lfns = message['lfns']
          f = open(fileName,'w')
          for file in lfns:
            f.write(file+'\n')       
        return True
          
      elif message.action() == 'JobInfo':
        files = self.__bkClient.getJobInfo(message['fileName'])
        message = Message({'action':'showJobInfos','items':files})
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        feedback = ct.messageFromParent(message)
        return feedback
      elif message.action() == 'waitCursor':
        self.getWidget().waitCursor()
        return True
      elif message.action() == 'arrowCursor':
        self.getWidget().arrowCursor()
        return True
      
      elif message.action() == 'getNbEventsAndFiles':
        path = message['node']
        result = self.__bkClient.getLimitedFiles({'fullpath':str(path)},['nb'],-1,-1)
        return result
      elif message.action() == 'StandardQuery':
        self.__bkClient.setAdvancedQueries(False)
      elif message.action() == 'AdvancedQuery':
        self.__bkClient.setAdvancedQueries(True)
      elif message.action() == 'GetFileName':
        return self.__fileName
      else:        
        print 'Unknown message!',message.action()
      
  #############################################################################  
  def root(self):
    item = self.__bkClient.get()
    items=Item(item,None)
    path = item['Value']['fullpath']
    for entity in self.__bkClient.list(path):
      childItem = Item(entity,items)
      items.addItem(childItem)
    return items
  
  #############################################################################  
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
  
  #############################################################################  
  def setFileName(self, fileName):
    self.__fileName = fileName
  