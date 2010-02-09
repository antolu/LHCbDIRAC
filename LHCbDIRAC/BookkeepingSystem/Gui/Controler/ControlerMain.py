########################################################################
# $Id$
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item
from LHCbDIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                 import LHCB_BKKDBClient
from LHCbDIRAC.BookkeepingSystem.Gui.ProgressBar.ProgressThread          import ProgressThread

from DIRAC.Interfaces.API.Dirac                                      import Dirac
from DIRAC                                                           import gLogger, S_OK, S_ERROR

import sys

from PyQt4.QtGui                                                     import *

__RCSID__ = "$Id$"

#############################################################################  
class ControlerMain(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerMain, self).__init__(widget, parent)
    self.__bkClient = LHCB_BKKDBClient()
    self.__diracAPI = Dirac()

    self.__fileName = ''
    self.__pathfilename = ''
    self.__SelectionDict = {}
    self.__SortDict = {} 
    self.__StartItem = 0
    self.__Maxitems = 0
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
        #, self.__SelectionDict, self.__SortDict, self.__StartItem, self.__Maxitems
        if message.has_key('StartItem') and message.has_key('MaxItem'):
          self.__StartItem = message['StartItem']
          self.__Maxitems = message['MaxItem']
        for entity in self.__bkClient.list(str(path), self.__SelectionDict, self.__SortDict, self.__StartItem, self.__Maxitems):
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
        
      elif message['action'] == 'productionButtonChanged':
        self.__bkClient.setParameter('Productions')
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        items = self.root()
        ctProd = controlers['ProductionLookup']
        message = Message({'action':'list','items':items})
        ctProd.messageFromParent(message)
      
      elif message['action'] == 'runLookup':
        self.__bkClient.setParameter('Runlookup')
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        items = self.root()
        ctProd = controlers['ProductionLookup']
        message = Message({'action':'list','items':items})
        ctProd.messageFromParent(message)
        
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
        return message
        #controlers = self.getChildren()
        #ct = controlers['TreeWidget']
        #feedback = ct.messageFromParent(message)
        #return feedback
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
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        items = self.root()
        message = Message({'action':'removeTree','items':items})
        ct.messageFromParent(message)
        
      elif message.action() == 'AdvancedQuery':
        self.__bkClient.setAdvancedQueries(True)
        items = self.root()
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        message = Message({'action':'removeTree','items':items})
        ct.messageFromParent(message)
        
      elif message.action() == 'GetFileName':
        return self.__fileName
      elif message.action() == 'GetPathFileName':
        return self.__pathfilename
      elif message.action() == 'getAnccestors':
        files = message['files']
        if len(files) == 0:
          message = Message({'action':'error','message':'Please select a file or files!'})
          return message
        res = self.__bkClient.getFileHistory(files)
        if not res['OK']:
          message = Message({'action':'error','message':res['Message']})
          return message
        else:
          return Message({'action':'showAncestors','files':res['Value']})
        
      elif message.action() == 'logfile':
        files = message['fileName']
        if len(files)==0:
          message = Message({'action':'error','message':'Please select a file'})
          return message
        else:
          res = self.__bkClient.getLogfile(files[0])
          if not res['OK']:
            message = Message({'action':'error','message':res['Message']})
            return message
          else:
            value = res['Value']
            f = value.split('/')
            logfile = ''
            for i in range(len(f)-1):
              if f[i] != '':
                logfile += '/'+str(f[i])
            
            name = f[len(f)-1].split('_')
            
            if logfile.find('/'+str(name[2])) < 0:
              logfile += '/'+str(name[2])
            
            message = Message({'action':'showLog','fileName':logfile})
            return message
      elif message.action() == 'procDescription':
        desc = message['groupdesc']
        passdesc = ''
        retVal = self.__bkClient.getProcessingPassDesc_new(desc)
        if not retVal['OK']:
          gLogger.error(retVal['Message'])
          return None   
        else:
          return retVal['Value']
      
      elif message.action() == 'createCatalog': 
        site = message['selection']['Site']
        filename = message['fileName']
        lfnList = message['lfns'].keys()
        totalFiles = len(lfnList)
        ff = filename.split('.')
        catalog = ff[0]+'.xml'
        retVal = self.__diracAPI.getInputDataCatalog(lfnList,site,catalog, True)
        nbofsuccsessful = 0
        if retVal['OK']:
          slist = retVal['Successful']
          faild = retVal['Failed']
          nbofsuccsessful = len(slist)
          nboffaild = len(faild)
          exist = {}
          lfns = message['lfns']
          for i in slist.keys():
            exist[i] = lfns[i]
          if message['selection']['pfn']:
            self.__bkClient.writeJobOptions(exist, filename, savedType = None, catalog= catalog, savePfn=slist)
          else:
            self.__bkClient.writeJobOptions(exist, filename, catalog= catalog)
          m = 'Total files:'+str(totalFiles)+'\n'
          if site != None:
            m += str(nbofsuccsessful)+' found '+str(site.split('.')[1])+'\n'
            m += str(nboffaild)+ ' not found '+str(site.split('.')[1])
          QMessageBox.information(self.getWidget(), "Information", m ,QMessageBox.Ok)
        else:
          QMessageBox.information(self.getWidget(), "Error", retVal['Message'], QMessageBox.Ok)
      
      elif message.action()=='ProductionInformations': 
        res = self.__bkClient.getProcessingPassDescfromProduction(int(message['production']))
        if res['OK']:
          return res['Value']
        else:
          QMessageBox.information(self.getWidget(), "Error", res['Message'], QMessageBox.Ok)
          return S_ERROR()
        
      elif message.action() == 'BookmarksPrefices':
        param = self.__bkClient.getCurrentParameter()
        type = self.__bkClient.getQueriesTypes()
        prefix = param +'+' + type
        return S_OK(prefix)
      else:        
        gLogger.error('Unknown message!',message.action())
        return S_ERROR('Unknown message!')
    
    elif sender.__class__.__name__=='ControlerProductionLookup':
      if message.action() =='showAllProduction':
        items = message['items']
        message = Message({'action':'removeTree','items':items})
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        ct.messageFromParent(message)        
      
      elif message.action() == 'error':
        QMessageBox.information(self.getWidget(), "Error", 'Please select a production or more productions!', QMessageBox.Ok)
      
      elif message.action() == 'showOneProduction':
        paths = message['paths']

        items=Item(paths.pop(),None)
        childItem = Item(items,items)
        items.addItem(childItem)
        
        for i in paths:
          item1=Item(i,None)
          childItems = Item(item1,items)
          items.addItem(childItems)

        message = Message({'action':'removeTree','items':items})
        controlers = self.getChildren()
        ct = controlers['TreeWidget']
        ct.messageFromParent(message)        
      else:
        print 'Unknown message!',message.action(),message
      
  #############################################################################  
  def root(self):
    item = self.__bkClient.get()
    items=Item(item,None)
    path = item['Value']['fullpath']
    retVal = self.__bkClient.list(path)
    if len(retVal) == 0:
      return None
    else:
      for entity in retVal:
        childItem = Item(entity,items)
        items.addItem(childItem)
      return items
  
  #############################################################################  
  def start(self):
    items = self.root()  
    if items != None:
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
  
  #############################################################################  
  def setPathFileName(self, filename):
    self.__pathfilename = filename
  