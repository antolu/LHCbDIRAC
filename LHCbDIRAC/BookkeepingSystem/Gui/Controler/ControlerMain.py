"""
main controller of the widgets.
"""
########################################################################
# $Id$
########################################################################

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item
from LHCbDIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient                 import LHCB_BKKDBClient
from DIRAC.Interfaces.API.Dirac                                          import Dirac
from DIRAC                                                               import gLogger, S_OK, S_ERROR

import sys

from PyQt4.QtGui                                                         import QMessageBox

__RCSID__ = "$Id$"

#############################################################################
class ControlerMain(ControlerAbstract):
  """
  ControlerMain class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the main controller"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__bkClient = LHCB_BKKDBClient()
    self.__diracAPI = Dirac()

    self.__fileName = ''
    self.__pathfilename = ''
    self.__SelectionDict = {}
    self.__SortDict = {}
    self.__StartItem = 0
    self.__Maxitems = 0
    self.__qualityFlags = {}
    retVal = self.__bkClient.getAvailableDataQuality()
    if not retVal['OK']:
      gLogger.error(retVal['Message'])
    else:
      for i in retVal['Value']:
        if i == "OK":
          self.__qualityFlags[i] = True
        else:
          self.__qualityFlags[i] = False
    #self.__progressBar = ProgressThread(False, 'Query on database...',self.getWidget())
    self.__bkClient.setDataQualities(self.__qualityFlags)
  #############################################################################
  def messageFromParent(self, message):
    """delivers the messages to the children controllers"""
    controlers = self.getChildren()
    for controler in controlers:
      ct = controlers[controler]
      ct.messageFromParent(message)

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the messages sent by the children controllers"""
    gLogger.debug(str(self.__class__)+' Sender' + str(sender.__class__))
    gLogger.debug(str(self.__class__)+' Message'+str(message))

    if sender.__class__.__name__  == 'ControlerTree':
      return self.handleTreewidget(sender, message)
    elif sender.__class__.__name__ == 'ControlerProductionLookup':
      return self.handleProductionlookup(sender, message)
    elif sender.__class__.__name__ == 'ControlerDataQualityDialog':
      return self.handleDataqualitydialog(sender, message)
    else:
      message = "Unknown message sent by %s. Message:%s" % (str(sender.__class__), str(message))
      return S_ERROR(message)

  #############################################################################
  def handleDataqualitydialog(self, sender, message):
    """ it handles the messages sent by the Data quality widget
    """
    if message.action() == 'changeQualities':
      self.__qualityFlags = message['Values']
      self.__bkClient.setDataQualities(self.__qualityFlags)
    else:
      message = "Unknown message sent by %s. Message:%s" % (str(sender.__class__), str(message))
      return S_ERROR(message)

  #############################################################################
  def handleProductionlookup(self, sender, message):
    """It handles the messages sent by the Production Lookup widget.
    """

    if message['action'] == 'configbuttonChanged':
      return self.__configurationbasedquery()

    elif message.action() == 'showAllProduction':
      items = message['items']
      message = Message({'action':'removeTree', 'items':items})
      controlers = self.getChildren()
      ct = controlers['TreeWidget']
      ct.messageFromParent(message)

    elif message.action() == 'error':
      QMessageBox.information(self.getWidget(),
                              "Error", 'Please select a production or more productions!',
                               QMessageBox.Ok)

    elif message.action() == 'showOneProduction':
      paths = message['paths']

      items = Item(paths.pop(), None)
      childItem = Item(items, items)
      items.addItem(childItem)

      for i in paths:
        item1 = Item(i, None)
        childItems = Item(item1, items)
        items.addItem(childItems)

      message = Message({'action':'removeTree', 'items':items})
      controlers = self.getChildren()
      ct = controlers['TreeWidget']
      ct.messageFromParent(message)
    else:
      message = "Unknown message sent by %s. Message:%s" % (str(sender.__class__), str(message))
      gLogger.error(message)
      return S_ERROR(message)

  #############################################################################
  def handleTreewidget(self, sender, message):
    """It handles the messages sent by the tree panel.
    """
    result = None
    if message['action'] == 'configbuttonChanged':
      result = self.__configurationbasedquery()

    elif message['action'] == 'eventbuttonChanged':
      result = self.__eventbasedquery()

    elif message['action'] == 'productionButtonChanged':
      result = self.__productionLookup()

    elif message['action'] == 'runLookup':
      result = self.__runLookup()

    elif message['action'] == 'expande':
      result = self.__expandeTreenode(message)

    elif message.action() == 'SaveAs':
      result = self.__saveAsDataset(message)

    elif message.action() == 'SaveToTxt':
      result = self.__saveToTxtformat(message)

    elif message.action() == 'JobInfo':
      result = self.__getJobInformation(message)

    elif message.action() == 'waitCursor':
      self.getWidget().waitCursor()

    elif message.action() == 'arrowCursor':
      self.getWidget().arrowCursor()

    elif message.action() == 'getNbEventsAndFiles':
      result = self.__getChunkofFiles(message)

    elif message.action() == 'StandardQuery':
      result = self.__handleStandardQuery()

    elif message.action() == 'AdvancedQuery':
      result = self.__handleAdvancedquery()

    elif message.action() == 'GetFileName':
      result = self.__fileName

    elif message.action() == 'GetPathFileName':
      result = self.__pathfilename

    elif message.action() == 'getAnccestors':
      result = self.__getFileAncestor(message)

    elif message.action() == 'logfile':
      result = self.__getLogfile(message)

    elif message.action() == 'procDescription':
      result = self.__getProcessingPass(message)

    elif message.action() == 'createCatalog':
      result = self.__createCatalogMessage(message)

    elif message.action() == 'ProductionInformations':
      result = self.__getProductionSteps(message)

    elif message.action() == 'BookmarksPrefices':
      result = self.__getBookmarksPrefix()

    elif message.action() == "detailedProcessingPassDescription":
      result = self.__getDetailedProcessingPass(message)

    else:
      message = "Unknown message sent by %s. Message:%s" % (str(sender.__class__), str(message))
      gLogger.error(message)
      result = message

    return result

  #############################################################################
  def __expandeTreenode(self, message):
    """It expands a given tree node (It opens a directory)
    """
    self.getWidget().waitCursor()
    path = message['node']
    items = Item({'fullpath':path}, None)

    if message.has_key('StartItem') and message.has_key('MaxItem'):
      self.__StartItem = message['StartItem']
      self.__Maxitems = message['MaxItem']
    for entity in self.__bkClient.list(str(path),
                                       self.__SelectionDict,
                                       self.__SortDict,
                                       self.__StartItem,
                                       self.__Maxitems):
      childItem = Item(entity, items)
      items.addItem(childItem)

    self.getWidget().arrowCursor()
    message = Message({'action':'showNode', 'items':items})
    return message

  #############################################################################
  def __configurationbasedquery(self):
    """It change the type of the Bookkeeping tree.
    """
    self.__bkClient.setParameter('Configuration')
    controlers = self.getChildren()
    ct = controlers['TreeWidget']
    items = self.root()
    message = Message({'action':'removeTree', 'items':items})
    gLogger.debug('ControlerMain:Remove Tree')
    return ct.messageFromParent(message)

  #############################################################################
  def __eventbasedquery(self):
    """It change the type of the Bookkeeping tree
    """
    self.__bkClient.setParameter('Event type')
    controlers = self.getChildren()
    ct = controlers['TreeWidget']
    items = self.root()
    message = Message({'action':'removeTree', 'items':items})
    return ct.messageFromParent(message)

  #############################################################################
  def __productionLookup(self):
    """It change the type of the Bookkeeping tree
    """
    self.__bkClient.setParameter('Productions')
    controlers = self.getChildren()
    items = self.root()
    ctProd = controlers['ProductionLookup']
    message = Message({'action':'list', 'items':items})
    return ctProd.messageFromParent(message)

  #############################################################################
  def __runLookup(self):
    """It change the type of the bookkeeping query.
    """
    self.__bkClient.setParameter('Runlookup')
    controlers = self.getChildren()
    items = self.root()
    ctProd = controlers['ProductionLookup']
    message = Message({'action':'list', 'items':items})
    return ctProd.messageFromParent(message)

  #############################################################################
  def __saveAsDataset(self, message):
    """it creates an gaudi card format file
    """
    dataset = message['dataset']
    if self.__fileName != '':
      fileName = self.__fileName
    else:
      fileName = message['fileName']

    lfns = message['lfns']
    self.__bkClient.writeJobOptions(lfns,
                                    str(fileName),
                                    savedType=None,
                                    catalog=None,
                                    savePfn=None,
                                    dataset=dataset)
    return True

  #############################################################################
  def __saveToTxtformat(self, message):
    """ it save the selected lfns to a given file in a text format
    """
    if self.__fileName != '':
      fileName = self.__fileName
      lfns = message['lfns']
      filedescriptor = open(fileName, 'w')
      for lfn in lfns:
        filedescriptor.write(lfn + '\n')
      sys.exit(0)
    else:
      fileName = message['fileName']
      lfns = message['lfns']
      filedescriptor = open(fileName, 'w')
      for lfn in lfns:
        filedescriptor.write(lfn + '\n')
    return True

  #############################################################################
  def __getJobInformation(self, message):
    """
    It returns the information of a job which created a given file.
    """
    files = self.__bkClient.getJobInfo(message['fileName'])
    message = Message({'action':'showJobInfos', 'items':files})
    return message

  #############################################################################
  def __getChunkofFiles(self, message):
    """
    It used by the File dialog window during the paging.
    It returns only a limited number of files.
    """
    path = message['node']
    result = self.__bkClient.getLimitedFiles({'fullpath':str(path)}, ['nb'], -1, -1)
    return result

  #############################################################################
  def __handleStandardQuery(self):
    """it sets the standard query in the GUI and the Bookkeeping client
    """
    self.__bkClient.setAdvancedQueries(False)
    controlers = self.getChildren()
    ct = controlers['TreeWidget']
    items = self.root()
    message = Message({'action':'removeTree', 'items':items})
    return ct.messageFromParent(message)

  #############################################################################
  def __handleAdvancedquery(self):
    """
    It sets the advanced query in the GUI and the bookkeeping client.
    """
    self.__bkClient.setAdvancedQueries(True)
    items = self.root()
    controlers = self.getChildren()
    ct = controlers['TreeWidget']
    message = Message({'action':'removeTree', 'items':items})
    return ct.messageFromParent(message)

  #############################################################################
  def __getFileAncestor(self, message):
    """It used to navigate through the creation history of files.
    """
    files = message['files']
    if len(files) == 0:
      message = Message({'action':'error', 'message':'Please select a file or files!'})
      return message
    res = self.__bkClient.getFileHistory(files)
    if not res['OK']:
      message = Message({'action':'error', 'message':res['Message']})
      return message
    else:
      return Message({'action':'showAncestors', 'files':res['Value']})

  #############################################################################
  def __getProcessingPass(self, message):
    """It returns the processing pass for a given step description
    """
    desc = message['groupdesc']
    retVal = self.__bkClient.getProcessingPassSteps({'StepName':desc})
    if not retVal['OK']:
      gLogger.error(retVal['Message'])
      return None
    else:
      return retVal['Value']
  #############################################################################
  def __getDetailedProcessingPass(self, message):
    """
    It returns the corresponding steps created by the productions
    """
    bkDict = message['bkDict']
    retVal = self.__bkClient.getStepsMetadata(bkDict)
    if not retVal['OK']:
      gLogger.error(retVal['Message'])
      return None
    else:
      return retVal['Value']

  #############################################################################
  def __getProductionSteps(self, message):
    """It returns the steps and processing passes of a given production.
    """
    res = self.__bkClient.getProductionProcessingPassSteps({'Production':int(message['production'])})
    if res['OK']:
      return res['Value']
    else:
      QMessageBox.information(self.getWidget(), "Error", res['Message'], QMessageBox.Ok)
      return S_ERROR()

  #############################################################################
  def __getBookmarksPrefix(self):
    """It returns the type of the query which can be advanced or standard from the Bookkmarks.
    """
    param = self.__bkClient.getCurrentParameter()
    querytype = self.__bkClient.getQueriesTypes()
    prefix = param + '+' + querytype
    return S_OK(prefix)

  #############################################################################
  def __getLogfile(self, message):
    """It returns the log file of the job which created the selected file.
    """
    files = message['fileName']
    if len(files) == 0:
      message = Message({'action':'error', 'message':'Please select a file'})
      return message
    else:
      res = self.__bkClient.getLogfile(files[0])
      if not res['OK']:
        message = Message({'action':'error', 'message':res['Message']})
        return message
      else:
        value = res['Value']
        files = value.split('/')
        logfile = ''
        for i in range(len(files) - 1):
          if files[i] != '':
            logfile += '/' + str(files[i])

        name = files[len(files) - 1].split('_')

        if logfile.find('/' + str(name[2])) < 0:
          logfile += '/' + str(name[2])

        message = Message({'action':'showLog', 'fileName':logfile})
        return message

  #############################################################################
  def __createCatalogMessage(self, message):
    """It creates a POOL XML catalog for a given lfns.
    """
    if self.__fileName != '':
      lfnList = message['lfns'].keys()
      filedescriptor = open(self.__fileName, 'w')
      for i in lfnList:
        filedescriptor.write(i)
        filedescriptor.write('\n')
      filedescriptor.close()
      sys.exit(0)
    else:
      dataset = message['dataset']
      site = message['selection']['Site']
      filename = message['fileName']
      lfnList = message['lfns'].keys()
      totalFiles = len(lfnList)
      ff = filename.split('.')
      catalog = ff[0] + '.xml'
      retVal = self.__diracAPI.getInputDataCatalog(lfnList, site, catalog, True)
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
          self.__bkClient.writeJobOptions(exist,
                                          filename,
                                          savedType=None,
                                          catalog=catalog,
                                          savePfn=slist,
                                          dataset=dataset)
        else:
          self.__bkClient.writeJobOptions(exist,
                                          filename,
                                          catalog=catalog,
                                          dataset=dataset)
        message = 'Total files:' + str(totalFiles) + '\n'
        if site != None:
          message += str(nbofsuccsessful) + ' found ' + str(site.split('.')[1]) + '\n'
          message += str(nboffaild) + ' not found ' + str(site.split('.')[1])
        QMessageBox.information(self.getWidget(), "Information", message , QMessageBox.Ok)
      else:
        QMessageBox.information(self.getWidget(), "Error", retVal['Message'], QMessageBox.Ok)

  #############################################################################
  def root(self):
    """creates the root node"""
    item = self.__bkClient.get()
    items = Item(item, None)
    path = item['Value']['fullpath']
    retVal = self.__bkClient.list(path)
    if len(retVal) == 0:
      return None
    else:
      for entity in retVal:
        childItem = Item(entity, items)
        items.addItem(childItem)
      return items

  #############################################################################
  def start(self):
    """send the messages to the appropiate controllers"""
    items = self.root()
    if items != None:
      message = Message({'action':'list', 'items':items})
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
    """sets the file name"""
    self.__fileName = fileName

  #############################################################################
  def setPathFileName(self, filename):
    """sets the full file name"""
    self.__pathfilename = filename

  #############################################################################
  def dataQuality(self):
    """handles the data quality related actions"""
    controlers = self.getChildren()
    ct = controlers['DataQuality']
    message = Message({'action':'list', 'Values':self.__qualityFlags})
    ct.messageFromParent(message)

  #############################################################################
  def dataQualityFlagChecked(self, flag, value):
    """handles the actions of the data quality check boxes"""
    self.__qualityFlags[flag] = value

