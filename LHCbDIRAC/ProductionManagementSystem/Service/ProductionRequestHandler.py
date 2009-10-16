# $Id: ProductionRequestHandler.py,v 1.10 2009/10/16 15:39:28 azhelezo Exp $
"""
ProductionRequestHandler is the implementation of
the Production Request service
"""
__RCSID__ = "$Revision: 1.10 $"

import os
import re
import tempfile

from types import *
import threading
import DIRAC
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionRequestDB import ProductionRequestDB
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

from DIRAC.Core.Utilities.Shifter import getShifterProxy

# This is a global instance of the ProductionRequestDB class
productionRequestDB = False

def initializeProductionRequestHandler( serviceInfo ):
  global productionRequestDB
  productionRequestDB = ProductionRequestDB()
  return S_OK()

class ProductionRequestHandler( RequestHandler ):

  def __init__(self,*args,**kargs):

    RequestHandler.__init__(self, *args,**kargs)

    self.database = productionRequestDB
    self.lock = threading.Lock()


  def __clientCredentials(self):
    creds = self.getRemoteCredentials()
    group = creds.get('group','(unknown)')
#    if 'DN' in creds:
#      cn = re.search('/CN=([^/]+)',creds['DN'])
#      if cn:
#        return { 'User':cn.group(1), 'Group':group }
    return { 'User':creds.get('username','Anonymous'), 'Group':group }

  types_createProductionRequest = [DictType]
  def export_createProductionRequest(self,requestDict):
    """ Create production request
    """
    creds = self.__clientCredentials()
    if not 'MasterID' in requestDict:
      requestDict['RequestAuthor'] = creds['User']
    return self.database.createProductionRequest(requestDict,creds)

  types_getProductionRequest = [ListType]
  def export_getProductionRequest(self,requestIDList):
    """ Get production request(s) specified by the list of requestIDs
        AZ!!: not tested !! 
    """
    if not requestIDList:
      return S_OK({})
    result = self.database.getProductionRequest(requestIDList)
    if not result['OK']:
      return result
    rows = {}
    for row in result['Value']['Rows']:
      id = row['RequestID']
      rows[id] = row
    return S_OK(rows)

  types_getProductionRequestList = [LongType,StringType,StringType,
                                    LongType,LongType]
  def export_getProductionRequestList(self,subrequestFor,
                                      sortBy,sortOrder,offset,limit):
    """ Get production requests in list format (for portal grid)
    """
    return self.database.getProductionRequest([],subrequestFor,
                                             sortBy,sortOrder,
                                             offset,limit)

  types_updateProductionRequest = [LongType,DictType]
  def export_updateProductionRequest(self,requestID,requestDict):
    """ Update production request specified by requestID
    """
    creds = self.__clientCredentials()
    return self.database.updateProductionRequest(requestID,requestDict,creds)

  types_duplicateProductionRequest = [LongType]
  def export_duplicateProductionRequest(self,requestID):
    """ Duplicate production request with subrequests.
    """
    creds = self.__clientCredentials()
    return self.database.duplicateProductionRequest(requestID,creds)

  types_deleteProductionRequest = [LongType]
  def export_deleteProductionRequest(self,requestID):
    """ Delete production request specified by requestID
    """
    creds = self.__clientCredentials()
    return self.database.deleteProductionRequest(requestID,creds)

  types_splitProductionRequest = [LongType,ListType]
  def export_splitProductionRequest(self,requestID,splitList):
    """ split production request
    """
    creds = self.__clientCredentials()
    return self.database.splitProductionRequest(requestID,splitList,creds)

  types_getProductionProgressList = [LongType]
  def export_getProductionProgressList(self,requestID):
    """ Return the list of associated with requestID productions
    """
    return self.database.getProductionProgress(requestID)

  types_addProductionToRequest = [DictType]
  def export_addProductionToRequest(self,pdict):
    """ Associate production to request
    """
    return self.database.addProductionToRequest(pdict)

  types_removeProductionFromRequest = [LongType]
  def export_removeProductionFromRequest(self,productionID):
    """ Deassociate production
    """
    return self.database.removeProductionFromRequest(productionID)

  types_useProductionForRequest = [LongType,BooleanType]
  def export_useProductionForRequest(self,productionID,used):
    """ Set Used flags for production
    """
    return self.database.useProductionForRequest(productionID,used)

  types_getRequestHistory = [LongType]
  def export_getRequestHistory(self,requestID):
    """ Return the list of state changes for the request
    """
    return self.database.getRequestHistory(requestID)

  types_getTrackedProductions = []
  def export_getTrackedProductions(self):
    """ Return the list of productions in active requests
    """
    return self.database.getTrackedProductions()

  types_updateTrackedProductions = [ListType]
  def export_updateTrackedProductions(self,update):
    """ Update tracked productions (used by Agent)
    """
    return self.database.updateTrackedProductions(update)

  types_getAllSubRequestSummary = []
  def export_getAllSubRequestSummary(self,status='',type=''):
    """ Return a summary for each subrequest
    """
    return self.database.getAllSubRequestSummary(status,type)
  
  types_getAllProductionProgress = []
  def export_getAllProductionProgress(self):
    """ Return all the production progress
    """
    return self.database.getAllProductionProgress()

  def __getTplFolder(self):
    csS=PathFinder.getServiceSection( 'ProductionManagement/ProductionRequest' )
    if not csS:
      return S_ERROR("No ProductionRequest parameters in CS")
    tplFolder = gConfig.getValue('%s/templateFolder' % csS,'')
    if not tplFolder:
      return S_ERROR("No templateFolder in ProductionRequest parameters in CS")
    if not os.path.exists(tplFolder) or not os.path.isdir(tplFolder):
      return S_ERROR("Template Folder %s doesn't exist" % tplFolder)
    return S_OK(tplFolder)

  def __getTemplate(self,name):
    ret = self.__getTplFolder()
    if not ret['OK']:
      return ret
    tplFolder = ret['Value']
    if not os.path.exists(os.path.join(tplFolder,name)):
      return S_ERROR("Template %s doesn't exist" % name)
    try:
      f = open(os.path.join(tplFolder,name))
      body = f.read()
      f.close()
    except Exception, e:
      return S_ERROR("Can't read template (%s)" % str(e))
    return S_OK(body)

  types_getProductionTemplateList = []
  def export_getProductionTemplateList(self):
    """ Return production template list (file based) """
    ret = self.__getTplFolder()
    if not ret['OK']:
      return ret
    tplFolder = ret['Value']
    tpls = [x for x in os.listdir(tplFolder) \
            if os.path.isfile(os.path.join(tplFolder,x))]
    results = []
    for t in tpls:
      if t[-1] == '~':
        continue
      result = self.__getTemplate(t)
      if not result['OK']:
        return result
      body = result['Value']
      m = re.search("\$\Id: ([^$]*) \$",body)
      ptime = ''
      author = ''
      ver = ''
      if m:
        m = re.match("[^ ]+ ([^ ]+) ([^ ]+ [^ ]+) ([^ ]+) Exp",m.group(1))
        if m:
          ptime = m.group(2)
          author = m.group(3)
          ver = m.group(1)
          tpl = { "AuthorGroup": '', "Author": author, "PublishingTime": ptime,
                  "LongDescription": '', "WFName": t, "AuthorDN": '',
                  "WFParent": '', "Description": ver }
          results.append(tpl)
    return S_OK(results)

  types_getProductionTemplate = [StringType]
  def export_getProductionTemplate(self,name):
    return self.__getTemplate(name)

  types_execProductionScript = [StringType,StringType]
  def export_execProductionScript(self,script,workflow):
    creds = self.__clientCredentials()
    if creds['Group'] != 'lhcb_prmgr':
      return S_ERROR("You have to be production manager")
    res = getShifterProxy("ProductionManager")
    if not res['OK']:
      return res
    proxyFile = res['Value']['proxyFile']
    try:
      f = tempfile.mkstemp()
      os.write(f[0],workflow)
      os.close(f[0])
      fs= tempfile.mkstemp()
      os.write(fs[0],script)
      os.close(fs[0])
    except Exception,msg:
      gLogger.error("In temporary files createion: "+str(msg))
      os.remove(proxyFile)
      return S_ERROR(str(msg))
    setenv = "source /opt/dirac/bashrc"
    proxy = "X509_USER_PROXY=%s" % proxyFile
    cmd = "python %s %s" % (fs[1],f[1])
    try:
      res = DIRAC.shellCall(1800,[ "/bin/bash -c '%s;%s %s'" \
                                   % (setenv,proxy,cmd) ])
      if res['OK']:
        result = S_OK(str(res['Value'][1])+str(res['Value'][2]))
      else:
        gLogger.error(res['Message'])
        result = res
    except Exception,msg:
      gLogger.error("During execution: "+str(msg))
      result = S_ERROR("Failed to execute: %s" % str(msg))
    os.remove(f[1])
    os.remove(fs[1])
    os.remove(proxyFile)
    return result

  types_getProductionList = [LongType]
  def export_getProductionList(self,requestID):
    """ Return the list of productions associated with request and
        its subrequests
    """
    return self.database.getProductionList(requestID)

  types_getProductionRequestSummary = [StringType,StringType]
  def export_getProductionRequestSummary(self,status,requestType):
    """ Method to retrieve the production / request relations for a given request status.
    """
    reqList = self.database.getProductionRequest([],long(0),'','',long(0),long(0))
    if not reqList['OK']:
      return reqList

    requests = reqList['Value']
    resultDict = {}
    reqTypes = [requestType]
    selectStatus = [status]

    for req in requests['Rows']:
      id = int(req['RequestID'])
      if not req['RequestType'] in reqTypes:
        gLogger.verbose('Skipping %s request ID %s...' %(req['RequestType'],id))
        continue
      if not req['RequestState'] in selectStatus:
        gLogger.verbose('Skipping request ID %s in state %s' %(id,req['RequestState']))
        continue
      if req['HasSubrequest']:
        gLogger.verbose('Simulation request %s is a parent, getting subrequests...' %id)
        subReq = self.database.getProductionRequest([],long(id),'','',long(0),long(0))
        if not subReq['OK']:
          gLogger.error('Could not get production request for %s' %id)
          return subReq
        for sreq in subReq['Value']['Rows']:
          sid = int(sreq['RequestID'])
          resultDict[sid] = { 'reqTotal':sreq['rqTotal'],'bkTotal':sreq['bkTotal'],'master':id }
      else:
        gLogger.verbose('Simulation request %s is a single request' %id)
        resultDict[id] = {'reqTotal':req['rqTotal'],'bkTotal':req['bkTotal'],'master':0}

    return S_OK(resultDict)
