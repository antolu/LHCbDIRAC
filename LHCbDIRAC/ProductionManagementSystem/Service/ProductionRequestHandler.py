# $Id$
"""
ProductionRequestHandler is the implementation of
the Production Request service
"""
__RCSID__ = "$Revision$"

import DIRAC
import os
import re
import tempfile
import threading

from DIRAC                            import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler  import RequestHandler
from DIRAC.Core.Utilities.Shifter     import getShifterProxy
from DIRAC.Core.Utilities.Subprocess  import shellCall

from LHCbDIRAC.ProductionManagementSystem.DB.ProductionRequestDB import ProductionRequestDB

# This is a global instance of the ProductionRequestDB class
productionRequestDB = False

def initializeProductionRequestHandler( _serviceInfo ):
  global productionRequestDB
  productionRequestDB = ProductionRequestDB()
  return S_OK()

class ProductionRequestHandler( RequestHandler ):

  def __init__( self, *args, **kargs ):

    RequestHandler.__init__( self, *args, **kargs )

    self.database = productionRequestDB
    self.lock     = threading.Lock()

  def __clientCredentials( self ):
    creds = self.getRemoteCredentials()
    group = creds.get( 'group', '(unknown)' )
#    if 'DN' in creds:
#      cn = re.search('/CN=([^/]+)',creds['DN'])
#      if cn:
#        return { 'User':cn.group(1), 'Group':group }
    return { 'User'  : creds.get( 'username' , 'Anonymous' ), 
             'Group' : group }

  types_createProductionRequest = [ dict ]
  def export_createProductionRequest( self, requestDict ):
    """ Create production request
    """
    creds = self.__clientCredentials()
    if not 'MasterID' in requestDict:
      requestDict['RequestAuthor'] = creds['User']
    return self.database.createProductionRequest( requestDict, creds )

  types_getProductionRequest = [ list ]
  def export_getProductionRequest( self, requestIDList ):
    """ Get production request(s) specified by the list of requestIDs
        AZ!!: not tested !! 
    """
    if not requestIDList:
      return S_OK({})
    result = self.database.getProductionRequest( requestIDList )
    if not result['OK']:
      return result
    rows = {}
    for row in result['Value']['Rows']:
      iD = row['RequestID']
      rows[iD] = row
    return S_OK(rows)

  types_getProductionRequestList_v2 = [ long, str, str, long, long, dict ]
  def export_getProductionRequestList_v2( self, subrequestFor, sortBy, sortOrder,
                                          offset, limit, filter ):
    """ Get production requests in list format (for portal grid)
    """
    return self.database.getProductionRequest( [], subrequestFor, sortBy, sortOrder,
                                               offset, limit, filter )

  types_getProductionRequestList = [ long, str, str, long,long ]
  def export_getProductionRequestList( self, subrequestFor, sortBy, sortOrder, offset, limit ):
    """ Get production requests in list format (compat version)
    """
    return self.database.getProductionRequest( [], subrequestFor, sortBy, sortOrder,
                                               offset, limit, {} )

  types_updateProductionRequest = [ long, dict ]
  def export_updateProductionRequest( self, requestID, requestDict ):
    """ Update production request specified by requestID
    """
    creds = self.__clientCredentials()
    return self.database.updateProductionRequest( requestID, requestDict, creds )

  types_duplicateProductionRequest_v2 = [ long, bool ]
  def export_duplicateProductionRequest_v2( self, requestID, clearpp ):
    """ Duplicate production request with subrequests.
    """
    creds = self.__clientCredentials()
    return self.database.duplicateProductionRequest( requestID, creds, clearpp )

  types_duplicateProductionRequest = [ long ]
  def export_duplicateProductionRequest( self, requestID ):
    """ Duplicate production request with subrequests (compat version)
    """
    creds = self.__clientCredentials()
    return self.database.duplicateProductionRequest( requestID, creds, False )

  types_deleteProductionRequest = [ long ]
  def export_deleteProductionRequest( self, requestID ):
    """ Delete production request specified by requestID
    """
    creds = self.__clientCredentials()
    return self.database.deleteProductionRequest( requestID, creds )

  types_splitProductionRequest = [ long, list ]
  def export_splitProductionRequest( self, requestID, splitList ):
    """ split production request
    """
    creds = self.__clientCredentials()
    return self.database.splitProductionRequest( requestID, splitList, creds )

  types_getProductionProgressList = [ long ]
  def export_getProductionProgressList( self, requestID ):
    """ Return the list of associated with requestID productions
    """
    return self.database.getProductionProgress( requestID )

  types_addProductionToRequest = [ dict ]
  def export_addProductionToRequest( self, pdict ):
    """ Associate production to request
    """
    return self.database.addProductionToRequest( pdict )

  types_removeProductionFromRequest = [ long ]
  def export_removeProductionFromRequest( self, productionID ):
    """ Deassociate production
    """
    return self.database.removeProductionFromRequest( productionID )

  types_useProductionForRequest = [ long, bool ]
  def export_useProductionForRequest( self, productionID, used ):
    """ Set Used flags for production
    """
    return self.database.useProductionForRequest( productionID, used )

  types_getRequestHistory = [ long ]
  def export_getRequestHistory( self, requestID ):
    """ Return the list of state changes for the request
    """
    return self.database.getRequestHistory( requestID )

  types_getTrackedProductions = []
  def export_getTrackedProductions( self ):
    """ Return the list of productions in active requests
    """
    return self.database.getTrackedProductions()

  types_updateTrackedProductions = [ list ]
  def export_updateTrackedProductions( self, update ):
    """ Update tracked productions (used by Agent)
    """
    return self.database.updateTrackedProductions( update )

  types_getTrackedInput = []
  def export_getTrackedInput( self ):
    """ Return the list of requests with dynamic input data
    """
    return self.database.getTrackedInput()

  types_updateTrackedInput = [ list ]
  def export_updateTrackedInput( self, update ):
    """ Update real number of input events (used by Agent)
    """
    return self.database.updateTrackedInput( update )

  types_getAllSubRequestSummary = []
  def export_getAllSubRequestSummary( self, status='', type='' ):
    """ Return a summary for each subrequest
    """
    return self.database.getAllSubRequestSummary( status, type )
  
  types_getAllProductionProgress = []
  def export_getAllProductionProgress( self ):
    """ Return all the production progress
    """
    return self.database.getAllProductionProgress()

  def __getTplFolder( self, tt ):
    
    csS = PathFinder.getServiceSection( 'ProductionManagement/ProductionRequest' )
    if not csS:
      return S_ERROR("No ProductionRequest parameters in CS")
    if tt == 'template':
      tplFolder = gConfig.getValue('%s/templateFolder' % csS, '')
      if not tplFolder:
        return S_ERROR("No templateFolder in ProductionRequest parameters in CS")
    else:
      tplFolder = gConfig.getValue('%s/testFolder' % csS, '')
      if not tplFolder:
        return S_ERROR("No testFolder in ProductionRequest parameters in CS")
    if not os.path.exists(tplFolder) or not os.path.isdir(tplFolder):
      return S_ERROR("Template Folder %s doesn't exist" % tplFolder)
    return S_OK(tplFolder)

  def __getTemplate( self, tt, name ):
    ret = self.__getTplFolder( tt )
    if not ret['OK']:
      return ret
    tplFolder = ret['Value']
    if not os.path.exists(os.path.join(tplFolder, name)):
      return S_ERROR("Template %s doesn't exist" % name)
    try:
      f = open(os.path.join(tplFolder, name))
      body = f.read()
      f.close()
    except Exception, e:
      return S_ERROR("Can't read template (%s)" % str(e))
    return S_OK(body)

  def __productionTemplateList( self, tt ):
    """ Return production template list (file based) """
    ret = self.__getTplFolder(tt)
    if not ret['OK']:
      return ret
    tplFolder = ret['Value']
    tpls = [x for x in os.listdir(tplFolder) \
            if os.path.isfile(os.path.join(tplFolder,x))]
    results = []
    for t in tpls:
      if t[-1] == '~':
        continue
      result = self.__getTemplate( tt, t )
      if not result['OK']:
        return result
      body = result['Value']
      m = re.search( "\$\Id: ([^$]*) \$", body )
      ptime = ''
      author = ''
      ver = ''
      if m:
        m = re.match( "[^ ]+ ([^ ]+) ([^ ]+ [^ ]+) ([^ ]+)", m.group(1) )
        if m:
          ptime = m.group(2)
          author = m.group(3)
          ver = m.group(1)
          tpl = { "AuthorGroup"     : '', 
                  "Author"          : author, 
                  "PublishingTime"  : ptime,
                  "LongDescription" : '', 
                  "WFName"          : t, 
                  "AuthorDN"        : '',
                  "WFParent"        : '', 
                  "Description"     : ver }
          results.append(tpl)
    return S_OK(results)

  types_getProductionTemplateList = []
  def export_getProductionTemplateList( self ):
    """ Return production template list (file based) """
    return self.__productionTemplateList( 'template' )

  types_getProductionTestList = []
  def export_getProductionTestList(self):
    """ Return production tests list (file based) """
    return self.__productionTemplateList('test')

  types_getProductionTemplate = [ str ]
  def export_getProductionTemplate( self, name ):
    return self.__getTemplate( 'template', name )

  types_getProductionTest = [ str ]
  def export_getProductionTest( self, name ):
    return self.__getTemplate( 'test', name )

  types_execProductionScript = [ str, str ]
  def export_execProductionScript( self, script, workflow ):
    creds = self.__clientCredentials()
    if creds['Group'] != 'lhcb_prmgr':
      return S_ERROR("You have to be production manager")
    res = getShifterProxy("ProductionManager")
    if not res['OK']:
      return res
    proxyFile = res['Value']['proxyFile']
    try:
      f = tempfile.mkstemp()
      os.write(f[0], workflow)
      os.close(f[0])
      fs = tempfile.mkstemp()
      os.write(fs[0], script)
      os.close(fs[0])
    except Exception, msg:
      gLogger.error("In temporary files createion: "+str(msg))
      os.remove(proxyFile)
      return S_ERROR(str(msg))
    setenv = "source /opt/dirac/bashrc"
    proxy = "X509_USER_PROXY=%s" % proxyFile
    cmd = "python %s %s" % (fs[1], f[1])
    try:
      res = shellCall(1800, [ "/bin/bash -c '%s;%s %s'" \
                                   % (setenv,proxy,cmd) ])
      if res['OK']:
        result = S_OK(str(res['Value'][1])+str(res['Value'][2]))
      else:
        gLogger.error(res['Message'])
        result = res
    except Exception, msg:
      gLogger.error("During execution: "+str(msg))
      result = S_ERROR("Failed to execute: %s" % str(msg))
    os.remove(f[1])
    os.remove(fs[1])
    os.remove(proxyFile)
    return result

  types_execWizardScript = [ str, dict ]
  def export_execWizardScript( self, wizard, wizpar ):
    """ Execure wizard with parameters """
    creds = self.__clientCredentials()
    if creds['Group'] != 'lhcb_prmgr':
      #return S_ERROR("You have to be production manager")
      if 'Generate' in wizpar:
        del wizpar['Generate']
    res = getShifterProxy("ProductionManager")
    if not res['OK']:
      return res
    proxyFile = res['Value']['proxyFile']
    try:
      f = tempfile.mkstemp()
      os.write(f[0], "wizardParameters = {\n")
      for name, value in wizpar.items():
        os.write(f[0], "  \""+str(name)+"\": \"\"\""+str(value)+"\"\"\",\n")
      os.write(f[0], "}\n")
      os.write(f[0], wizard)
      os.close(f[0])
    except Exception, msg:
      gLogger.error("In temporary files createion: "+str(msg))
      os.remove(proxyFile)
      return S_ERROR(str(msg))
    setenv = "source /opt/dirac/bashrc"
    ##proxy = "X509_USER_PROXY=xxx"
    proxy = "X509_USER_PROXY=%s" % proxyFile
    cmd = "python %s" % (f[1])
    try:
      res = shellCall(1800, [ "/bin/bash -c '%s;%s %s'" \
                                   % (setenv,proxy,cmd) ])
      if res['OK']:
        result = S_OK(str(res['Value'][1])+str(res['Value'][2]))
      else:
        gLogger.error(res['Message'])
        result = res
    except Exception, msg:
      gLogger.error("During execution: "+str(msg))
      result = S_ERROR("Failed to execute: %s" % str(msg))
    os.remove(f[1])
    os.remove(proxyFile)
    return result

  types_getProductionList = [ long ]
  def export_getProductionList( self, requestID ):
    """ Return the list of productions associated with request and
        its subrequests
    """
    return self.database.getProductionList(requestID)

  types_getProductionRequestSummary = [ str, str ]
  def export_getProductionRequestSummary( self, status, requestType ):
    """ Method to retrieve the production / request relations for a given request status.
    """
    reqList = self.database.getProductionRequest( [], long(0), '', '', long(0), long(0) )
    if not reqList['OK']:
      return reqList

    requests = reqList['Value']
    resultDict = {}
    reqTypes = [requestType]
    selectStatus = [status]

    for req in requests['Rows']:
      iD = int(req['RequestID'])
      if not req['RequestType'] in reqTypes:
        gLogger.verbose('Skipping %s request ID %s...' %(req['RequestType'], iD))
        continue
      if not req['RequestState'] in selectStatus:
        gLogger.verbose('Skipping request ID %s in state %s' %(iD, req['RequestState']))
        continue
      if req['HasSubrequest']:
        gLogger.verbose('Simulation request %s is a parent, getting subrequests...' %iD)
        subReq = self.database.getProductionRequest([], long(iD), '', '', long(0), long(0))
        if not subReq['OK']:
          gLogger.error('Could not get production request for %s' %iD)
          return subReq
        for sreq in subReq['Value']['Rows']:
          sid = int(sreq['RequestID'])
          resultDict[sid] = { 'reqTotal' : sreq['rqTotal'],
                              'bkTotal'  : sreq['bkTotal'],
                              'master'   : iD }
      else:
        gLogger.verbose('Simulation request %s is a single request' %iD)
        resultDict[iD] = { 'reqTotal' : req['rqTotal'],
                           'bkTotal'  : req['bkTotal'],
                           'master'   : 0}

    return S_OK(resultDict)

  types_getFilterOptions = []
  def export_getFilterOptions(self):
    """ Return the dictionary with possible values for filter
    """
    return self.database.getFilterOptions()

  types_getTestList = [ long ]
  def export_getTestList(self, requestID):
    """ Get production requests in list format (for portal grid)
    """
    return self.database.getTestList(requestID)

  types_submitTest = [ dict, dict, str, str ]
  def export_submitTest(self, input, pars, script, tpl):
    """ Save the test request in the database
    """
    creds = self.__clientCredentials()
    return self.database.submitTest(creds, input, pars, script, tpl)

  types_getTests = [ str ]
  def export_getTests(self, state):
    """ Return the list of tests in specified state
    """
    return self.database.getTests(state)

  types_setTestResult = [ long, str, str ]
  def export_setTestResult(self, requestID, state, link):
    """ Set test result (to be called by test agent) """
    creds = self.__clientCredentials()
    return self.database.setTestResult(requestID, state, link)
