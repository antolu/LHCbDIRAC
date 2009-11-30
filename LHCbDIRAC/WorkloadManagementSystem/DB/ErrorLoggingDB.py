""" ErrorLoggingDB class is a front-end to the ErrorLogging database
"""

import time
import types
import threading
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

class ErrorLoggingDB(DB):

  def __init__( self, maxQueueSize = 10 ):

    DB.__init__( self, 'ErrorLoggingDB', 'LHCb/ErrorLoggingDB', maxQueueSize )
    self.lock = threading.Lock()

  def setError( self, production, project='', version='', errornumber=0 ):
    """ Create a new error record
    """
    
    parameters = ['ProductionID','Project','Version','ErrorNumber','ErrorDate']
    pString = ','.join(parameters)
    
    evalues = []
    values = [str(production),project,version,str(errornumber)]
    gLogger.info("Values",values)
    for v in values:
      gLogger.info("Value",v)
      result = self._escapeString(v)
      if result['OK']:
        evalues.append(result['Value'])
      else:
        return S_ERROR('Failed to escape value %s' % v)  
    vString = ','.join(evalues)    
    vString += ",UTC_TIMESTAMP()" 
    
    req = "INSERT INTO ErrorLog (%s) VALUES (%s)" % (pString,vString)
    
    print "AT >>>>",req
    
    self.lock.acquire()
    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req,connection)
    self.lock.release()
    if not res['OK']:
      return res
    errorID = int(res['Value'][0][0])
    return S_OK(errorID)
    
  def getErrorAttribute(self,errorID,attribute):
    """ Get error info
    """  
    
    req = "SELECT %s FROM ErrorLog WHERE ErrorID=%d" % (attribute,int(alarmID))
    result = self._query(req)
    if not result['OK']:
      return result
    
    if not result['Value']:
      return S_ERROR('Error %d not found' % int(alarmID))
    
    value = result['Value'][0][0]
    return S_OK(value)
        
  def getErrors(self,production='',project='',version='',errornumber=''):
    """ Check for availability of errors with given properties
    """  
    
    condDict = {}
    if production:
      condDict['ProductionID'] = str(production)
    if project:
      condDict['Project'] = project
    if version:
      condDict['Version'] = version
    if errornumber:
      condDict['ErrorNumber'] = errornumber
      
    order = 'ErrorID'
    
    result = self.selectErrors(condDict,order)
    return result
  
  def selectErrors(self,selectDict,order='',startID=0,startTime='',endTime=''):
    """ 
    """
        
    conditions = self.buildCondition(selectDict, older=startTime, 
                                    newer=endTime, timeStamp='ErrorDate')
    print conditions
    
    if startID:
      if conditions:
        conditions += ' AND ErrorID > %d' % int(startID) 
      else:
        conditions += ' WHERE ErrorID > %d' % int(startID)
        
    parameters = [ 'ErrorID', 'ProductionID', 'Project', 'Version', 'ErrorNumber', 'ErrorDate' ]    
    
    parString = ','.join(parameters)    
        
    req = "SELECT %s FROM ErrorLog " % parString
    req += conditions
#    if order:
#      req += " %s" % order
    result = self._query(req)
    
    print req
    
    if not result['OK']:
      return result
    
    if not result['Value']:
      return S_OK([])

    resultDict = {}
    resultDict['ParameterNames'] = parameters
    resultDict['Records'] = [ list(v) for v in result['Value'] ]
    return S_OK(resultDict)

