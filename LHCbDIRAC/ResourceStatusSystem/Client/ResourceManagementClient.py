# $HeadURL$
''' ResourceManagementClient

  Extension for the DIRAC version of the ResourceManagementClient.
  
'''

from datetime import datetime

from DIRAC import S_ERROR, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
     ResourceManagementClient as DIRACResourceManagementClient

__RCSID__ = '$Id$'

class ResourceManagementClient( DIRACResourceManagementClient ):

  """
  The :class:`ResourceManagementClient` class extends the client on DIRAC.

  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - get
   - delete

  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.

  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceManagementDB` and
  :class:`ResourceManagementHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceManagementSystem.Client.ResourceManagementClient import ResourceManagementClient
   >>> rsClient = ResourceManagementClient()

  All functions calling methods exposed on the database or on the booster are
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.
  """

  def __query( self, queryType, tableName, kwargs ):
    '''
      This method is a rather important one. It will format the input for the DB
      queries, instead of doing it on a decorator. Two dictionaries must be passed
      to the DB. First one contains 'columnName' : value pairs, being the key
      lower camel case. The second one must have, at lease, a key named 'table'
      with the right table name. 
    '''
    # Functions we can call, just a light safety measure.
    _gateFunctions = [ 'insert', 'update', 'get', 'delete' ] 
    if not queryType in _gateFunctions:
      return S_ERROR( '"%s" is not a proper gate call' % queryType )
    
    gateFunction = getattr( self.gate, queryType )
    
    meta   = kwargs.pop( 'meta' )
    params = kwargs
    del params[ 'self' ]     
        
    meta[ 'table' ] = tableName
    
    gLogger.info( 'Calling %s, with \n params %s \n meta %s' % ( queryType, params, meta ) )  
    return gateFunction( params, meta )

  def insertMonitoringTest( self, metricName, serviceURI, siteName,
                            serviceFlavour, metricStatus, summaryData,
                            timestamp, lastCheckTime, meta = {} ):
    return self.__query( 'insert', 'MonitoringTest', locals() )
  def updateMonitoringTest( self, metricName, serviceURI, siteName,
                            serviceFlavour, metricStatus, summaryData,
                            timestamp, lastCheckTime, meta = {} ):
    return self.__query( 'update', 'MonitoringTest', locals() )
  def getMonitoringTest( self, metricName = None, serviceURI = None,
                         siteName = None, serviceFlavour = None,
                         metricStatus = None, summaryData = None,
                         timestamp = None, lastCheckTime = None, meta = {} ):
    return self.__query( 'get', 'MonitoringTest', locals() )
  def deleteMonitoringTest( self, metricName = None, serviceURI = None,
                            siteName = None, serviceFlavour = None,
                            metricStatus = None, summaryData = None,
                            timestamp = None, lastCheckTime = None, meta = {} ):
    return self.__query( 'delete', 'MonitoringTest', locals() )
  def addOrModifyMonitoringTest( self, metricName, serviceURI, siteName,
                                 serviceFlavour, metricStatus, summaryData,
                                 timestamp, lastCheckTime ):
    return self.__addOrModifyElement( 'MonitoringTest', locals() )


  def insertHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter,
                             meta = {} ):
    return self.__query( 'insert', 'HammerCloudTest', locals() )
  def updateHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter,
                             meta = {} ):
    return self.__query( 'update', 'HammerCloudTest', locals() )
  def getHammerCloudTest( self, testID = None, siteName = None,
                          resourceName = None, testStatus = None,
                          submissionTime = None, startTime = None,
                          endTime = None, counterTime = None,
                          agentStatus = None, formerAgentStatus = None,
                          counter = None, meta = {} ):
    return self.__query( 'get', 'HammerCloudTest', locals() )
  def deleteHammerCloudTest( self, testID = None, siteName = None,
                             resourceName = None, testStatus = None,
                             submissionTime = None, startTime = None,
                             endTime = None, counterTime = None,
                             agentStatus = None, formerAgentStatus = None,
                             counter = None, meta = {} ):
    return self.__query( 'delete', 'HammerCloudTest', locals() )
  def addOrModifyHammerCloudTest( self, testID, siteName, resourceName,
                                  testStatus, submissionTime, startTime,
                                  endTime, counterTime, agentStatus,
                                  formerAgentStatus, counter ):
    return self.__addOrModifyElement( 'HammerCloudTest', locals() )
  
  def insertSLSTest( self, testName, target, availability, result, description, 
                     dateEffective, meta = {} ):
    return self.__query( 'insert', 'SLSTest', locals() )
  def updateSLSTest( self, testName, target, availability, result, description, 
                     dateEffective, meta = {} ):
    return self.__query( 'update', 'SLSTest', locals() )
  def getSLSTest( self, testName = None, target = None, availability = None, 
                  result = None, description = None, dateEffective = None, 
                  meta = {} ):
    return self.__query( 'get', 'SLSTest', locals() )
  def deleteSLSTest( self, testName = None, target = None, availability = None, 
                     result = None, description = None, dateEffective = None, 
                     meta = {} ):
    return self.__query( 'delete', 'SLSTest', locals() )
  def addOrModifySLSTest( self, testName, target, availability, result,
                          description, dateEffective ):
    return self.__addOrModifyElement( 'SLSTest', locals() )

  def insertSLSService( self, system, service, timeStamp, availability,
                        serviceUptime, hostUptime, instantLoad, message, meta = {} ):
    return self.__query( 'insert', 'SLSService', locals() )
  def updateSLSService( self, system, service, timeStamp, availability,
                        serviceUptime, hostUptime, instantLoad, message, meta = {} ):
    return self.__query( 'update', 'SLSService', locals() )
  def getSLSService( self, system = None, service = None, timeStamp = None,
                     availability = None, serviceUptime = None,
                     hostUptime = None, instantLoad = None, message = None, meta = {} ):
    return self.__query( 'get', 'SLSService', locals() )
  def deleteSLSService( self, system = None, service = None, timeStamp = None,
                        availability = None, serviceUptime = None,
                        hostUptime = None, instantLoad = None, message = None, meta = {} ):
    return self.__query( 'delete', 'SLSService', locals() )
  def addOrModifySLSService( self, system, service, timeStamp, availability,
                             serviceUptime, hostUptime, instantLoad, message):
    return self.__addOrModifyElement( 'SLSService', locals() )
  
  
  def insertSLST1Service( self, site, service, timeStamp, availability,
                          serviceUptime, hostUptime, message, meta = {} ):
    return self.__query( 'insert', 'SLST1Service', locals() )
  def updateSLST1Service( self, site, service, timeStamp, availability,
                          serviceUptime, hostUptime, message, meta = {} ):
    return self.__query( 'update', 'SLST1Service', locals() )
  def getSLST1Service( self, site = None, service = None, timeStamp = None,
                       availability = None, serviceUptime = None,
                       hostUptime = None, message = None, meta = {} ):
    return self.__query( 'get', 'SLST1Service', locals() )
  def deleteSLST1Service( self, site = None, service = None, timeStamp = None,
                          availability = None, serviceUptime = None,
                          hostUptime = None, message = None, meta = {} ):
    return self.__query( 'delete', 'SLST1Service', locals() )
  def addOrModifySLST1Service( self, site, service, timeStamp, availability,
                               serviceUptime, hostUptime, message ):
    return self.__addOrModifyElement( 'SLST1Service', locals() )
  
  
  def insertSLSLogSE( self, name, timeStamp, validityDuration, availability,
                     dataPartitionUsed, dataPartitionTotal, meta = {}):
    return self.__query( 'insert', 'SLSLogSE', locals() )
  def updateSLSLogSE( self, name, timeStamp, validityDuration, availability,
                      dataPartitionUsed, dataPartitionTotal, meta = {} ):
    return self.__query( 'update', 'SLSLogSE', locals() )
  def getSLSLogSE( self, name = None, timeStamp = None, validityDuration = None,
                   availability = None, dataPartitionUsed = None,
                   dataPartitionTotal = None, meta = {} ):
    return self.__query( 'get', 'SLSLogSE', locals() )
  def deleteSLSLogSE( self, name = None, timeStamp = None,
                      validityDuration = None, availability = None,
                      dataPartitionUsed = None, dataPartitionTotal = None,
                      meta = {} ):
    return self.__query( 'delete', 'SLSLogSE', locals() )
  def addOrModifySLSLogSE( self, name, timeStamp, validityDuration, availability,
                           dataPartitionUsed, dataPartitionTotal ):
    return self.__addOrModifyElement( 'SLSLogSE', locals() )
  
  
  def insertSLSStorage( self, site, token, timeStamp, availability,
                        refreshPeriod, validityDuration, totalSpace,
                        guaranteedSpace, freeSpace, meta = {} ):
    return self.__query( 'insert', 'SLSStorage', locals() )
  def updateSLSStorage( self, site, token, timeStamp, availability,
                        refreshPeriod, validityDuration, totalSpace,
                        guaranteedSpace, freeSpace, meta = {} ):
    return self.__query( 'update', 'SLSStorage', locals() )
  def getSLSStorage( self, site = None, token = None, timeStamp = None,
                     availability = None, refreshPeriod = None,
                     validityDuration = None, totalSpace = None,
                     guaranteedSpace = None, freeSpace = None, meta = {} ):
    return self.__query( 'get', 'SLSStorage', locals() )
  def deleteSLSStorage( self, site = None, token = None, timeStamp = None,
                        availability = None, refreshPeriod = None,
                        validityDuration = None, totalSpace = None,
                        guaranteedSpace = None, freeSpace = None, meta = {} ):
    return self.__query( 'delete', 'SLSStorage', locals() )
  def addOrModifySLSStorage( self, site, token, timeStamp, availability,
                             refreshPeriod, validityDuration, totalSpace,
                             guaranteedSpace, freeSpace ):
    return self.__addOrModifyElement( 'SLSStorage', locals() )
  

  def insertSLSCondDB( self, site, timeStamp, availability, accessTime,
                       meta = {} ):
    return self.__query( 'insert', 'SLSCondDB', locals() )
  def updateSLSCondDB( self, site, timeStamp, availability, accessTime,
                       meta = {} ):
    return self.__query( 'update', 'SLSCondDB', locals() )
  def getSLSCondDB( self, site = None, timeStamp = None, availability = None,
                    accessTime = None, meta = {} ):
    return self.__query( 'get', 'SLSCondDB', locals() )
  def deleteSLSCondDB( self, site = None, timeStamp = None, availability = None,
                       accessTime = None, meta = {} ):
    return self.__query( 'delete', 'SLSCondDB', locals() )
  def addOrModifySLSCondDB( self, site, timeStamp, availability, accessTime ):
    return self.__addOrModifyElement( 'SLSCondDB', locals() )

  '''
  ##############################################################################
  # addOrModify PRIVATE FUNCTIONS
  ##############################################################################
  '''

  def __addOrModifyElement( self, element, kwargs ):

    del kwargs[ 'self' ]

    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
    sqlQuery = self._getElement( element, **kwargs )

    if "Value" not in sqlQuery.keys():
      print (element, kwargs)

    if sqlQuery[ 'Value' ]:
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )

      return self._updateElement( element, **kwargs )
    else:
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )
      if kwargs.has_key( 'dateEffective' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )

      return self._insertElement( element, **kwargs )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF