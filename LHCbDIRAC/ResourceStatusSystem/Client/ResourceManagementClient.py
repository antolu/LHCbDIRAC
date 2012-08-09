# $HeadURL $
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
    
    # If meta is None, we set it to {}
    meta   = ( True and kwargs.pop( 'meta' ) ) or {}
    params = kwargs
    del params[ 'self' ]     
        
    meta[ 'table' ] = tableName
    
    gLogger.debug( 'Calling %s, with \n params %s \n meta %s' % ( queryType, params, meta ) )  
    return gateFunction( params, meta )

################################################################################
# MONITORING TEST METHODS

  def insertMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour, 
                            metricStatus, summaryData, timestamp, lastCheckTime, 
                            meta = None ):
    '''
    Inserts on MonitoringTest a new row with the arguments given.
    
    :Parameters:
      **metricName** - `string`
        name of the metric 
      **serviceURI** - `string`
        URI of the service
      **siteName** - `string`
        name of the site
      **serviceFlavour** - `string`
        type of service
      **metricStatus** - `string`
        metric's status
      **summaryData** - `string`
        result of the monitoring test
      **timestamp** - `datetime`
        timestamp of the test
      **lastCheckTime** - `datetime`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613    
    return self.__query( 'insert', 'MonitoringTest', locals() )
  def updateMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour, 
                            metricStatus, summaryData, timestamp, lastCheckTime, 
                            meta = None ):
    '''
    Updates on MonitoringTest a new row with the arguments given.
    
    :Parameters:
      **metricName** - `string`
        name of the metric 
      **serviceURI** - `string`
        URI of the service
      **siteName** - `string`
        name of the site
      **serviceFlavour** - `string`
        type of service
      **metricStatus** - `string`
        metric's status
      **summaryData** - `string`
        result of the monitoring test
      **timestamp** - `datetime`
        timestamp of the test
      **lastCheckTime** - `datetime`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613       
    return self.__query( 'update', 'MonitoringTest', locals() )
  def getMonitoringTest( self, metricName = None, serviceURI = None, 
                         siteName = None, serviceFlavour = None,
                         metricStatus = None, summaryData = None,
                         timestamp = None, lastCheckTime = None, meta = None ):
    '''
    Gets from MonitoringTest all rows that match the parameters given.
    
    :Parameters:
      **metricName** - `[, string, list]`
        name of the metric 
      **serviceURI** - `[, string, list]`
        URI of the service
      **siteName** - `[, string, list]`
        name of the site
      **serviceFlavour** - `[, string, list]`
        type of service
      **metricStatus** - `[, string, list]`
        metric's status
      **summaryData** - `[, string, list]`
        result of the monitoring test
      **timestamp** - `[, datetime, list]`
        timestamp of the test
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613       
    return self.__query( 'get', 'MonitoringTest', locals() )
  def deleteMonitoringTest( self, metricName = None, serviceURI = None,
                            siteName = None, serviceFlavour = None, 
                            metricStatus = None, summaryData = None,
                            timestamp = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from MonitoringTest all rows that match the parameters given.
    
    :Parameters:
      **metricName** - `[, string, list]`
        name of the metric 
      **serviceURI** - `[, string, list]`
        URI of the service
      **siteName** - `[, string, list]`
        name of the site
      **serviceFlavour** - `[, string, list]`
        type of service
      **metricStatus** - `[, string, list]`
        metric's status
      **summaryData** - `[, string, list]`
        result of the monitoring test
      **timestamp** - `[, datetime, list]`
        timestamp of the test
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613       
    return self.__query( 'delete', 'MonitoringTest', locals() )

################################################################################
# HAMMERCLOUD TEST METHODS

  def insertHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter, meta = None ):
    '''
    Inserts on HammerCloud a new row with the arguments given.
    
    :Parameters:
      **testID** - `integer`
        ID given to the test by HammerCloud 
      **siteName** - `string`
        name of the site
      **resourceName** - `string`
        name of the resource
      **testStatus** - `string`
        status of the test
      **submissionTime** - `datetime`
        test submission time
      **startTime** - `datetime`
        test start time
      **endTime** - `datetime`
        test end time
      **counterTime** - `datetime`
        timestamp associated to the counter
      **agentStatus** - `string`
        status associated to the agent      
      **formerAgentStatus** - `string`
        previous status associated to the agent      
      **counter** - `integer`
        counter assigned by the agent                   
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613      
    return self.__query( 'insert', 'HammerCloudTest', locals() )
  def updateHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter, meta = None ):
    '''
    Updates on HammerCloud a new row with the arguments given.
    
    :Parameters:
      **testID** - `integer`
        ID given to the test by HammerCloud 
      **siteName** - `string`
        name of the site
      **resourceName** - `string`
        name of the resource
      **testStatus** - `string`
        status of the test
      **submissionTime** - `datetime`
        test submission time
      **startTime** - `datetime`
        test start time
      **endTime** - `datetime`
        test end time
      **counterTime** - `datetime`
        timestamp associated to the counter
      **agentStatus** - `string`
        status associated to the agent      
      **formerAgentStatus** - `string`
        previous status associated to the agent      
      **counter** - `integer`
        counter assigned by the agent                   
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613    
    return self.__query( 'update', 'HammerCloudTest', locals() )
  def getHammerCloudTest( self, testID = None, siteName = None, resourceName = None, 
                          testStatus = None, submissionTime = None, startTime = None,
                          endTime = None, counterTime = None, agentStatus = None, 
                          formerAgentStatus = None, counter = None, meta = None ):
    '''
    Gets from HammerCloud all rows that match the parameters given.
    
    :Parameters:
      **testID** - `[, integer, list]`
        ID given to the test by HammerCloud 
      **siteName** - `[, string, list]`
        name of the site
      **resourceName** - `[, string, list]`
        name of the resource
      **testStatus** - `[, string, list]`
        status of the test
      **submissionTime** - `[, datetime, list]`
        test submission time
      **startTime** - `[, datetime, list]`
        test start time
      **endTime** - `[, datetime, list]`
        test end time
      **counterTime** - `[, datetime, list]`
        timestamp associated to the counter
      **agentStatus** - `[, string, list]`
        status associated to the agent      
      **formerAgentStatus** - `[, string, list]`
        previous status associated to the agent      
      **counter** - `[, integer, list]`
        counter assigned by the agent                   
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613        
    return self.__query( 'get', 'HammerCloudTest', locals() )
  def deleteHammerCloudTest( self, testID = None, siteName = None, resourceName = None, 
                             testStatus = None, submissionTime = None, startTime = None,
                             endTime = None, counterTime = None, agentStatus = None, 
                             formerAgentStatus = None, counter = None, meta = None ):
    '''
    Deletes from HammerCloud all rows that match the parameters given.
    
    :Parameters:
      **testID** - `[, integer, list]`
        ID given to the test by HammerCloud 
      **siteName** - `[, string, list]`
        name of the site
      **resourceName** - `[, string, list]`
        name of the resource
      **testStatus** - `[, string, list]`
        status of the test
      **submissionTime** - `[, datetime, list]`
        test submission time
      **startTime** - `[, datetime, list]`
        test start time
      **endTime** - `[, datetime, list]`
        test end time
      **counterTime** - `[, datetime, list]`
        timestamp associated to the counter
      **agentStatus** - `[, string, list]`
        status associated to the agent      
      **formerAgentStatus** - `[, string, list]`
        previous status associated to the agent      
      **counter** - `[, integer, list]`
        counter assigned by the agent                   
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613     
    return self.__query( 'delete', 'HammerCloudTest', locals() )

################################################################################
# SLS TEST METHODS
  
  def insertSLSTest( self, testName, target, availability, result, description, 
                     dateEffective, meta = None ):
    '''
    Inserts on SLSTest a new row with the arguments given.
    
    :Parameters:
      **testName** - `string`
        test name ( type of test ) 
      **target** - `string`
        name, URI, id.. of the target
      **availability** - `integer`
        computed availability
      **result** - `integer`
        result of the test, used to compute availability
      **description** - `string`
        verbose result
      **dateEffective** - `datetime`
        test timestamp
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613      
    return self.__query( 'insert', 'SLSTest', locals() )
  def updateSLSTest( self, testName, target, availability, result, description, 
                     dateEffective, meta = None ):
    '''
    Updates on SLSTest a new row with the arguments given.
    
    :Parameters:
      **testName** - `string`
        test name ( type of test ) 
      **target** - `string`
        name, URI, id.. of the target
      **availability** - `integer`
        computed availability
      **result** - `integer`
        result of the test, used to compute availability
      **description** - `string`
        verbose result
      **dateEffective** - `datetime`
        test timestamp
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613     
    return self.__query( 'update', 'SLSTest', locals() )
  def getSLSTest( self, testName = None, target = None, availability = None, 
                  result = None, description = None, dateEffective = None, 
                  meta = None ):
    '''
    Gets from SLSTest all rows that match the parameters given.
    
    :Parameters:
      **testName** - `[, string, list]`
        test name ( type of test ) 
      **target** - `[, string, list]`
        name, URI, id.. of the target
      **availability** - `[, integer, list]`
        computed availability
      **result** - `[, integer, list]`
        result of the test, used to compute availability
      **description** - `[, string, list]`
        verbose result
      **dateEffective** - `[, datetime, list]`
        test timestamp
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613     
    return self.__query( 'get', 'SLSTest', locals() )
  def deleteSLSTest( self, testName = None, target = None, availability = None, 
                     result = None, description = None, dateEffective = None, 
                     meta = None ):
    '''
    Deletes from SLSTest all rows that match the parameters given.
    
    :Parameters:
      **testName** - `[, string, list]`
        test name ( type of test ) 
      **target** - `[, string, list]`
        name, URI, id.. of the target
      **availability** - `[, integer, list]`
        computed availability
      **result** - `[, integer, list]`
        result of the test, used to compute availability
      **description** - `[, string, list]`
        verbose result
      **dateEffective** - `[, datetime, list]`
        test timestamp
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613        
    return self.__query( 'delete', 'SLSTest', locals() )

################################################################################
# EXTENDED BASE API METHODS

  def addOrModifyMonitoringTest( self, metricName, serviceURI, siteName,
                                 serviceFlavour, metricStatus, summaryData,
                                 timestamp, lastCheckTime ):
    '''
    Using `metricName` and `serviceURI` to query the database, decides whether 
    to insert or update the table.
    
    :Parameters:
      **metricName** - `string`
        name of the metric 
      **serviceURI** - `string`
        URI of the service
      **siteName** - `string`
        name of the site
      **serviceFlavour** - `string`
        type of service
      **metricStatus** - `string`
        metric's status
      **summaryData** - `string`
        result of the monitoring test
      **timestamp** - `datetime`
        timestamp of the test
      **lastCheckTime** - `datetime`
        last time it was cheched    

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613        
    return self.__addOrModifyElement( 'MonitoringTest', locals() )

  def addOrModifyHammerCloudTest( self, testID, siteName, resourceName,
                                  testStatus, submissionTime, startTime,
                                  endTime, counterTime, agentStatus,
                                  formerAgentStatus, counter ):
    '''
    Using `submissionTime` to query the database, decides whether 
    to insert or update the table.
    
    :Parameters:
      **testID** - `integer`
        ID given to the test by HammerCloud 
      **siteName** - `string`
        name of the site
      **resourceName** - `string`
        name of the resource
      **testStatus** - `string`
        status of the test
      **submissionTime** - `datetime`
        test submission time
      **startTime** - `datetime`
        test start time
      **endTime** - `datetime`
        test end time
      **counterTime** - `datetime`
        timestamp associated to the counter
      **agentStatus** - `string`
        status associated to the agent      
      **formerAgentStatus** - `string`
        previous status associated to the agent      
      **counter** - `integer`
        counter assigned by the agent                   

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613      
    return self.__addOrModifyElement( 'HammerCloudTest', locals() )

  def addOrModifySLSTest( self, testName, target, availability, result,
                          description, dateEffective ):
    '''
    Using `testName` and `target` to query the database, decides whether 
    to insert or update the table.
    
    :Parameters:
      **testName** - `string`
        test name ( type of test ) 
      **target** - `string`
        name, URI, id.. of the target
      **availability** - `integer`
        computed availability
      **result** - `integer`
        result of the test, used to compute availability
      **description** - `string`
        verbose result
      **dateEffective** - `datetime`
        test timestamp                

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613          
    return self.__addOrModifyElement( 'SLSTest', locals() )

################################################################################
# To be deleted...

#  def insertSLSService( self, system, service, timeStamp, availability,
#                        serviceUptime, hostUptime, instantLoad, message, meta = None ):
#    return self.__query( 'insert', 'SLSService', locals() )
#  def updateSLSService( self, system, service, timeStamp, availability,
#                        serviceUptime, hostUptime, instantLoad, message, meta = None ):
#    return self.__query( 'update', 'SLSService', locals() )
#  def getSLSService( self, system = None, service = None, timeStamp = None,
#                     availability = None, serviceUptime = None,
#                     hostUptime = None, instantLoad = None, message = None, meta = None ):
#    return self.__query( 'get', 'SLSService', locals() )
#  def deleteSLSService( self, system = None, service = None, timeStamp = None,
#                        availability = None, serviceUptime = None,
#                        hostUptime = None, instantLoad = None, message = None, meta = None ):
#    return self.__query( 'delete', 'SLSService', locals() )
#  def addOrModifySLSService( self, system, service, timeStamp, availability,
#                             serviceUptime, hostUptime, instantLoad, message):
#    return self.__addOrModifyElement( 'SLSService', locals() )
  
  
#  def insertSLST1Service( self, site, service, timeStamp, availability,
#                          serviceUptime, hostUptime, message, meta = None ):
#    return self.__query( 'insert', 'SLST1Service', locals() )
#  def updateSLST1Service( self, site, service, timeStamp, availability,
#                          serviceUptime, hostUptime, message, meta = None ):
#    return self.__query( 'update', 'SLST1Service', locals() )
#  def getSLST1Service( self, site = None, service = None, timeStamp = None,
#                       availability = None, serviceUptime = None,
#                       hostUptime = None, message = None, meta = None ):
#    return self.__query( 'get', 'SLST1Service', locals() )
#  def deleteSLST1Service( self, site = None, service = None, timeStamp = None,
#                          availability = None, serviceUptime = None,
#                          hostUptime = None, message = None, meta = None ):
#    return self.__query( 'delete', 'SLST1Service', locals() )
#  def addOrModifySLST1Service( self, site, service, timeStamp, availability,
#                               serviceUptime, hostUptime, message ):
#    return self.__addOrModifyElement( 'SLST1Service', locals() )
  
  
#  def insertSLSLogSE( self, name, timeStamp, validityDuration, availability,
#                     dataPartitionUsed, dataPartitionTotal, meta = None ):
#    return self.__query( 'insert', 'SLSLogSE', locals() )
#  def updateSLSLogSE( self, name, timeStamp, validityDuration, availability,
#                      dataPartitionUsed, dataPartitionTotal, meta = None ):
#    return self.__query( 'update', 'SLSLogSE', locals() )
#  def getSLSLogSE( self, name = None, timeStamp = None, validityDuration = None,
#                   availability = None, dataPartitionUsed = None,
#                   dataPartitionTotal = None, meta = None ):
#    return self.__query( 'get', 'SLSLogSE', locals() )
#  def deleteSLSLogSE( self, name = None, timeStamp = None,
#                      validityDuration = None, availability = None,
#                      dataPartitionUsed = None, dataPartitionTotal = None,
#                      meta = None ):
#    return self.__query( 'delete', 'SLSLogSE', locals() )
#  def addOrModifySLSLogSE( self, name, timeStamp, validityDuration, availability,
#                           dataPartitionUsed, dataPartitionTotal ):
#    return self.__addOrModifyElement( 'SLSLogSE', locals() )
  
  
#  def insertSLSStorage( self, site, token, timeStamp, availability,
#                        refreshPeriod, validityDuration, totalSpace,
#                        guaranteedSpace, freeSpace, meta = None ):
#    return self.__query( 'insert', 'SLSStorage', locals() )
#  def updateSLSStorage( self, site, token, timeStamp, availability,
#                        refreshPeriod, validityDuration, totalSpace,
#                        guaranteedSpace, freeSpace, meta = None ):
#    return self.__query( 'update', 'SLSStorage', locals() )
#  def getSLSStorage( self, site = None, token = None, #timeStamp = None,
#                     availability = None, refreshPeriod = None,
#                     validityDuration = None, totalSpace = None,
#                     guaranteedSpace = None, freeSpace = None, meta = None ):
#    return self.__query( 'get', 'SLSStorage', locals() )
#  def deleteSLSStorage( self, site = None, token = None, timeStamp = None,
#                        availability = None, refreshPeriod = None,
#                        validityDuration = None, totalSpace = None,
#                        guaranteedSpace = None, freeSpace = None, meta = None ):
#    return self.__query( 'delete', 'SLSStorage', locals() )
#  def addOrModifySLSStorage( self, site, token, timeStamp, availability,
#                             refreshPeriod, validityDuration, totalSpace,
#                             guaranteedSpace, freeSpace ):
#    return self.__addOrModifyElement( 'SLSStorage', locals() )
  

#  def insertSLSCondDB( self, site, timeStamp, availability, accessTime,
#                       meta = None ):
#    return self.__query( 'insert', 'SLSCondDB', locals() )
#  def updateSLSCondDB( self, site, timeStamp, availability, accessTime,
#                       meta = None ):
#    return self.__query( 'update', 'SLSCondDB', locals() )
#  def getSLSCondDB( self, site = None, timeStamp = None, availability = None,
#                    accessTime = None, meta = None ):
#    return self.__query( 'get', 'SLSCondDB', locals() )
#  def deleteSLSCondDB( self, site = None, timeStamp = None, availability = None,
#                       accessTime = None, meta = None ):
#    return self.__query( 'delete', 'SLSCondDB', locals() )
#  def addOrModifySLSCondDB( self, site, timeStamp, availability, accessTime ):
#    return self.__addOrModifyElement( 'SLSCondDB', locals() )

################################################################################
# addOrModify PRIVATE FUNCTIONS

  def __addOrModifyElement( self, element, kwargs ):
    '''
      Method that executes update if the item is not new, otherwise inserts it
      on the element table.
    '''
    
    del kwargs[ 'self' ]

    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }

    sqlQuery = self._getElement( element, kwargs )
    if not sqlQuery[ 'OK' ]:
      return sqlQuery

    if sqlQuery[ 'Value' ]:
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )

      return self._updateElement( element, kwargs )
    else:
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )
      if kwargs.has_key( 'dateEffective' ):
        kwargs[ 'dateEffective' ] = datetime.utcnow().replace( microsecond = 0 )

      return self._insertElement( element, kwargs )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF