''' LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient

   ResourceManagementClient.__bases__:
     DIRAC.ResourceStatusSystem.Client.ResourceManagementClient.ResourceManagementClient
  
'''

from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
     ResourceManagementClient as DIRACResourceManagementClient

__RCSID__ = '$Id$'

class ResourceManagementClient( DIRACResourceManagementClient ):
  """
  Extension for the DIRAC version of the ResourceManagementClient.
  
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

  ##############################################################################
  # MONITORING TEST METHODS

#  def insertMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour, 
#                            metricStatus, summaryData, timestamp, lastCheckTime, 
#                            meta = None ):
#    '''
#    Inserts on MonitoringTest a new row with the arguments given.
#    
#    :Parameters:
#      **metricName** - `string`
#        name of the metric 
#      **serviceURI** - `string`
#        URI of the service
#      **siteName** - `string`
#        name of the site
#      **serviceFlavour** - `string`
#        type of service
#      **metricStatus** - `string`
#        metric's status
#      **summaryData** - `string`
#        result of the monitoring test
#      **timestamp** - `datetime`
#        timestamp of the test
#      **lastCheckTime** - `datetime`
#        last time it was cheched      
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613    
#    return self._query( 'insert', 'MonitoringTest', locals() )
#  def updateMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour, 
#                            metricStatus, summaryData, timestamp, lastCheckTime, 
#                            meta = None ):
#    '''
#    Updates on MonitoringTest a new row with the arguments given.
#    
#    :Parameters:
#      **metricName** - `string`
#        name of the metric 
#      **serviceURI** - `string`
#        URI of the service
#      **siteName** - `string`
#        name of the site
#      **serviceFlavour** - `string`
#        type of service
#      **metricStatus** - `string`
#        metric's status
#      **summaryData** - `string`
#        result of the monitoring test
#      **timestamp** - `datetime`
#        timestamp of the test
#      **lastCheckTime** - `datetime`
#        last time it was cheched      
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613       
#    return self._query( 'update', 'MonitoringTest', locals() )
  def selectMonitoringTest( self, metricName = None, serviceURI = None, 
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
    return self._query( 'select', 'MonitoringTest', locals() )
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
    return self._query( 'delete', 'MonitoringTest', locals() )

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
    meta = { 'onlyUniqueKeys' : True } 
    return self._query( 'addOrModify', 'MonitoringTest', locals() )

  ##############################################################################
  # JOB ACCOUNTING CACHE METHODS

  def selectJobAccountingCache( self, name = None, checking = None, completed = None,
                                done = None, failed = None, killed = None, 
                                matched = None, running = None, stalled = None,
                                lastCheckTime = None, meta = None ):
    '''
    Selects from JobAccountingCach all rows that match the parameters given.
    
    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element 
      **checking** - [, `float`, `list` ]
        number of checking jobs
      **completed** - [, `float`, `list` ]
        number of completed jobs
      **done** - [, `float`, `list` ]
        number of done jobs
      **failed** - [, `float`, `list` ]
        number of failed jobs    
      **killed** - [, `float`, `list` ]
        number of killed jobs
      **matched** - [, `float`, `list` ]
        number of matched jobs
      **running** - [, `float`, `list` ]
        number of running jobs
      **stalled** - [, `float`, `list` ]
        number of stalled jobs     
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''                                 

    return self._query( 'select', 'JobAccountingCache', locals() )
  def deleteJobAccountingCache( self, name = None, checking = None, completed = None,
                                done = None, failed = None, killed = None, 
                                matched = None, running = None, stalled = None,
                                lastCheckTime = None, meta = None ):
    '''
    Deletes from JobAccountingCach all rows that match the parameters given.
    
    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element 
      **checking** - [, `float`, `list` ]
        number of checking jobs
      **completed** - [, `float`, `list` ]
        number of completed jobs
      **done** - [, `float`, `list` ]
        number of done jobs
      **failed** - [, `float`, `list` ]
        number of failed jobs    
      **killed** - [, `float`, `list` ]
        number of killed jobs
      **matched** - [, `float`, `list` ]
        number of matched jobs
      **running** - [, `float`, `list` ]
        number of running jobs
      **stalled** - [, `float`, `list` ]
        number of stalled jobs     
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''                                                
    return self._query( 'delete', 'JobAccountingCache', locals() )
  def addOrModifyJobAccountingCache( self, name = None, checking = None, completed = None,
                                     done = None, failed = None, killed = None, 
                                     matched = None, running = None, stalled = None,
                                     lastCheckTime = None, meta = None ):
    '''
    Using `name` to query the database, decides whether to insert or update the t
    table.
    
    :Parameters:
      **name** - `string`
        name of the element 
      **checking** - `float`
        number of checking jobs
      **completed** - `float`
        number of completed jobs
      **done** - `float`
        number of done jobs
      **failed** - `float`
        number of failed jobs    
      **killed** - `float`
        number of killed jobs
      **matched** - `float`
        number of matched jobs
      **running** - `float`
        number of running jobs
      **stalled** - `float`
        number of stalled jobs     
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'JobAccountingCache', locals() )     

  ##############################################################################
  # PILOT ACCOUNTING CACHE METHODS

  def selectPilotAccountingCache( self, name = None, aborted = None, deleted = None, 
                                  done = None, failed = None, lastCheckTime = None, 
                                  meta = None ):
    '''
    Selects from PilotAccountingCache all rows that match the parameters given.
    
    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element 
      **aborted** - [, `float`, `list` ]
        number of aborted pilots
      **deleted** - [, `float`, `list` ]
        number of deleted pilots
      **done** - [, `float`, `list` ]
        number of done pilots  
      **failed** - [, `float`, `list` ]
        number of failed pilots    
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''                                 

    return self._query( 'select', 'PilotAccountingCache', locals() )
  def deletePilotAccountingCache( self, name = None, aborted = None, deleted = None, 
                                  done = None, failed = None, lastCheckTime = None, 
                                  meta = None ):
    '''
    Deletes from PilotAccountingCache all rows that match the parameters given.
    
    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element 
      **aborted** - [, `float`, `list` ]
        number of aborted pilots
      **deleted** - [, `float`, `list` ]
        number of deleted pilots
      **done** - [, `float`, `list` ]
        number of done pilots  
      **failed** - [, `float`, `list` ]
        number of failed pilots    
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''                                                
    return self._query( 'delete', 'PilotAccountingCache', locals() )
  def addOrModifyPilotAccountingCache( self, name = None, aborted = None, deleted = None, 
                                       done = None, failed = None, lastCheckTime = None, 
                                       meta = None ):
    '''
    Using `name` to query the database, decides whether to insert or update the t
    table.
    
    :Parameters:
      **name** - `string`
        name of the element 
      **aborted** - `float`
        number of aborted pilots
      **deleted** - `float`
        number of deleted pilots
      **done** - `float`
        number of done pilots  
      **failed** - `float`
        number of failed pilots    
      **lastCheckTime** - `datetime`
        last time it was cheched    
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'PilotAccountingCache', locals() )     

  #..................
  # ENVIRONMENT CACHE methods

  def selectEnvironmentCache( self, hashKey = None, environment = None, 
                              siteName = None, arguments = None,
                              dateEffective = None, lastCheckTime = None, 
                              meta = None ):
    '''
    Gets from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashKey** - `[, string, list]`
        hash of the environment 
      **environment** - `[, string, list]`
        string with the environment dump
      **siteName** - `[, string, list]`
        name of the site
      **arguments** - `[, string, list]`
        SetupProject arguments
      **dateEffective** - `[, datetime, list]`
        creation time of the hash
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613       
    return self._query( 'select', 'EnvironmentCache', locals() )
  def deleteEnvironmentCache( self, hashKey = None, environment = None, 
                              siteName = None, arguments = None,
                              dateEffective = None, lastCheckTime = None, 
                              meta = None ):
    '''
    Deletes from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashKey** - `[, string, list]`
        hash of the environment 
      **environment** - `[, string, list]`
        string with the environment dump
      **siteName** - `[, string, list]`
        name of the site
      **arguments** - `[, string, list]`
        SetupProject arguments
      **dateEffective** - `[, datetime, list]`
        creation time of the hash
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched      
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613       
    return self._query( 'delete', 'EnvironmentCache', locals() )
  def addOrModifyEnvironmentCache( self, hashKey, environment, siteName, 
                                   arguments, dateEffective, lastCheckTime ):
    '''
    Using `hashKey` to query the database, decides whether to insert or update 
    the table.
    
    :Parameters:
      **hashKey** - `string`
        hash of the environment 
      **environment** - `string`
        string with the environment dump
      **siteName** - `string`
        name of the site
      **arguments** - `string`
        SetupProject arguments
      **dateEffective** - `datetime`
        creation time of the hash
      **lastCheckTime** - `datetime`
        last time it was cheched      

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613      
    meta = { 'onlyUniqueKeys' : True } 
    return self._query( 'addOrModify', 'EnvironmentCache', locals() )

  ##############################################################################
  # HAMMERCLOUD TEST METHODS

#  def insertHammerCloudTest( self, testID, siteName, resourceName, testStatus,
#                             submissionTime, startTime, endTime, counterTime,
#                             agentStatus, formerAgentStatus, counter, meta = None ):
#    '''
#    Inserts on HammerCloud a new row with the arguments given.
#    
#    :Parameters:
#      **testID** - `integer`
#        ID given to the test by HammerCloud 
#      **siteName** - `string`
#        name of the site
#      **resourceName** - `string`
#        name of the resource
#      **testStatus** - `string`
#        status of the test
#      **submissionTime** - `datetime`
#        test submission time
#      **startTime** - `datetime`
#        test start time
#      **endTime** - `datetime`
#        test end time
#      **counterTime** - `datetime`
#        timestamp associated to the counter
#      **agentStatus** - `string`
#        status associated to the agent      
#      **formerAgentStatus** - `string`
#        previous status associated to the agent      
#      **counter** - `integer`
#        counter assigned by the agent                   
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613      
#    return self._query( 'insert', 'HammerCloudTest', locals() )
#  def updateHammerCloudTest( self, testID, siteName, resourceName, testStatus,
#                             submissionTime, startTime, endTime, counterTime,
#                             agentStatus, formerAgentStatus, counter, meta = None ):
#    '''
#    Updates on HammerCloud a new row with the arguments given.
#    
#    :Parameters:
#      **testID** - `integer`
#        ID given to the test by HammerCloud 
#      **siteName** - `string`
#        name of the site
#      **resourceName** - `string`
#        name of the resource
#      **testStatus** - `string`
#        status of the test
#      **submissionTime** - `datetime`
#        test submission time
#      **startTime** - `datetime`
#        test start time
#      **endTime** - `datetime`
#        test end time
#      **counterTime** - `datetime`
#        timestamp associated to the counter
#      **agentStatus** - `string`
#        status associated to the agent      
#      **formerAgentStatus** - `string`
#        previous status associated to the agent      
#      **counter** - `integer`
#        counter assigned by the agent                   
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613    
#    return self._query( 'update', 'HammerCloudTest', locals() )
#  def selectHammerCloudTest( self, testID = None, siteName = None, resourceName = None, 
#                             testStatus = None, submissionTime = None, startTime = None,
#                             endTime = None, counterTime = None, agentStatus = None, 
#                             formerAgentStatus = None, counter = None, meta = None ):
#    '''
#    Gets from HammerCloud all rows that match the parameters given.
#    
#    :Parameters:
#      **testID** - `[, integer, list]`
#        ID given to the test by HammerCloud 
#      **siteName** - `[, string, list]`
#        name of the site
#      **resourceName** - `[, string, list]`
#        name of the resource
#      **testStatus** - `[, string, list]`
#        status of the test
#      **submissionTime** - `[, datetime, list]`
#        test submission time
#      **startTime** - `[, datetime, list]`
#        test start time
#      **endTime** - `[, datetime, list]`
#        test end time
#      **counterTime** - `[, datetime, list]`
#        timestamp associated to the counter
#      **agentStatus** - `[, string, list]`
#        status associated to the agent      
#      **formerAgentStatus** - `[, string, list]`
#        previous status associated to the agent      
#      **counter** - `[, integer, list]`
#        counter assigned by the agent                   
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613        
#    return self._query( 'select', 'HammerCloudTest', locals() )
#  def deleteHammerCloudTest( self, testID = None, siteName = None, resourceName = None, 
#                             testStatus = None, submissionTime = None, startTime = None,
#                             endTime = None, counterTime = None, agentStatus = None, 
#                             formerAgentStatus = None, counter = None, meta = None ):
#    '''
#    Deletes from HammerCloud all rows that match the parameters given.
#    
#    :Parameters:
#      **testID** - `[, integer, list]`
#        ID given to the test by HammerCloud 
#      **siteName** - `[, string, list]`
#        name of the site
#      **resourceName** - `[, string, list]`
#        name of the resource
#      **testStatus** - `[, string, list]`
#        status of the test
#      **submissionTime** - `[, datetime, list]`
#        test submission time
#      **startTime** - `[, datetime, list]`
#        test start time
#      **endTime** - `[, datetime, list]`
#        test end time
#      **counterTime** - `[, datetime, list]`
#        timestamp associated to the counter
#      **agentStatus** - `[, string, list]`
#        status associated to the agent      
#      **formerAgentStatus** - `[, string, list]`
#        previous status associated to the agent      
#      **counter** - `[, integer, list]`
#        counter assigned by the agent                   
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613     
#    return self._query( 'delete', 'HammerCloudTest', locals() )
#
#  def addOrModifyHammerCloudTest( self, testID, siteName, resourceName,
#                                  testStatus, submissionTime, startTime,
#                                  endTime, counterTime, agentStatus,
#                                  formerAgentStatus, counter ):
#    '''
#    Using `submissionTime` to query the database, decides whether 
#    to insert or update the table.
#    
#    :Parameters:
#      **testID** - `integer`
#        ID given to the test by HammerCloud 
#      **siteName** - `string`
#        name of the site
#      **resourceName** - `string`
#        name of the resource
#      **testStatus** - `string`
#        status of the test
#      **submissionTime** - `datetime`
#        test submission time
#      **startTime** - `datetime`
#        test start time
#      **endTime** - `datetime`
#        test end time
#      **counterTime** - `datetime`
#        timestamp associated to the counter
#      **agentStatus** - `string`
#        status associated to the agent      
#      **formerAgentStatus** - `string`
#        previous status associated to the agent      
#      **counter** - `integer`
#        counter assigned by the agent                   
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613   
#    meta = { 'onlyUniqueKeys' : True }   
#    return self._query( 'addOrModify', 'HammerCloudTest', locals() )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF