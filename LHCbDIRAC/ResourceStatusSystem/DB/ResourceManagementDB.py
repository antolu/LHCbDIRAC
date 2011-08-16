'''
LHCbDIRAC/ResourceStatusSystem/DB/ResourceManagementDB.py
'''

__RCSID__ = "$Id$"

# First, pythonic stuff
from datetime import datetime

# Second, DIRAC stuff
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import \
     ResourceManagementDB as DIRACResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import RSSManagementDBException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.Utilities import Utils

# Third, LHCbDIRAC stuff
# ...

class ResourceManagementDB( DIRACResourceManagementDB ):
  '''
  Class ResourceManagementDB, extension of the class with the same name
  on DIRAC.

  The extension provides the following methods:

  Note:
    marked with "S" if they have a server method with the same name
    to access them.

  - addHCTest
  - updateHCTest
  - getHCTestList                  S

  - addMonitoringTest              S
  - updateMonitoringTest           S
  - getMonitoringTestList          S
  '''

################################################################################
# HammerCloudTest functions
################################################################################

  def addHCTest( self, siteName, submissionTime, agentStatus = '', resourceName = '' ):
    '''
    Add a new test to the HammerCloudTests table.

    :params:
      :attr:`siteName`: string - site where the test is submitted

      :attr:`submissionTime`: date time - test submission attempt (on time)

      :attr:`agentStatus`: string - agent status

      :attr:`resourceName`: string - CE name if executed the test on a particular CE
    '''

    keys   = []
    values = []

    for k,v in locals().items():
      if k == 'self' or k == 'keys' or k == 'values':
        continue
      keys.append( k[0].upper() + k[1:] )
      # If by some reason, v is not string, this line will crash
      values.append( "'" + v + "'" )

    req  = "INSERT INTO HammerCloudTests ( %s ) " % ', '.join( keys )
    req += "VALUES ( %s )" % ', '.join( values )

    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSException, where( self, self.addHCTest ) + resUpdate['Message']

################################################################################

  def updateHCTest( self, submissionTime, testID = None, testStatus = None,
                   startTime = None, endTime = None, counterTime = None,
                   agentStatus = None, formerAgentStatus = None, counter = None ):
    '''
    Update test parameters on the HammerCloudTests table.

    :params:
      :attr: `submissionTime`: date time - test submission attempt (on time),
              key to find a test on the table

      :attr: `testID` : integer - (optional) assigned (by HC) test ID

      :attr: `testStatus` : string - (optional) assigned (by HC) test status

      :attr: `startTime` : date time - (optional) assigned (by HC) start time

      :attr: `endTime` : date time - (optional) assigned (by HC) end time

      :attr: `counterTime` : date time - (optional) error registered time

      :attr: `agentStatus` : string - (optional) agent status

      :attr: `formerAgentStatus` : string - (optional) former agent status

      :attr: `counter` : integer - number of times test has failed
    '''

    paramsList = []

    for k,v in locals().items():
      if k == 'self' or k == 'paramsList':
        continue
      if v is not None:
        paramsList.append( '%s = "%s"' % ( k[0].upper() + k[1:], v ) )

    req = 'UPDATE HammerCloudTests SET '
    req += ', '.join( paramsList )

    if submissionTime:
      req += ' WHERE SubmissionTime = "%s";' % submissionTime
    elif testID:
      req += ' WHERE TestID = %d;' % testID
    else:
      raise RSSException, where( self, self.updateHCTest ) + 'Submission time and TestID missing'

    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSException, where( self, self.updateHCTest ) + resUpdate['Message']

################################################################################

  def getHCTestList( self, submissionTime = None, testStatus = None, siteName = None,
                    testID = None, resourceName = None, agentStatus = None,
                    formerAgentStatus = None, counter = None, last = False ):
    '''
    Get list of tests, having as query filters the parameters.

    :params:
      :attr: `submissionTime`: date time - test submission attempt (on time),
              key to find a test on the table

      :attr: `testStatus` : string - (optional) assigned (by HC) test status

      :attr: `siteName` : string - (optional) site where the test run

      :attr: `testID` : integer - (optional) assigned (by HC) test ID

      :attr: `startTime` : date time - (optional) assigned (by HC) start time

      :attr: `endTime` : date time - (optional) assigned (by HC) end time

      :attr: `counterTime` : date time - (optional) error registered time

      :attr: `agentStatus` : string - (optional) agent status

      :attr: `formerAgentStatus` : string - (optional) former agent status

      :attr: `counter` : integer - number of times test has failed

      :attr: `last` : bool - return last entry
    '''

    paramsList = []

    for k,v in locals().items():
      if k == 'self' or k == 'paramsList' or k == 'last' or k == 'counter':
        continue
      if v is not None:
        paramsList.append( '%s = "%s"' % ( k[0].upper() + k[1:], v ) )

    if counter is not None:
      paramsList.append( 'Counter > %d' % counter )

    req = 'SELECT TestID, SiteName, ResourceName, TestStatus, AgentStatus, FormerAgentStatus, \
           SubmissionTime, StartTime, EndTime, Counter, CounterTime from HammerCloudTests'

    if paramsList:
      req += ' WHERE ' + ' AND '.join( paramsList )
    req += ' ORDER BY SubmissionTime'

    if last:
      req += ' DESC LIMIT 1'

    resQuery = self.db._query( req )
    if not resQuery['OK']:
      raise RSSException, where( self, self.getHCTestList ) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return [res for res in resQuery['Value']]

################################################################################
# END HammerCloudTest functions
################################################################################

################################################################################
# MonitoringTest functions
################################################################################

  def addMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour,
                         metricStatus, summaryData, timestamp ):
    '''
    Add a new test to the MonitoringTests table.

    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service

      :attr:`siteName`: string - site where the test was run

      :attr:`serviceFlavour`: string - type of service tested

      :attr:`metricStatus`: string - output of the test

      :attr:`summaryData`: string - detailed output

      :attr:`timestamp`: date time - test execution timestamp
    '''

    keys   = []
    values = []

    # MySQL DATETIME field cannot handle microseconds
    timestamp = timestamp.replace( microsecond = 0 ).isoformat(' ')

    for k,v in locals().items():
      if k == 'self' or k == 'keys' or k == 'values':
        continue
      keys.append( k[0].upper() + k[1:] )
      # If by some reason, v is not string, this line will crash
      values.append( "'" + v + "'" )

    now = datetime.now().replace( microsecond = 0 ).isoformat(' ')

    req  = "INSERT INTO MonitoringTests ( %s, LastUpdate ) " % ', '.join( keys )
    req += "VALUES ( %s, '%s' )" % ( ', '.join( values ), now )

    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSException, where( self, self.addMonitoringTest ) + resUpdate['Message'] + req

################################################################################

  def updateMonitoringTest( self, metricName, serviceURI, siteName = None,
                            serviceFlavour = None, metricStatus = None,
                            summaryData = None, timestamp = None ):
    '''
    Update test on the MonitoringTests table.

    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service

      :attr:`siteName`: string - (optional) site where the test was run

      :attr:`serviceFlavour`: string - (optional) type of service tested

      :attr:`metricStatus`: string - (optional) output of the test

      :attr:`summaryData`: string - (optional) detailed output

      :attr:`timestamp`: date time - (optional) test execution timestamp
    '''

    paramsList = []

    # MySQL DATETIME field cannot handle microseconds
    if timestamp is not None:
      timestamp = timestamp.replace( microsecond = 0 ).isoformat( ' ' )

    for k,v in locals().items():
      if k == 'self' or k == 'paramsList' or k == 'metricName' or k == 'serviceURI':
        continue
      if v is not None:
        paramsList.append( '%s = "%s"' % ( k[0].upper() + k[1:], v ) )

    paramsList.append( 'LastUpdate = "%s"' % datetime.now().replace( microsecond = 0 ).isoformat(' ') )

    req = 'UPDATE MonitoringTests SET '
    req += ', '.join( paramsList )

    req += ' WHERE MetricName = "%s" AND' % metricName
    req += ' ServiceURI = "%s";' % serviceURI

    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSException, where( self, self.updateMonitoringTest ) + resUpdate['Message']

################################################################################

  def getMonitoringTestList( self, metricName = None, serviceURI = None,
                             siteName = None, serviceFlavour = None,
                             metricStatus = None, summaryData = None, timestamp = None ):
    '''
    Get list of monitoring tests matching the given parameters

    :params:
      :attr:`metricName`: string - (optional) name of the metric

      :attr:`serviceURI`: string - (optional) full name of the service

      :attr:`siteName`: string - (optional) site where the test was run

      :attr:`serviceFlavour`: string - (optional) type of service tested

      :attr:`metricStatus`: string - (optional) output of the test

      :attr:`summaryData`: string - (optional) detailed output

      :attr:`timestamp`: date time - (optional) test execution timestamp
    '''

    paramsList = []

    # MySQL DATETIME field cannot handle microseconds
    if timestamp is not None:
      timestamp = timestamp.replace( microsecond = 0 ).isoformat( ' ' )

    for k,v in locals().items():
      if k == 'self' or k == 'paramsList':
        continue
      if v is not None:
        paramsList.append( '%s = "%s"' % ( k[0].upper() + k[1:], v ) )

    req = 'SELECT * from MonitoringTests'

    if paramsList:
      req += ' WHERE ' + ' AND '.join( paramsList )
    req += ' ORDER BY Timestamp;'

    resQuery = self.db._query( req )
    if not resQuery['OK']:
      raise RSSException, where( self, self.getMonitoringTestList ) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return [res for res in resQuery['Value']]

################################################################################
# END MonitoringTest functions
################################################################################

  def _doUpdate(self, req):
    res = self.db._update(req)
    if not res['OK']:
      raise RSSManagementDBException, where(self, self.updateSLSServices) + res['Message']
    return res['Value']


  def updateSLSServices(self, system, service, avail, service_uptime, host_uptime, load):

    req = Utils.sql_insert_update("SLSServices", ["System", "Service"], System=system, Service=service,
                                  Availability=avail, TStamp=Utils.SQLParam("NOW()"),
                                  ServiceUptime=service_uptime,
                                  HostUptime=host_uptime,
                                  InstantLoad=load)

    self._doUpdate(req)

  def updateSLST1Services(self, site, service, avail, service_uptime, host_uptime):

    req = Utils.sql_insert_update("SLST1Services", ["Site", "Service"], Site=site, Service=service,
                                  Availability=avail, TStamp=Utils.SQLParam("NOW()"),
                                  ServiceUptime=service_uptime,
                                  HostUptime=host_uptime)

    self._doUpdate(req)


  def updateSLSLogSE(self, id_, validity, avail, used, total):
    req = Utils.sql_insert_update("SLSLogSE", ["ID"], ID=id_, TStamp=Utils.SQLValues.now,
                                  ValidityDuration=validity, Availability=avail,
                                  DataPartitionUsed=used, DataPartitionTotal=total)

    self._doUpdate(req)


  def getSLSServices(self, url=""):
    req = "SELECT Url, Item, TStamp, Availability, ServiceUptime, HostUptime FROM SLSServices"
    if url != "":
      req += " WHERE Url = '%s'" % url

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.updateSLSServices) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value'][0]
