################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id$"

from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
     ResourceManagementClient as DIRACResourceManagementClient

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientFastDec

from datetime import datetime

class ResourceManagementClient( DIRACResourceManagementClient ):
  '''
  Class ResourceManagementClient, extension of the class with the same name
  on DIRAC.
  The extension provides the following methods:

  - getHCTestList
  - getHCTest
  - getHCTestListBySite

  - addMonitoringTest
  - updateMonitoringTest
  - getMonitoringTestList

  And also adds some more handy methods, which are not a plain return from
  the RPC server:

  - addOrUpdateMonitoringTest
  '''

  @ClientFastDec
  def insertMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour,
                            metricStatus, summaryData, timestamp, lastCheckTime, meta = {} ):
    return locals()
  @ClientFastDec
  def updateMonitoringTest( self, metricName, serviceURI, siteName, serviceFlavour,
                            metricStatus, summaryData, timestamp, lastCheckTime, meta = {} ):
    return locals()
  @ClientFastDec
  def getMonitoringTest( self, metricName = None, serviceURI = None, siteName = None,
                         serviceFlavour = None, metricStatus = None, summaryData = None,
                         timestamp = None, lastCheckTime = None, meta = {} ):
    return locals()
  @ClientFastDec
  def deleteMonitoringTest( self, metricName = None, serviceURI = None, siteName = None,
                            serviceFlavour = None, metricStatus = None, summaryData = None,
                            timestamp = None, lastCheckTime = None, meta = {} ):
    return locals()
  def addOrModifyMonitoringTest( self, metricName, serviceURI, siteName,
                                 serviceFlavour, metricStatus, summaryData,
                                 timestamp, lastCheckTime ):
    return self.__addOrModifyElement( 'MonitoringTest', locals() )

  @ClientFastDec
  def insertHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter, meta = {} ):
    return locals()
  @ClientFastDec
  def updateHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                             submissionTime, startTime, endTime, counterTime,
                             agentStatus, formerAgentStatus, counter, meta = {} ):
    return locals()
  @ClientFastDec
  def getHammerCloudTest( self, testID = None, siteName = None, resourceName = None,
                          testStatus = None, submissionTime = None, startTime = None,
                          endTime = None, counterTime = None, agentStatus = None,
                          formerAgentStatus = None, counter = None, meta = {} ):
    return locals()
  @ClientFastDec
  def deleteHammerCloudTest( self, testID = None, siteName = None, resourceName = None,
                             testStatus = None, submissionTime = None, startTime = None,
                             endTime = None, counterTime = None, agentStatus = None,
                             formerAgentStatus = None, counter = None, meta = {} ):
    return locals()
  def addOrModifyHammerCloudTest( self, testID, siteName, resourceName, testStatus,
                                  submissionTime, startTime, endTime, counterTime,
                                  agentStatus, formerAgentStatus, counter ):
    return self.__addOrModifyElement( 'HammerCloudTest', locals() )

  #
  # SLS Functions
  #

  # SLS Service ########################
  @ClientFastDec
  def insertSLSService(self, system, service, timeStamp,
                       availability, serviceUptime, hostUptime, instantLoad, meta = {}):
    return locals()

  @ClientFastDec
  def updateSLSService(self, system, service, timeStamp,
                       availability, serviceUptime, hostUptime, instantLoad, meta = {}):
    return locals()

  @ClientFastDec
  def getSLSService(self, system = None, service = None, timeStamp = None,
                    availability = None, serviceUptime = None, hostUptime = None, instantLoad = None, meta = {}):
    return locals()

  @ClientFastDec
  def deleteSLSService(self, system = None, service = None, timeStamp = None,
                       availability = None, serviceUptime = None, hostUptime = None, instantLoad = None, meta = {}):
    return locals()

  def addOrModifySLSService(self, system, service, timeStamp,
                            availability, serviceUptime, hostUptime, instantLoad):
    return self.__addOrModifyElement( 'SLSService', locals() )

  # SLST1Service ######################
  @ClientFastDec
  def insertSLST1Service(self, site, service, timeStamp, availability,
                         serviceUptime, hostUptime, meta = {}):
    return locals()

  @ClientFastDec
  def updateSLST1Service(self, site, service, timeStamp, availability,
                         serviceUptime, hostUptime, meta = {}):
    return locals()

  @ClientFastDec
  def getSLST1Service(self, site = None, service = None, timeStamp = None, availability = None,
                      serviceUptime = None, hostUptime = None, meta = {}):
    return locals()

  @ClientFastDec
  def deleteSLST1Service(self, site = None, service = None, timeStamp = None, availability = None,
                         serviceUptime = None, hostUptime = None, meta = {}):
    return locals()

  def addOrModifySLST1Service(self, site, service, timeStamp, availability,
                              serviceUptime, hostUptime):
    return self.__addOrModifyElement( 'SLST1Service', locals() )

  # SLS LogSE ########################
  @ClientFastDec
  def insertSLSLogSE(self, iD, timeStamp, validityDuration, availability,
                     dataPartitionUsed, dataPartitionTotal, meta = {}):
    return locals()

  @ClientFastDec
  def updateSLSLogSE(self, iD, timeStamp, validityDuration, availability,
                     dataPartitionUsed, dataPartitionTotal, meta = {}):
    return locals()

  @ClientFastDec
  def getSLSLogSE(self, iD = None, timeStamp = None, validityDuration = None, availability = None,
                  dataPartitionUsed = None, dataPartitionTotal = None, meta = {}):
    return locals()

  @ClientFastDec
  def deleteSLSLogSE(self, iD = None, timeStamp = None, validityDuration = None, availability = None,
                     dataPartitionUsed = None, dataPartitionTotal = None, meta = {}):
    return locals()

  def addOrModifySLSLogSE(self, iD, timeStamp, validityDuration, availability,
                          dataPartitionUsed, dataPartitionTotal):
    return self.__addOrModifyElement( 'SLSLogSE', locals() )

  # SLS Storage #########################
  @ClientFastDec
  def insertSLSStorage(self, site, token, timeStamp, availability, refreshPeriod,
                       validityDuration, totalSpace, guaranteedSpace, freeSpace, meta = {}):
    return locals()

  @ClientFastDec
  def updateSLSStorage(self, site, token, timeStamp, availability, refreshPeriod,
                       validityDuration, totalSpace, guaranteedSpace, freeSpace, meta = {}):
    return locals()

  @ClientFastDec
  def getSLSStorage(self, site = None, token = None, timeStamp = None, availability = None, refreshPeriod = None,
                       validityDuration = None, totalSpace = None, guaranteedSpace = None, freeSpace = None, meta = {}):
    return locals()

  @ClientFastDec
  def deleteSLSStorage(self, site = None, token = None, timeStamp = None, availability = None, refreshPeriod = None,
                       validityDuration = None, totalSpace = None, guaranteedSpace = None, freeSpace = None, meta = {}):
    return locals()

  def addOrModifySLSStorage(self, site, token, timeStamp, availability, refreshPeriod,
                       validityDuration, totalSpace, guaranteedSpace, freeSpace):
    return self.__addOrModifyElement( 'SLSStorage', locals() )

  # SLS CondDB ###########################
  @ClientFastDec
  def insertSLSCondDB(self, site, timeStamp, availability, accessTime, meta = {}):
    return locals()

  @ClientFastDec
  def updateSLSCondDB(self, site, timeStamp, availability, accessTime, meta = {}):
    return locals()

  @ClientFastDec
  def getSLSCondDB(self, site = None, timeStamp = None, availability = None, accessTime = None, meta = {}):
    return locals()

  @ClientFastDec
  def deleteSLSCondDB(self, site = None, timeStamp = None, availability = None, accessTime = None, meta = {}):
    return locals()

  def addOrModifySLSCondDB(self, site, timeStamp, availability, accessTime):
    return self.__addOrModifyElement( 'SLSCondDB', locals() )

  ### Helper functions ######################
  def __addOrModifyElement( self, element, kwargs ):

    del kwargs[ 'self' ]

    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
    sqlQuery = self._getElement( element, **kwargs )

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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
