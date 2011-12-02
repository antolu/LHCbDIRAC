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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    