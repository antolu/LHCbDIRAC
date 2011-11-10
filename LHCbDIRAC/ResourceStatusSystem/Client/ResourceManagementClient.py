"""
LHCbDIRAC/ResourceStatusSystem/Client/ResourceManagementClient.py
"""

__RCSID__ = "$Id$"

# First pythonic stuff
# ...

# Second, DIRAC stuff
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
     ResourceManagementClient as DIRACResourceManagementClient

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientFastDec

from datetime import datetime

# Third, LHCbDIRAC stuff
# ...

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
## HammerCloudTest functions
################################################################################
#
#  def getHCTestList( self ):
#    '''
#    Returns a list with the HammerCloud tests on the DB, if any.  
#    '''
#
#    return self.rsM.getHCTestList()
#
################################################################################
#
#  def getHCTest( self, testID ):
#    '''
#    Returns the details of a specific HammerCloud test.
#    
#    :params:
#      :attr: `testID` : integer - (optional) assigned (by HC) test ID    
#    '''
#
#    return self.rsM.getHCTest( testID )    
#
################################################################################
#      
#  def getHCTestListBySite( self, siteName, last ):
#    '''
#    Returns the details of the tests on a specific site if any. If last
#    parameter is used, only the last entry of the selection will be returned.
#    
#    :params:
#      :attr:`siteName`: string - site where the test is submitted
#      
#      :attr: `last` : bool - return last entry
#    '''
#        
#    return self.rsM.getHCTestListBySite( siteName, last )
#
################################################################################
## END HammerCloudTest functions
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    