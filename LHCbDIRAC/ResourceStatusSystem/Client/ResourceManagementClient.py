"""
LHCbDIRAC/ResourceStatusSystem/Client/ResourceManagementClient.py
"""

__RCSID__ = "$Id$"

# First pythonic stuff
# ...

# Second, DIRAC stuff
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
     ResourceManagementClient as DIRACResourceManagementClient

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

################################################################################
# HammerCloudTest functions
################################################################################

  def getHCTestList( self ):
    '''
    Returns a list with the HammerCloud tests on the DB, if any.  
    '''

    return self.rsM.getHCTestList()

################################################################################

  def getHCTest( self, testID ):
    '''
    Returns the details of a specific HammerCloud test.
    
    :params:
      :attr: `testID` : integer - (optional) assigned (by HC) test ID    
    '''

    return self.rsM.getHCTest( testID )    

################################################################################
      
  def getHCTestListBySite( self, siteName, last ):
    '''
    Returns the details of the tests on a specific site if any. If last
    parameter is used, only the last entry of the selection will be returned.
    
    :params:
      :attr:`siteName`: string - site where the test is submitted
      
      :attr: `last` : bool - return last entry
    '''
        
    return self.rsM.getHCTestListBySite( siteName, last )

################################################################################
# END HammerCloudTest functions
################################################################################

################################################################################
# MonitoringTest functions
################################################################################

  def addMonitoringTest( self, metricName, serviceURI, siteName, 
                         serviceFlavour, metricStatus, summaryData, timestamp ):
    '''
    Adds, if not already there a test with same metricName and serviceURI, a 
    new entry on the DB with all parameters above. If not, an exception will be
    returned.
    
    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service
           
      :attr:`siteName`: string - site where the test was run
      
      :attr:`serviceFlavour`: string - type of service tested
      
      :attr:`metricStatus`: string - output of the test
      
      :attr:`summaryData`: string - detailed output
      
      :attr:`timestamp`: date time - test execution timestamp 
    '''

    return self.rsM.addMonitoringTest( metricName, serviceURI, siteName, 
                                       serviceFlavour, metricStatus, summaryData, 
                                       timestamp )
      
################################################################################

  def updateMonitoringTest( self, metricName, serviceURI, siteName = None, 
                            serviceFlavour = None, metricStatus = None, 
                            summaryData = None, timestamp = None ):
    '''
    Updates the entry with metricName and serviceURI if any. If not, an
    exception will be returned.
    
    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service
           
      :attr:`siteName`: string - (optional) site where the test was run
      
      :attr:`serviceFlavour`: string - (optional) type of service tested
      
      :attr:`metricStatus`: string - (optional) output of the test
      
      :attr:`summaryData`: string - (optional) detailed output
      
      :attr:`timestamp`: date time - (optional) test execution timestamp 
    '''


    return self.rsM.updateMonitoringTest( metricName, serviceURI, siteName, 
                                          serviceFlavour, metricStatus, summaryData, 
                                          timestamp )

################################################################################

  def getMonitoringTestList( self, metricName = None, serviceURI = None, 
                            siteName = None, serviceFlavour = None, 
                            metricStatus = None, summaryData = None, timestamp = None ):
    '''
    Returns a list of Monitoring tests, using the parameters as filters. 
    
    :params:
      :attr:`metricName`: string - (optional) name of the metric

      :attr:`serviceURI`: string - (optional) full name of the service
           
      :attr:`siteName`: string - (optional) site where the test was run
      
      :attr:`serviceFlavour`: string - (optional) type of service tested
      
      :attr:`metricStatus`: string - (optional) output of the test
      
      :attr:`summaryData`: string - (optional) detailed output
      
      :attr:`timestamp`: date time - (optional) test execution timestamp     
    '''

    return self.rsM.getMonitoringTestList( metricName, serviceURI, siteName, 
                                           serviceFlavour, metricStatus, summaryData, 
                                           timestamp )


################################################################################

  def addOrUpdateMonitoringTest( self, metricName, serviceURI, siteName, 
                         serviceFlavour, metricStatus, summaryData, timestamp ):
    '''
    This method adds a monitoring test if there is no entry on the DB with
    metricName and serviceURI. If there is already one, it updates it.
    
    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service
           
      :attr:`siteName`: string - site where the test was run
      
      :attr:`serviceFlavour`: string - type of service tested
      
      :attr:`metricStatus`: string - output of the test
      
      :attr:`summaryData`: string - detailed output
      
      :attr:`timestamp`: date time - test execution timestamp    
    '''
    
    res = self.rsM.getMonitoringTestList( metricName, serviceURI )
    
    if res:
      res = self.rsM.updateMonitoringTestList( metricName, serviceURI, siteName, 
                                               serviceFlavour, metricStatus, 
                                               summaryData, timestamp )
    else:
      res = self.rsM.addMonitoringTest( metricName, serviceURI, siteName, 
                                        serviceFlavour, metricStatus, summaryData, 
                                        timestamp )
    return res
  
################################################################################
# END MonitoringTest functions
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    