'''
LHCbDIRAC/ResourceStatusSystem/Service/ResourceManagementHandler.py  
'''

__RCSID__ = "$Id$"

# First, pythonic stuff
from types import BooleanType, IntType, StringType, NoneType
from datetime import datetime

# Second, DIRAC stuff
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import whoRaised, where
from DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler import \
     ResourceManagementHandler as DIRACResourceManagementHandler 

# Third, LHCbDIRAC stuff
from LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

#Horrible, horrible, horrible !!!
rmDB = False
rsDB = False

def initializeResourceManagementHandler( serviceInfo ):
  '''
  Initialize of the ResourceManagementHandler.
  Starts connections with the DBs:
    
  - ResourceStatusDB
  - ResourceManagementDB
  '''
  
  global rsDB
  rsDB = ResourceStatusDB()    
  global rmDB
  rmDB = ResourceManagementDB()
   
  return S_OK()

################################################################################

class ResourceManagementHandler( DIRACResourceManagementHandler ):
  '''
  Class ResourceManagementHandler, extension of the class with the same name
  on DIRAC.
    
  The extension provides the following methods:
  Note:
    marked with "C" if they have a client method with the same name
    to access them.
    
  - getHCTestList                 C              
  - getHCTestListBySite           C
  - getHCTest                     C
  
  - addMonitoringTest             C 
  - updateMonitoringTest          C
  - getMonitoringTestList         C  
  '''

################################################################################
    
  def __init__( self, *args, **kargs ):
    '''
    Constructor of the class ResourceManagementHandler.
    It has a particularity, in order to instantiate the proper Resource_X_DB,
    we have to make use of the inherited method setResource_X_Database.
    '''
    
    self.setResourceStatusDatabase( rsDB )
    self.setResourceManagementDatabase( rmDB )
    
    DIRACResourceManagementHandler.__init__( self, *args, **kargs )   
    
################################################################################
# HammerCloudTest functions
################################################################################

  types_getHCTestList = []
  def export_getHCTestList( self ):
    ''' 
    Method that connects to the ResourceManagementDB to get all tests on the
    HammerCloudTests table. 
    
    :params:
    '''   

    gLogger.info( "ResourceManagementHandler.getHCTestList: Attempting to get test list" )
    res = []
    try:
    
      res = rmDB.getHCTestList()
    
    except RSSException, x:
      gLogger.error( whoRaised( x ) )
      errorStr = where( self, self.export_getHCTestList )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )  
   
    gLogger.info( "ResourceManagementHandler.getHCTestList: got test list" )
    return S_OK( res )

################################################################################  
  
  types_getHCTestListBySite = [ StringType, BooleanType ]    
  def export_getHCTestListBySite( self, siteName, last ):
    ''' 
    Method that connects to the ResourceManagementDB to get all tests on the
    HammerCloudTests table on siteName. If the last flag is used, it will 
    return the latest entry on the selection.
    
    :params:
      :attr: `siteName` : string - site where the test run
      
      :attr: `last` : bool - return last entry
    '''   

    gLogger.info( "ResourceManagementHandler.getHCTestListBySite: Attempting to \
                   get test list by siteName" )
    res = []
    try:
    
      res = rmDB.getHCTestList( siteName = siteName, last = last )
    
    except RSSException, x:
      gLogger.error( whoRaised( x ) )
      errorStr = where( self, self.export_getHCTestList )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )
     
    gLogger.info( "ResourceManagementHandler.getHCTestListBySite: got test list by siteName" )
    return S_OK( res )

################################################################################
  
  types_getHCTest = [ IntType ]
  def export_getHCTest( self, testID ):
    ''' 
    Method that connects to the ResourceManagementDB to get the test with
    testID.
    
    :params:
      :attr: `testID` : integer - assigned (by HC) test ID
    '''  

    gLogger.info( "ResourceManagementHandler.getHCTest: Attempting to get test" )
    res = []
    try:
 
      res = rmDB.getHCTestList( testID = testID )
    
    except RSSException, x:
      gLogger.error( whoRaised( x ) )
      errorStr = where( self, self.export_getHCTestList )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

    gLogger.info( "ResourceManagementHandler.getHCTestList: got test" )
    return S_OK( res )

################################################################################
# END HammerCloudTest functions
################################################################################

################################################################################
# MonitoringTest functions
################################################################################

  types_addMonitoringTest = [ 
                             StringType, StringType, StringType, StringType, 
                             StringType, StringType, datetime
                            ]
  def export_addMonitoringTest( self, metricName, serviceURI, siteName, 
                                serviceFlavour, metricStatus, summaryData, 
                                timestamp ):
    ''' 
    Method that connects to the ResourceManagementDB to add a test on the
    MonitoringTests table. 
    
    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service
           
      :attr:`siteName`: string - site where the test was run
      
      :attr:`serviceFlavour`: string - type of service tested
      
      :attr:`metricStatus`: string - output of the test
      
      :attr:`summaryData`: string - detailed output
      
      :attr:`timestamp`: date time - test execution timestamp  
    '''   
  
    gLogger.info( "ResourceManagementHandler.addMonitoringTest: Attempting to add a test" )
    res = []
    try:

      res = rmDB.addMonitoringTest( metricName, serviceURI, siteName, 
                                    serviceFlavour, metricStatus, summaryData, 
                                    timestamp )
          
    except RSSException, x:
      gLogger.error( whoRaised( x ) )
      errorStr = where( self, self.export_addMonitoringTest )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

    gLogger.info( "ResourceManagementHandler.addMonitoringTest: added test" )          
    return S_OK( res )        

################################################################################  

  types_updateMonitoringTest = [ 
                                StringType, StringType, ( StringType, NoneType ) , 
                                ( StringType, NoneType ), ( StringType, NoneType ),
                                ( StringType, NoneType ), ( datetime, NoneType ) 
                               ]
  def export_updateMonitoringTest( self, metricName, serviceURI, siteName, 
                                   serviceFlavour, metricStatus, summaryData, 
                                   timestamp ):
    ''' 
    Method that connects to the ResourceManagementDB to update a test on the
    MonitoringTests table. 
    
    :params:
      :attr:`metricName`: string - name of the metric

      :attr:`serviceURI`: string - full name of the service
           
      :attr:`siteName`: string / None - site where the test was run
      
      :attr:`serviceFlavour`: string / None  - type of service tested
      
      :attr:`metricStatus`: string / None - output of the test
      
      :attr:`summaryData`: string / None - detailed output
      
      :attr:`timestamp`: date time / None - test execution timestamp  
    '''     

    gLogger.info( "ResourceManagementHandler.updateMonitoringTest: Attempting to update a test" )
    res = []
    try:

      res = rmDB.updateMonitoringTest( metricName, serviceURI, siteName, 
                                       serviceFlavour, metricStatus, summaryData, 
                                       timestamp )

    except RSSException, x:
      gLogger.error( whoRaised( x ) )  
      errorStr = where( self, self.export_updateMonitoringTest )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )   


    gLogger.info( "ResourceManagementHandler.updateMonitoringTest: updated test" )
    return S_OK( res )

################################################################################ 

  types_getMonitoringTestList = [ 
                                ( StringType, NoneType ), ( StringType, NoneType ), 
                                ( StringType, NoneType ), ( StringType, NoneType ), 
                                ( StringType, NoneType ), ( StringType, NoneType ), 
                                ( datetime, NoneType ) 
                                ]
  def export_getMonitoringTestList( self, metricName, serviceURI, siteName, 
                                serviceFlavour, metricStatus, summaryData, timestamp ):
    ''' 
    Method that connects to the ResourceManagementDB to get the tests matching
    the filters above.
    
    :params:
      :attr:`metricName`: string / None - name of the metric

      :attr:`serviceURI`: string / None - full name of the service
           
      :attr:`siteName`: string / None - site where the test was run
      
      :attr:`serviceFlavour`: string / None  - type of service tested
      
      :attr:`metricStatus`: string / None - output of the test
      
      :attr:`summaryData`: string / None - detailed output
      
      :attr:`timestamp`: date time / None - test execution timestamp  
    '''      

    gLogger.info( "ResourceManagementHandler.getMonitoringTestList: Attempting to get test list" )
    res = []
    try:

      res = rmDB.getMonitoringTestList( metricName, serviceURI, siteName, 
                                        serviceFlavour, metricStatus, summaryData, 
                                        timestamp )
    except RSSException, x:
      gLogger.error( whoRaised( x ) )  
      errorStr = where( self, self.export_getMonitoringTestList )
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )   

    gLogger.info( "ResourceManagementHandler.getMonitoringTestList: got test list" )
    return S_OK( res )

################################################################################   
# END MonitoringTest functions
################################################################################   

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF        