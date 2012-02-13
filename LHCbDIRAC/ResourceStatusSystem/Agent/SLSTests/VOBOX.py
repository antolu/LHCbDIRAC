################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRA                                                   import gConfig 
from LHCbDIRAC.ResourceStatusSystem.Agent.SLSTests.TestBase import TestBase

class TestModule( TestBase ):
  pass

  def launchTest( self ):
    '''
      Main method
    '''
    
    '''
     # Development hardcoded !!!!!!
    '''
    
    rm_urls = gConfig.getValue( '/Systems/RequestManagement/Development/URLs/allURLS', [] )
    cs_urls = gConfig.getServersList()
    
    
  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF