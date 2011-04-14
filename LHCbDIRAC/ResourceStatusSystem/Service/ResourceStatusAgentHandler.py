""" ``ResourceStatusAgentHandler`` exposes the Status of the ResourceStatusAgent System. 
    It uses :mod:`LHCbDIRAC.ResourceStatusSystem.DB.ResourceStatusAgentDB` for database persistence. 
    
    To use this service
      
    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceStatusAgent")

"""
__RCSID__ = "$Id$"

# it crashes epydoc
# __docformat__ = "restructuredtext en"


from types import StringType, IntType, BooleanType#, DictType, ListType, IntType
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gConfig

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import RSSDBException

from LHCbDIRAC.ResourceStatusSystem.DB.ResourceStatusAgentDB import ResourceStatusAgentDB, RSSAgentDBException


rsaDB = False

def initializeResourceStatusAgentHandler(serviceInfo):

  global rsaDB
  rsaDB = ResourceStatusAgentDB()
    
  return S_OK()

class ResourceStatusAgentHandler(RequestHandler):

  def initialize(self):
    pass

##############################################################################

  types_getTestList = []
  def export_getTestList( self ):
    """ blah blah blah
    """   
    try:
      gLogger.info("ResourceStatusAgentHandler.getTestList: Attempting to get test list")
      try:
        res = rsaDB.getTestList()
      except RSSAgentDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusAgentHandler.getTestList: got test list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getTestList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
  
##############################################################################  
  
  types_getTestListBySite = [ StringType, BooleanType ]    
  def export_getTestListBySite( self, siteName, last ):
    """ blah blah blah
    """   
    try:
      gLogger.info("ResourceStatusAgentHandler.getTestListBySite: Attempting to get test list by siteName")
      try:
        res = rsaDB.getTestList( siteName = siteName, last = last )
      except RSSAgentDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusAgentHandler.getTestListBySite: got test list by siteName")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getTestList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

##############################################################################       

  types_getLastTest = [ StringType, StringType ]    
  def export_getLastTest( self, siteName, reason ):
    """ blah blah blah
    """   
    try:
      gLogger.info("ResourceStatusAgentHandler.getLastTest: Attempting to get last test by siteName and reason")
      try:
        res = rsaDB.getTestList( siteName = siteName, reason = reason, last = True )
      except RSSAgentDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusAgentHandler.getLastTest: got last test by siteName and reason")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getLastTest)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
       
##############################################################################       
       
#  types_getTestListByStatus = [ StringType ]
#  def export_getTestListByStatus( self, status ):
#    """ blah blah blah
#    """   
#    try:
#      gLogger.info("ResourceStatusAgentHandler.getTestListByStatus: Attempting to get test list by status")
#      try:
#        res = rsaDB.getTestList( status = status )
#      except RSSAgentDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusAgentHandler.getTestListByStatus: got test list by status")
#      return S_OK(res)
#    except Exception:
#      errorStr = where(self, self.export_getTestList)
#      gLogger.exception(errorStr)
#      return S_ERROR(errorStr)    
    
##############################################################################  
  
  types_getTest = [ IntType ]
  def export_getTest( self, testID ):
    """ blah blah blah
    """   
    try:
      gLogger.info("ResourceStatusAgentHandler.getTest: Attempting to get test")
      try:
        res = rsaDB.getTestList( testID = testID )
      except RSSAgentDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusAgentHandler.getTestList: got test")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getTestList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
        

##############################################################################

