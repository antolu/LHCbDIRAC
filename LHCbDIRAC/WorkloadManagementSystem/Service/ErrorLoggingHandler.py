########################################################################
# $Id$
########################################################################

""" The error logging service does something...
"""

__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from LHCbDIRAC.LHCbSystem.DB.ErrorLoggingDB import ErrorLoggingDB

errorLoggingDB = None

def initializeErrorLoggingHandler( serviceInfo ):

  global errorLoggingDB
  errorLoggingDB = ErrorLoggingDB()
  return S_OK()

class ErrorLoggingHandler( RequestHandler ):

  ###########################################################################
  def initialize(self):
    pass

  ###########################################################################
  types_sayHello = [StringType]
  def export_sayHello(self,helloString):
    """ Test 
    """
    gLogger.info('Received action to say: %s' %helloString)

    return S_OK(helloString)

  ###########################################################################
  types_setError = [LongType,StringType,StringType,LongType]
  def export_setError(self,production,project,version,errornumber):
    """ New error record
    """
    gLogger.info('Received action to create error: %d %s %s' %(production,project,version))
    
    result = errorLoggingDB.setError(production,project,version,errornumber)

    return result

  ###########################################################################
  types_getErrors = [LongType,StringType,StringType,LongType]
  def export_getErrors(self,production,project,version,errornumber):
    """ Get errors 
    """

    result = errorLoggingDB.getErrors(production,project,version,errornumber)
    return result
