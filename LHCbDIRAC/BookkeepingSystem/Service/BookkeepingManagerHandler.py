########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.1 2008/01/29 14:05:05 atsareg Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.1 2008/01/29 14:05:05 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """

  return S_OK()

class BookkeepingManagerHandler( RequestHandler ):

  def __init__(self):
    """ The new handler instance is created for each service query. Put here neccesary
        initializations before the uery is executed.
    """

    pass

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed 
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self,input):
    """ Echo input to output
    """

    return S_OK(input) 
