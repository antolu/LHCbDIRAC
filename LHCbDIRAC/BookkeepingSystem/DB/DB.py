########################################################################
# $Id$
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import mdclient

__RCSID__ = "$Id$"

class DB(mdclient.MDClient,object):
  
  #############################################################################
  def __init__(self, host, port, password = ''):
    super(DB, self).__init__(host, port, password)
    try:
      self.connect()
    except Exception, ex:
      gLogger.error(ex)    
  #############################################################################
    