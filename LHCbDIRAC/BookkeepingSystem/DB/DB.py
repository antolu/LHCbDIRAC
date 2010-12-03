########################################################################
# $Id: DB.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import mdclient

__RCSID__ = "$Id: DB.py 18161 2009-11-11 12:07:09Z acasajus $"

class DB(mdclient.MDClient,object):
  
  #############################################################################
  def __init__(self, host, port, password = ''):
    super(DB, self).__init__(host, port, password)
    try:
      self.connect()
    except Exception, ex:
      gLogger.error(ex)    
  #############################################################################
    