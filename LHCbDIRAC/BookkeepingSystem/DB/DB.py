########################################################################
# $Id: DB.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import mdclient

__RCSID__ = "$Id: DB.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $"

class DB(mdclient.MDClient,object):
  
  #############################################################################
  def __init__(self, host, port, password = ''):
    super(DB, self).__init__(host, port, password)
    try:
      self.connect()
    except Exception, ex:
      gLogger.error(ex)    
  #############################################################################
    