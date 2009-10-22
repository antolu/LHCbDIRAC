########################################################################
# $Id: Command.py,v 1.1 2009/10/22 20:38:03 zmathe Exp $
########################################################################

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: Command.py,v 1.1 2009/10/22 20:38:03 zmathe Exp $"

########################################################################
class Command:
  
  def __init__(self):
    pass
  ########################################################################
  def execute(self):
    gLogger.error('Can not use this method from this class')
  
  ########################################################################
  def unexecute(self):
    gLogger.error('Can not use this method from this class')