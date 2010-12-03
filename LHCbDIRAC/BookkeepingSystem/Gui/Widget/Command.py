########################################################################
# $Id: Command.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: Command.py 18161 2009-11-11 12:07:09Z acasajus $"

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