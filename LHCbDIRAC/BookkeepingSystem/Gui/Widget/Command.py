########################################################################
# $Id$
########################################################################

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

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