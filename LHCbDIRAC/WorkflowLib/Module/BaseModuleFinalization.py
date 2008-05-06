########################################################################
# $Id: BaseModuleFinalization.py,v 1.1 2008/05/06 04:07:33 gkuznets Exp $
########################################################################
""" Just example of the Base class """

__RCSID__ = "$Id: BaseModuleFinalization.py,v 1.1 2008/05/06 04:07:33 gkuznets Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Utilities.Tools import combineMutipleAttributes

class BaseModuleFinalization(BaseModule):

  def __init__(self):
    BaseModule.__init__(self)

    self.version = __RCSID__
    self.debug = True

    self.result_1 = None
    self.result_2 = None
    self.result_3 = None
    self.result_4 = None
    self.result_5 = None
    self.result_6 = None

  def execute(self):
    if not self.enable:
      self.result = S_OK("Module was disabled by the workflow author; %s" % self.create_warning() )
      return self.result

    self.result_combined = combineMutipleAttributes(self)
    for ind in self.result_combined.keys():
        if self.result_combined[ind]["result"] != None:
           # here we can sort out which module produced error
           if not self.result_combined[ind]["result"]["OK"]:
             print "Error in the Module ", self.result_combined[ind]["result"]

    # some calculation here
    # if result is fine comment next line
    self.result = S_ERROR("Error during execution of %s" % self.create_warning())
    return self.result


