########################################################################
# $Id: BaseModule.py,v 1.1 2008/05/06 04:08:56 gkuznets Exp $
########################################################################
""" Just example of the Base class """

__RCSID__ = "$Id: BaseModule.py,v 1.1 2008/05/06 04:08:56 gkuznets Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

class BaseModule(object):

  def __init__(self):
    self.enable = true
    self.result_prev = S_OK("Default value")
    self.version = __RCSID__
    self.debug = True
    #self.log = gLogger.getSubLogger( "BaseModule" )
    self.result = S_OK("Default value")

  def execute(self):
    if not self.enable:
      self.result = S_OK("Module was disabled by the workflow author; %s" % self.create_warning() )
      return self.result

    if not self.result_prev["OK"]:
      # possible error during execution of the previous module
      # we have to make a decision what to do
      self.result = S_ERROR("Module was disabled due to ERROR in the previous module")
      return self.result

    # some calculation here
    self.result = S_ERROR("Error during execution of %s" % self.create_warning())
    return self.result


  def crate_warning(self):
    return "module number % instname=%s of type=% within step % instname=%s of type %s" % \
    (self.MODULE_NUMBER, self.MODULE_INSTANCE_NAME, self.MODULE_DEFINITION_NAME, \
     self.STEP_NUMBER, self.STEP_INSTANCE_NAME, self.STEP_DEFINITION_NAME)
