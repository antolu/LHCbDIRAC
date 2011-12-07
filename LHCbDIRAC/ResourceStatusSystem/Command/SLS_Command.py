"""
  The SLS_Command class is a command class to properly interrogate the SLS
"""

__RCSID__ = "$Id:  $"

import re

from DIRAC                                           import gLogger
from DIRAC.ResourceStatusSystem.Command.Command      import Command
from DIRAC.ResourceStatusSystem.Utilities            import CS
from DIRAC.Core.LCG                                  import SLSClient

def slsid_of_service(granularity, name, type_ = None):
  """Return the SLS id of various services."""
  if type_ == "CondDB"   : return name.split('@')[1] + "_CondDB"
  if type_ == 'VO-BOX'   : return name.split('.')[1] + "_VOBOX"
  elif type_ == 'VOMS'   : return 'VOMS'
  elif (granularity, type_) == ("StorageElement", None) : return re.split("[-_]", name)[0] + "_" + CS.getSEToken(name)
  elif type_ == "CASTOR" :
    try               : return "CASTORLHCB_LHCB" + re.split("[-_]", CS.getSEToken(name))[1].upper()
    except IndexError : return ""
  else                : return ""

class SLSStatus_Command(Command):
  def doCommand(self):
    """
    Return getAvailabilityStatus from SLS Client

    :attr:`args`:
     - args[0]: string: should be a granularity
     - args[1]: string: should be the (DIRAC) name of the granularity
     - args[2]: string: should be the granularity type (e.g. 'VO-BOX')
    """

    super(SLSStatus_Command, self).doCommand()

    res = SLSClient.getAvailabilityStatus(slsid_of_service(*self.args))

    if not res['OK']:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return  { 'Result': None }
    return { 'Result': res['Value']['Availability'] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

class SLSLink_Command(Command):
  def doCommand(self):
    """
    Return getStatus from SLS Client

    :attr:`args`:
     - args[0]: string: should be a granularity
     - args[1]: string: should be the (DIRAC) name of the granularity
     - args[2]: string: should be the granularity type (e.g. 'VO-BOX')
    """

    super(SLSLink_Command, self).doCommand()

    res = SLSClient.getAvailabilityStatus(slsid_of_service(*self.args))

    if not res['OK']:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return  { 'Result': None }
    return { 'Result': res['Value']['Weblink'] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

class SLSServiceInfo_Command(Command):
  def doCommand(self):
    """
    Return getServiceInfo from SLS Client

    :attr:`args`:
     - args[0]: string: should be a ValidRes
     - args[1]: string: should be the (DIRAC) name of the ValidRes
    """

    super(SLSServiceInfo_Command, self).doCommand()
    if self.args[0] == 'StorageElement':
      SLSName = slsid_of_service(self.args[0], self.args[1], "CASTOR")
    else:
      SLSName = slsid_of_service(*self.args)

    res = SLSClient.getServiceInfo(SLSName)
    if not res[ 'OK' ]:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return { 'Result' : None }
    else:
      return { 'Result' : res["Value"] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
