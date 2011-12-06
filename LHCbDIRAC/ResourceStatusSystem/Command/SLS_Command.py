"""
  The SLS_Command class is a command class to properly interrogate the SLS
"""

__RCSID__ = "$Id:  $"

import re

from DIRAC                                           import gLogger
from DIRAC.ResourceStatusSystem.Command.Command      import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities            import CS
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where
from DIRAC.Core.LCG                                  import SLSClient

def _getSESLSName(SE):
  """Return the SLS id corresponding to the given SE (LHCb Storage
  Space section in SLS"""
  return re.split("[-_]", SE)[0] + "_" + CS.getSEToken(SE)

def _getCastorSESLSName(SE):
  """Return the SLS id corresponding to the given SE (CASTORLHCb
  section in SLS)"""
  try:
    return "CASTORLHCB_LHCB" + re.split("[-_]", CS.getSEToken(SE))[1].upper()
  except IndexError:
    return ""

def _getServiceSLSName(serv, type_):
  """Return the SLS id of various services."""
  if type_ == 'VO-BOX': return serv.split('.')[1] + "_VOBOX"
  elif type_ == 'VOMS': return 'VOMS'
  else                : return ""

class SLSStatus_Command(Command):
  def doCommand(self):
    """
    Return getStatus from SLS Client

    :attr:`args`:
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes

     - args[2]: string: should be the ValidRes type (e.g. 'VO-BOX')
    """

    super(SLSStatus_Command, self).doCommand()

    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)

    res = SLSClient.getAvailabilityStatus(SLSName)
    if not res['OK']:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return  { 'Result': None }
    return { 'Result': res['Value'] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

class SLSServiceInfo_Command(Command):
  def doCommand(self):
    """
    Return getServiceInfo from SLS Client

    :attr:`args`:
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes

     - args[2]: list: list of info requested
    """

    super(SLSServiceInfo_Command, self).doCommand()
    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getCastorSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)

    res = SLSClient.getServiceInfo(SLSName)
    if not res[ 'OK' ]:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return { 'Result' : None }
    else:
      return { 'Result' : res["Value"] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

class SLSLink_Command(Command):
  def doCommand(self):
    """
    Return getLink from SLS Client

    :attr:`args`:
      - args[0]: string: should be a ValidRes

      - args[1]: string: should be the (DIRAC) name of the ValidRes
    """

    super(SLSLink_Command, self).doCommand()

    if self.args[0] == 'StorageElement':
      #know the SLS name of the SE
      SLSName = _getSESLSName(self.args[1])
    elif self.args[0] == 'Service':
      #know the SLS name of the VO BOX
      SLSName = _getServiceSLSName(self.args[1], self.args[2])
    else:
      raise InvalidRes, where(self, self.doCommand)

    res = SLSClient.getInfo(SLSName)
    if not res['OK']:
      gLogger.error("No SLS sensors for " + self.args[0] + " " + self.args[1] )
      return { 'Result':None }
    else:
      return { 'Result': res['Weblink'] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

'''
  HOW DOES THIS WORK.

    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
