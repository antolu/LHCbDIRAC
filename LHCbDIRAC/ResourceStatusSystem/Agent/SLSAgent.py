from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.ResourceStatusSystem.Utilities import CS

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SLSAgent'

class SLSAgent(AgentModule):

  def execute(self):

    tier1_SE = CS.getTypedDictRootedAt(root="", relpath="/Resources/StorageElementGroups")['Tier1-USER']
    all_SE =  CS.getTypedDictRootedAt(root="", relpath="/Resources/StorageElements")


    stuffToCheck = []
    for se in tier1_SE:
      res = {}
      res['site'] = se.split("-")[0]
      res['endpoint'] = "httpg://" + all_SE[se]["AccessProtocol.1"]["Host"] + ":" + \
          str(all_SE[se]["AccessProtocol.1"]["Port"]) + all_SE[se]["AccessProtocol.1"]["WSUrl"].split("?")[0]
      res['token'] = ["LHCb-Tape", "LHCb-Disk", "LHCb_USER"]

      stuffToCheck.append(res)


    print stuffToCheck
    return S_OK()
