from LHCbDIRAC.ResourceStatusSystem.Utilities                       import SLSXML
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

_RMC = ResourceManagementClient()

def writeSLSXml( task, taskResult ):
  return SLSXML.writeSLSXml( task, taskResult, _RMC )
