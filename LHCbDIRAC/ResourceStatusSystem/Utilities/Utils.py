from LHCbDIRAC.ResourceStatusSystem.Utilities                       import SLSXML
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

rmc = ResourceManagementClient()

def writeSLSXml( task, taskResult ):
  return SLSXML.writeSLSXml( task, taskResult, rmc )
