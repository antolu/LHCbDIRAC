# $HeadURL: $
''' Utils

  This is a workaround to reuse the ResourceManagementClient on the SLSXML module.
    
'''

from LHCbDIRAC.ResourceStatusSystem.Utilities                       import SLSXML
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__  = '$Id:  $'

_RMC       = ResourceManagementClient()

def writeSLSXml( task, taskResult ):
  '''
  Call the real SLSXML.writeSLSXml function, but adding the connection to the 
  DB.
  '''
  return SLSXML.writeSLSXml( task, taskResult, _RMC )
