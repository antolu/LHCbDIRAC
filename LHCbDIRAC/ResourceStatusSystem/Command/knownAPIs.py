''' knownAPIs

  Here are all known and relevant APIs for the ResourceStatusSystem Commands
  They can be either clients or RPC servers.
   
'''

import DIRAC.ResourceStatusSystem.Command.knownAPIs as DIRACknownAPIs

# $HeadURL $
__RCSID__ = '$Id: $'

__APIs__ = {         
  'ResourceManagementClient' : 'LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient',
           }

DIRACknownAPIs.__APIs__.update( __APIs__ )

def initAPIs( desiredAPIs, knownAPIs, force = False ):
  return DIRACknownAPIs.initAPIs( desiredAPIs, knownAPIs, force )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF