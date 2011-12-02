################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

'''
  Here are all known and relevant APIs for the ResourceStatusSystem Commands
  They can be either clients or RPC servers. 
'''
__APIs__ = {         
  'ResourceManagementClient' : 'LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient',
           }


import DIRAC.ResourceStatusSystem.Command.knownAPIs as DIRACknownAPIs

DIRACknownAPIs.__APIs__.update( __APIs__ )

def initAPIs( desiredAPIs, knownAPIs, force = False ):
  return DIRACknownAPIs.initAPIs( desiredAPIs, knownAPIs, force )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF