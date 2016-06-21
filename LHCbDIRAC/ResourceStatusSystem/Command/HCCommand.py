''' HCCommand

 Command to interact with the server of HammerCloud.

'''

from DIRAC                                                          import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                     import Command

from LHCbDIRAC.ResourceStatusSystem.Client.HCClient                 import HCClient

__RCSID__ = "$Id$"


class HCCommand( Command ):

 def doNew( self, masterParams = None ):
   '''
    Gets from HC the last test for a given site.

     :params:
     :attr: `site` : str - the name of the site.
     :attr: `days` : int - days in the past to take in consideration.

    :return:
    {
      'OK': True,
      'Value':
        [
          {
            'count': ...,
            'reason': ...,
            'ganga_status': ...,
            'failover': ...,
            'minor_reason': ...
            },
             ...
             ...
          }
        ]
    }
   '''

   site, days = masterParams

   hcClient = HCClient()

   result = hcClient.getHistoryReport(days)['sites']

   if site in result:
     return S_OK( result[site] )
   else:
     return S_ERROR( 'No results exist for site %s in the past %s days' % (site, days) )

 def doCache( self ):
   pass

 def doMaster( self ):
   pass

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
