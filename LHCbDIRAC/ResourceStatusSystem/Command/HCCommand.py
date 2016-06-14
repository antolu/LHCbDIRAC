''' HCCommand

 Command to interact with the server of HammerCloud.

'''

from DIRAC                                                          import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                     import Command

from LHCbDIRAC.ResourceStatusSystem.Client.HCClient                 import HCClient

__RCSID__ = "$Id$"

class HCCommand( Command ):

 def doCommand( self, site = None ):
   """
     Gets from HC the last test for that site.
   """

   hcClient = HCClient()

   result = hcClient.getHistoryReport('1')['sites'][site]

   return { 'Result' : result }

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
