# $HeadURL:  $
''' SAMResultsCommand

  The Command is a command class to know about present SAM status.

'''

from DIRAC                                       import S_ERROR
from DIRAC.Core.LCG.SAMResultsClient             import SAMResultsClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName
from DIRAC.ResourceStatusSystem.Command.Command  import Command

__RCSID__ = "$Id$"

class SAMResultsCommand( Command ):
  '''
    SAMResultsCommand is a class to know about the results reported by SAM
  '''

  def __init__( self, args = None, clients = None ):

    super( SAMResultsCommand, self ).__init__( args, clients )

    if 'SAMResultsClient' in self.apis:
      self.samClient = self.apis[ 'SAMResultsClient' ]
    else:
      self.samClient = SAMResultsClient()

  def doCommand( self ):
    '''
    Return getStatus from SAM Results Client

    :attr:`args`:
     - args[0]: string: should be a ValidElement

     - args[1]: string: should be the (DIRAC) name of the ValidElement

     - args[2]: string: optional - should be the (DIRAC) site name of the ValidElement

     - args[3]: list: list of tests
    '''

    if not 'element' in self.args:
      return S_ERROR( 'SAMResultsCommand: "element" not found in self.args' )
    element = self.args[ 'element' ]

    if element not in ( 'Site', 'Resource' ):
      return S_ERROR( '%s is not a valid element' % element )

    if not 'siteName' in self.args:
      return S_ERROR( 'SAMResultsCommand: "siteName" not found in self.args' )
    siteName = self.args[ 'siteName' ]
    if siteName is None:
      return S_ERROR( 'SAMResultsCommand: "siteName" should not be None' )

    metrics = None
    if 'metrics' in self.args:
      metrics = self.args[ 'metrics' ]

    #####################

    gocSiteName = getGOCSiteName( siteName )
    if not gocSiteName[ 'OK' ]:
      return gocSiteName
    gocSiteName = gocSiteName[ 'Value' ]

    return self.samClient.getStatus( element, siteName, gocSiteName, metrics )

#...............................................................................
#EOF
