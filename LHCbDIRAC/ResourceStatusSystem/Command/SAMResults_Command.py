''' SAMResults_Command
  
  The Command is a command class to know about present SAM status.
  
'''

import httplib
import urllib2

from DIRAC                                           import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where 

# $HeadURL $
__RCSID__ = '$Id: $'

class SAMResults_Command( Command ):
  
  __APIs__ = [ 'SAMResultsClient', 'ResourceStatusClient' ]
  
  def doCommand(self, rsClientIn=None):
    """ 
    Return getStatus from SAM Results Client  
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the (DIRAC) name of the ValidRes
     
     - args[2]: string: optional - should be the (DIRAC) site name of the ValidRes
     
     - args[3]: list: list of tests
    """

    super(SAMResults_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      granularity = self.args[0]
      name        = self.args[1]
      
      if len( self.args ) > 2:  
        siteName = self.args[2]
      else:
        siteName = None

      if granularity == 'Site':
        siteName = getGOCSiteName(name)
        if not siteName['OK']:
          return siteName
        siteName = siteName['Value']
        
      elif granularity == 'Resource':
        if siteName is None:
          siteName = self.APIs[ 'ResourceStatusClient' ].getGridSiteName(granularity, name)
          if not siteName['OK']:
            return siteName    
          siteName = siteName[ 'Value' ]
        else:
          siteName = getGOCSiteName(siteName)
          if not siteName['OK']:
            return siteName
          siteName = siteName['Value']
      else:
        return S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) 
    
      if len( self.args ) > 3:
        tests = self.args[ 3 ]
      else:
        tests = None

      res = self.APIs[ 'SAMResultsClient' ].getStatus(granularity, name, siteName, tests, 
                                    timeout = self.timeout) 
  
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }
  
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  