################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The NagiosProbes_Command gets information from the MonitoringTest cache.
"""

from DIRAC                                            import gLogger

from DIRAC.ResourceStatusSystem.Command.Command       import *
from LHCbDIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs

################################################################################
################################################################################

class NagiosProbes_Command(Command):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand( self ):
    '''
    
    '''
    super( NagiosProbes_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs, force = True )  
    
    #granularity = self.args[0]
    name    = self.args[1]
    flavour = self.args[2]
    
    #ServiceURI is a quite misleading name.. it is a Resource in the RSS DB in fact.
    query = { 
              'serviceURI'     : name, 
              'serviceFlavour' : flavour,
              'meta'           :
                {
                  'columns' : ['MetricStatus','SummaryData'],
                  'count'   : True,
                  'group'   : 'MetricStatus' 
                 }
            }
    res = self.APIs[ 'ResourceManagementClient'].getMonitoringTest( **query )
    if not res['OK']:
      msg = "Error getting NagiosProbes for serviceURI '%s'\n %s" % ( name, res['Message'])
      gLogger.error( msg )
      raise RSSException, msg
    
    return { 'Result' : dict( [ ( r[0], r[1:] ) for r in res[ 'Value' ] ] ) }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  