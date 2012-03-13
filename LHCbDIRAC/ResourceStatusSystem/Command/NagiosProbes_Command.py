# $HeadURL: $
''' NagiosProbes_Command
  
  The Command gets information from the MonitoringTest cache.
  
'''

from DIRAC                                            import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command       import *

from LHCbDIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs

__RCSID__ = '$Id: $'

class NagiosProbes_Command( Command ):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand( self ):
    '''
    
    '''
    super( NagiosProbes_Command, self ).doCommand()
    self._APIs = initAPIs( self.__APIs__, self._APIs, force = True )  
    
#    try:
   
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
    
    res = self._APIs[ 'ResourceManagementClient'].getMonitoringTest( **query )
      
    if not res['OK']:
    #  msg = "Error getting NagiosProbes for serviceURI '%s'\n %s" % ( name, res['Message'])
    #  gLogger.error( msg )
      return { 'Result' : res }
    
    res = S_OK( dict( [ ( r[0], r[1:] ) for r in res[ 'Value' ] ] ) )

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }    

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  