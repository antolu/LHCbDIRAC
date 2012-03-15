# $HeadURL: $
''' NagiosProbesCommand
  
  The Command gets information from the MonitoringTest cache.
  
'''

from DIRAC                                            import S_OK
from DIRAC.ResourceStatusSystem.Command               import Command

from LHCbDIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs

__RCSID__ = '$Id: $'

class NagiosProbesCommand( Command ):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand( self ):
    '''
    
    '''
    super( NagiosProbesCommand, self ).doCommand()
    apis = initAPIs( self.__APIs__, self._APIs, force = True )  
    
    #granularity = self.args[0]
    name    = self.args[ 1 ]
    flavour = self.args[ 2 ]
    
    #ServiceURI is a quite misleading name.. it is a Resource in the RSS DB in fact.
    query = { 
            'serviceURI'     : name, 
            'serviceFlavour' : flavour,
            'meta'           :
              {
                'columns' : [ 'MetricStatus','SummaryData' ],
                'count'   : True,
                'group'   : 'MetricStatus' 
               }
          }
    
    res = apis[ 'ResourceManagementClient' ].getMonitoringTest( **query )
      
    if not res[ 'OK' ]:
      return { 'Result' : res }
    
    res = S_OK( dict( [ ( r[ 0 ], r[ 1: ] ) for r in res[ 'Value' ] ] ) )

    return { 'Result' : res }    

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  