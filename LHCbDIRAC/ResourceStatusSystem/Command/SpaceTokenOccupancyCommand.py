# $HeadURL: $
''' SpaceTokenOccupancyCommand
  
  The Command gets information of the SpaceTokenOccupancy from the lcg_utils
  
'''

import lcg_util

from DIRAC                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id: $'

class SpaceTokenOccupancyCommand( Command ):
  '''
  Uses lcg_util to query status of endpoint for a given token.
  ''' 

  def doCommand( self ):
    '''
    Run the command.
    '''
    super( SpaceTokenOccupancyCommand, self ).doCommand()

    #granularity = self.args[0]
    #name    = self.args[ 1 ]
    spaceTokenEndpoint = self.args[ 2 ]
    spaceToken         = self.args[ 3 ]  
    
    occupancy = lcg_util.lcg_stmd( spaceToken, spaceTokenEndpoint, True, 0 )
           
    if occupancy[ 0 ] == 0:
    
      output = answer[1][0]
      total  = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
      free   = float( output[ 'unusedsize' ] ) / 1e12
      
      res = S_OK( { 'total' : total, 'free' : free } )
          
    else:  

      res = S_ERROR( occupancy )

    return { 'Result' : res }
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF