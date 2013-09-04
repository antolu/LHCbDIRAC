# $HeadURL:  $
''' SpaceTokenOccupancyCommand
  
  LHCbDIRAC extension adding records to the accounting
  
'''

from DIRAC                                                         import S_OK
from DIRAC.AccountingSystem.Client.DataStoreClient                 import gDataStoreClient
from DIRAC.ResourceStatusSystem.Command.SpaceTokenOccupancyCommand import SpaceTokenOccupancyCommand as STOC

from LHCbDIRAC.AccountingSystem.Client.Types.SpaceToken            import SpaceToken

__RCSID__ = '$Id:  $'

class SpaceTokenOccupancyCommand( STOC ):
  '''
  Uses lcg_util to query status of endpoint for a given token.
  ''' 

  def _storeCommand( self, results ):
    '''
      Stores the results of doNew method on the database.
    '''
    
    for result in results:
      
      resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache( result[ 'Endpoint' ], 
                                                                    result[ 'Token' ], 
                                                                    result[ 'Total' ], 
                                                                    result[ 'Guaranteed' ], 
                                                                    result[ 'Free' ] )
      if not resQuery[ 'OK' ]:
        return resQuery
    
      accountingDict = {
                        'SpaceToken' : result [ 'Token' ],
                        'Endpoint'   : result[ 'Endpoint' ],
                        'Site'       : self.getSiteNameFromEndpoint( result[ 'Endpoint' ] )
                        }
      
      result[ 'Used' ] = result[ 'Total' ] - result[ 'Free' ]
    
      for sType in [ 'Total', 'Free', 'Used', 'Guaranteed' ]:
        spaceTokenAccounting = SpaceToken()
        spaceTokenAccounting.setNowAsStartAndEndTime()
        spaceTokenAccounting.setValuesFromDict( accountingDict )
        spaceTokenAccounting.setValueByKey( 'SpaceType', sType )
        spaceTokenAccounting.setValueByKey( 'Space', result[ sType ] * 1e12 )
      
        print gDataStoreClient.addRegister( spaceTokenAccounting )
    print gDataStoreClient.commit()
    
    return S_OK()  

      
#...............................................................................
#EOF
