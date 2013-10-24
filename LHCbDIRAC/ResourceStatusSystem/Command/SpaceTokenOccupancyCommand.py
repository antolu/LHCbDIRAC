# $HeadURL:  $
''' SpaceTokenOccupancyCommand
  
  LHCbDIRAC extension adding records to the accounting
  
'''

from DIRAC                                                         import S_OK, gLogger
from DIRAC.AccountingSystem.Client.DataStoreClient                 import gDataStoreClient
from DIRAC.Core.Utilities.SiteSEMapping                            import getSitesForSE
from DIRAC.ResourceStatusSystem.Command.SpaceTokenOccupancyCommand import SpaceTokenOccupancyCommand as STOC
from DIRAC.ResourceStatusSystem.Utilities                          import CSHelpers

from LHCbDIRAC.AccountingSystem.Client.Types.SpaceToken            import SpaceToken

__RCSID__ = '$Id:  $'

class SpaceTokenOccupancyCommand( STOC ):
  '''
  Uses lcg_util to query status of endpoint for a given token.
  ''' 


  def getSiteNameFromEndpoint( self, endpoint ):
    
    endpointSE = ''
    
    ses = CSHelpers.getStorageElements()
    if not ses[ 'OK' ]:
      gLogger.error( ses[ 'Message' ] )
    
    for se in ses[ 'Value' ]:
      # Ugly, ugly, ugly.. waiting for DIRAC v7r0 to do it properly
      if not ( '-' in se ) or ( '_' in se ):
        continue
      
      res = CSHelpers.getStorageElementEndpoint( se )
      if not res[ 'OK' ]:
        continue
      
      if endpoint == res[ 'Value' ]:
        endpointSE = se
        break
    
    if not endpointSE:
      return ''
    
    site = getSitesForSE( endpointSE, 'LCG' )
    try:
      return site[ 'Value' ][ 0 ]
    except:
      return ''

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
      
        gDataStoreClient.addRegister( spaceTokenAccounting )
    gDataStoreClient.commit()
    
    return S_OK()  

      
#...............................................................................
#EOF
