''' SpaceToken Type
  
'''

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

__RCSID__ = "$Id: $"

class SpaceToken( BaseAccountingType ):
  '''
    SpaceToken as extension of BaseAccountingType
  '''

  def __init__( self ):
    
    BaseAccountingType.__init__( self )
    
    self.definitionKeyFields = [ 
                                 ( 'Site'      , 'VARCHAR(64)' ),
                                 ( 'Endpoint'  , 'VARCHAR(256)' ),
                                 ( 'SpaceToken', 'VARCHAR(64)' ),
                                 ( 'SpaceType' , 'VARCHAR(64)')
                               ]
    
    self.definitionAccountingFields = [ ( 'Space', 'BIGINT UNSIGNED' )
                                      ]

    self.bucketsLength = [ ( 86400 * 2     , 3600   ), #<2d = 1h
                           ( 86400 * 10    , 9000   ), #<10d = 2.5h
                           ( 86400 * 35    , 18000  ), #<35d = 5h
                           ( 86400 * 30 * 6, 86400  ), #>5d <6m = 1d
                           ( 86400 * 600   , 604800 ), #>6m = 1w
                         ]
    
    self.checkType()

#...............................................................................
#EOF
