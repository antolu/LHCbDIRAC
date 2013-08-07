''' UserStorage Type
  
'''

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

__RCSID__ = "$Id$"

class UserStorage( BaseAccountingType ):
  '''
    UserStorage as extension of BaseAccountingType
  '''

  def __init__( self ):

    BaseAccountingType.__init__( self )
    
    self.definitionKeyFields = [ ( 'StorageElement' , "VARCHAR(32)" ),
                                 ( 'User',            "VARCHAR(32)" )
                               ]
    
    self.definitionAccountingFields = [ ( 'LogicalSize',   'BIGINT UNSIGNED' ),
                                        ( 'LogicalFiles',  'BIGINT UNSIGNED' ),
                                        ( 'PhysicalSize',  'BIGINT UNSIGNED' ),
                                        ( 'PhysicalFiles', 'BIGINT UNSIGNED' ),
                                        ( 'StorageSize',   'BIGINT UNSIGNED' ),
                                        ( 'StorageFiles',  'BIGINT UNSIGNED' )
                                      ]
    self.bucketsLength = [ ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 31104000,       604800 ), #>6m = 1w
                         ]
    self.checkType()

#...............................................................................
#EOF
