# $HeadURL$
__RCSID__ = "$Id:  $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class Storage( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'StorageElement' , "VARCHAR(32)" ),
                                 ( 'Directory', "VARCHAR(32)" )
                               ]
    self.definitionAccountingFields = [ ( 'LogicalSize', 'BIGINT UNSIGNED' ),
                                        ( 'LogicalFiles', 'BIGINT UNSIGNED' ),
                                        ( 'PhysicalSize', 'BIGINT UNSIGNED' ),
                                        ( 'PhysicalFiles', 'BIGINT UNSIGNED' )
                                      ]
    self.bucketsLength = [ ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 31104000, 604800 ), #>6m = 1w
                         ]
    self.checkType()
