# $HeadURL$
''' DataStorage Type
  
'''

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

__RCSID__ = "$Id$"

class DataStorage( BaseAccountingType ):
  '''
    DataStorage as extension of BaseAccountingType
  '''

  def __init__( self ):
    
    BaseAccountingType.__init__( self )
    
    self.definitionKeyFields = [ ( 'DataType',       "VARCHAR(64)" ),
                                 ( 'Activity',       "VARCHAR(64)" ),
                                 ( 'FileType',       "VARCHAR(32)" ),
                                 ( 'Production',     "VARCHAR(32)" ),
                                 ( 'ProcessingPass', "VARCHAR(64)" ),
                                 ( 'Conditions',     "VARCHAR(64)" ),
                                 ( 'EventType',      "VARCHAR(64)" ),
                                 ( 'StorageElement', "VARCHAR(32)" )
                               ]
    
    self.definitionAccountingFields = [ ( 'LogicalSize',   'BIGINT UNSIGNED' ),
                                        ( 'LogicalFiles',  'BIGINT UNSIGNED' ),
                                        ( 'PhysicalSize',  'BIGINT UNSIGNED' ),
                                        ( 'PhysicalFiles', 'BIGINT UNSIGNED' )
                                      ]
    
    self.bucketsLength = [ ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 31104000,       604800 ), #>6m = 1w
                         ]
    self.checkType()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF