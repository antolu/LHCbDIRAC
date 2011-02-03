# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class DataStorage( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'DataType' , "VARCHAR(32)" ),
                                 ( 'Activity', "VARCHAR(32)" ),
                                 ( 'FileType',  "VARCHAR(32)" ),
                                 ( 'Production', "INT UNSIGNED" ),
                                 ( 'ProcessingPass', "VARCHAR(32)" ),
                                 ( 'Conditions', "VARCHAR(32)" ),
                                 ( 'EventType', "VARCHAR(32)" )
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
