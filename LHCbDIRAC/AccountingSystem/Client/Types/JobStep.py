''' JobStep Type
  
'''

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

__RCSID__ = "$Id: JobStep.py 69325 2013-08-07 14:29:32Z ubeda $"

class JobStep( BaseAccountingType ):
  '''
    JobStep as extension of BaseAccountingType
  '''

  def __init__( self ):
    
    BaseAccountingType.__init__( self )

    self.definitionKeyFields = [ ( 'JobGroup',       'VARCHAR(32)' ),
                                 ( 'RunNumber',      'VARCHAR(32)' ),
                                 ( 'EventType',      'VARCHAR(32)' ),
                                 ( 'ProcessingType', 'VARCHAR(32)' ),
                                 ( 'ProcessingStep', 'VARCHAR(32)' ),
                                 ( 'Site',           'VARCHAR(32)' ),
                                 ( 'FinalStepState', 'VARCHAR(32)' )
                                ]

    self.definitionAccountingFields = [ ( 'CPUTime',      "INT UNSIGNED" ),
                                        ( 'NormCPUTime',  "INT UNSIGNED" ),
                                        ( 'ExecTime',     "INT UNSIGNED" ),
                                        ( 'InputData',    'BIGINT UNSIGNED' ),
                                        ( 'OutputData',   'BIGINT UNSIGNED' ),
                                        ( 'InputEvents',  'BIGINT UNSIGNED' ),
                                        ( 'OutputEvents', 'BIGINT UNSIGNED' )
                                      ]

    self.bucketsLength = [ ( 86400 * 7,      3600 ), #<1w = 1h
                           ( 86400 * 35,     3600 * 4 ), #<35d = 4h
                           ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 86400 * 365,    86400 * 2 ), #<1y = 2d
                           ( 86400 * 600,    604800 ), #>1y = 1w
                         ]

    self.checkType()
    
#...............................................................................
#EOF
