from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine         import State
from LHCbDIRAC.ProductionManagementSystem.Utilities.StateMachine  import LHCbStateMachine

class TransformationFilesStateMachine( LHCbStateMachine ):
  """ Implementation of the state machine for the TransformationFiles
  """
  def __init__( self, state ):
    """ c'tor
        Defines the state machine transactions
    """

    super( TransformationFilesStateMachine, self ).__init__( state )

    self.states = {
                   'Removed'      : State( 8 ),  # final state
                   'MissingInFC'  : State( 7 ),  # final state
                   'NotProcessed' : State( 6, ['Unused'] ),
                   'ProbInFC'     : State( 5, ),  # final state
                   'MaxReset'     : State( 4, ['Unused'] ),
                   'Problematic'  : State( 3 ),  # final state
                   'Processed'    : State( 2 ),  # final state
                   'Assigned'     : State( 1, ['Processed', 'MaxReset'], defState = 'Processed' ),
                   'Unused'       : State( 0, ['Assigned', 'MissingInFC', 'ProbInFC'], defState = 'Assigned' )
                   }
