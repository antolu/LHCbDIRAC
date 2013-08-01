""" A module defining the state machine for the Productions
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import State, StateMachine

class LHCbStateMachine( StateMachine ):
  """Just redefining one method
  """

  def setState( self, candidateState ):
    """ Makes sure the state is either None or known to the machine, and that it is a valid state to move into
        This is a re-definition of original one that wasn't making these checks
    """

    if candidateState == self.state:
      return S_OK()

    if candidateState is None:
      self.state = candidateState
    elif candidateState in self.states.keys():
      if not self.states[self.state].stateMap:
        return S_ERROR( 'Final state' )
      nextState = self.getNextState( candidateState )
      if not nextState[ 'OK' ]:
        return nextState
      nextState = nextState[ 'Value' ]
      # If the StateMachine does not accept the candidate, return error message
      if candidateState != nextState:
        return S_ERROR( '%s is not a valid state' % candidateState )
      else:
        self.state = candidateState
    else:
      return S_ERROR( '%s is not a valid state' % candidateState )

    return S_OK()



class ProductionsStateMachine( LHCbStateMachine ):
  """ PMS implementation of the state machine
  """
  def __init__( self, state ):
    """ c'tor
        Defines the state machine transactions
    """

    super( ProductionsStateMachine, self ).__init__( state )

    # Current states
    self.states = {
                   'Cleaned'              : State( 15 ),  # final state
                   'Cleaning'             : State( 14, ['Cleaned'] ),
                   'Completing'           : State( 13, ['Validating', 'Cleaning'], defState = 'Validating' ),
                   'Stopped'              : State( 12, ['Active', 'Flush', 'Cleaning'], defState = 'Active' ),
                   'TransformationCleaned': State( 16, ['Archived', 'Cleaned'], defState = 'Archived' ),
                   'Archived'             : State( 11 ),  # final state
                   'Completed'            : State( 10, ['Archived'] ),
                   'WaitingIntegrity'     : State( 9, ['ValidatedOutput'] ),
                   'ValidatedOutput'      : State( 7, ['Active', 'Completed', 'Cleaning'], defState = 'Completed' ),
                   'ValidatingOutput'     : State( 7, ['Active', 'ValidatedOutput',
                                                       'WaitingIntegrity'], defState = 'ValidatedOutput' ),
                   'RemovedFiles'         : State( 6, ['Completed'] ),
                   'RemovingFiles'        : State( 5, ['RemovedFiles'] ),
                   'ValidatingInput'      : State( 4, ['Active', 'RemovingFiles', 'Cleaning'], defState = 'Active' ),
                   'Flush'                : State( 3, ['Active', 'Cleaning'], defState = 'Active' ),
                   'Idle'                 : State( 2, ['Active', 'ValidatingInput',
                                                       'ValidatingOutput'], defState = 'Active' ),
                   'Active'               : State( 1, ['Flush', 'Idle', 'Stopped', 'Completing', 'ValidatingInput',
                                                       'ValidatingOutput', 'Cleaning', 'TransformationCleane'],
                                                  defState = 'Flush' ),
                   'New'                  : State( 0, ['Active', 'Cleaning'], defState = 'Active' )  # initial state
                  }

    # NEW states proposal
#    self.states = {
#                   'Cleaned'    : State( 10 ), # final state
#                   'Cleaning'   : State( 9, ['Cleaned'] ),
#                   'Completing' : State( 8, ['Validating', 'Cleaning'], defState = 'Validating' ),
#                   'TransformationCleaned': State( 16, ['Archived', 'Cleaned'], defState = 'Archived' ),
#                   'Stopped'    : State( 7, ['Active', 'Flush', 'Cleaning'], defState = 'Active' ),
#                   'Archived'   : State( 6 ),  # final state
#                   'Completed'  : State( 5, ['Archived'], defState = 'Archived' ),
#                   'Validating' : State( 4, ['Active', 'Completed', 'Cleaning'], defState = 'Completed' ),
#                   'Idle'       : State( 3, ['Active', 'Validating', 'Cleaning'], defState = 'Active' ),
#                   'Flush'      : State( 2, ['Active', 'Cleaning'], defState = 'Active' ),
#                   'Active'     : State( 1, ['Flush', 'Idle', 'Stopped', 'Completing', 'Validating', 'Cleaning'], defState = 'Flush' ),
#                   'New'        : State( 0, ['Active', 'Cleaning'], defState = 'Active' )  # initial state
#                  }
