""" A module defining the state machine for the Productions
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.PolicySystem.StateMachine import State, StateMachine

class ProductionsStateMachine( StateMachine ):
  """ PMS implementation of the state machine
  """
  def __init__( self, state ):
    """ c'tor
        Defines the state machine transactions
    """

    super( ProductionsStateMachine, self ).__init__( state )

    self.states = {
                   'Deleted'    : State( 10 ),  # final state
                   'Cleaning'   : State( 9, ['Deleted'] ),
                   'Completing' : State( 8, ['Validating', 'Cleaning'], defState = 'Validating' ),
                   'Stopped'    : State( 7, ['Active', 'Cleaning'], defState = 'Active' ),
                   'Archived'   : State( 6 ),  # final state
                   'Completed'  : State( 5, ['Archived'], defState = 'Archived' ),
                   'Validating' : State( 4, ['Active', 'Completed', 'Cleaning'], defState = 'Completed' ),
                   'Idle'       : State( 3, ['Active', 'Validating', 'Cleaning'], defState = 'Active' ),
                   'Flush'      : State( 2, ['Active', 'Cleaning'], defState = 'Active' ),
                   'Active'     : State( 1, ['Flush', 'Idle', 'Stopped', 'Completing', 'Validating', 'Cleaning'], defState = 'Flush' ),
                   'New'        : State( 0, ['Active', 'Cleaning'], defState = 'Active' )  # initial state
                  }

  def setState( self, candidateState ):
    """ Makes sure the state is either None or known to the machine, and that it is a valid state to move into
        This is a re-definition of original one that wasn't making these checks
    """

    if candidateState == self.state:
      return S_OK()

    if candidateState is None:
      self.state = candidateState
    elif candidateState in self.states.keys():
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
