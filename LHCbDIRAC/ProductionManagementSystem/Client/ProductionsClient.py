""" An extension of the LHCb TransformationClient to handle some specificities of productions
"""

from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from LHCbDIRAC.ProductionManagementSystem.Utilities.StateMachine  import ProductionsStateMachine

class ProductionsClient( TransformationClient ):
  """ An extension of the LHCb TransformationClient to handle some specificities of productions
  """

  def setTransformationParameter( self, transID, paramName, paramValue ):
    """ just invokes the state machine check when needed
    """
    if paramName.lower() == 'status':
      return self.setStatus( transID, paramValue )
    else:
      return TransformationClient.setTransformationParameter( self, transID, paramName, paramValue )

  def setStatus( self, transID, status, originalStatus = '' ):
    """ performs a state machine check for productions when asked to change the status
    """
    if not originalStatus:
      originalStatus = TransformationClient.getTransformationParameters( transID, 'Status' )
      if not originalStatus['OK']:
        return originalStatus
      originalStatus = originalStatus['Value']

    psm = ProductionsStateMachine( originalStatus )
    return psm.setState( status )
