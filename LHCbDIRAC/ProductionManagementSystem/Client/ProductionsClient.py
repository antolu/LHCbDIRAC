""" An extension of the LHCb TransformationClient to handle some specificities of productions
"""

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

class ProductionsClient(TransformationClient):
  """ An extension of the LHCb TransformationClient to handle some specificities of productions
  """
  
  def setTransformationParameter(self, transName, paramName, paramValue):
    """ just invokes the state machine check when needed
    """
    if paramName.lower() == 'status':
      self.setStatus( transName, paramValue )
    else:
      self.setTransformationParameter( transName, paramName, paramValue )

  def setStatus( self, transName, status ):
    """ performs a state machine check for productions when asked to change the status
    """
