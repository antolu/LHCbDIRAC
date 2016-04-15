""" :mod: ValidateRequest
    ==================

    .. module: ValidateRequest
    :synopsis: validateRequest operation handler
"""

__RCSID__ = "$Id $"


# # imports
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

########################################################################
class WMSSecureOutputData( OperationHandlerBase ):
  """
  .. class:: ValidateRequest

  Validate operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    OperationHandlerBase.__init__( self, operation, csPath )


  def __call__( self ):
    """ It expects to find the reqID in operation.Arguments
    """
    try:
      decode = DEncode.decode( self.operation.Arguments )
      self.log.debug (decode)
      gLogger.debug ( "Validating output" )
    except ValueError as error:
      self.log.exception( error )
      self.operation.Error = str( error )
      self.operation.Status = "Failed"
      return S_ERROR( str( error ) )

    self.operation.Status = "Done"
    return S_OK()

