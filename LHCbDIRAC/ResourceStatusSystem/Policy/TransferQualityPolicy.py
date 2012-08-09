# $HeadURL$
''' TransferQualityPolicy
 
  The TransferQualityPolicy class is a policy class to check the transfer
  quality.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

from LHCbDIRAC.ResourceStatusSystem.Policy import Configurations

__RCSID__ = '$Id$'

class TransferQualityPolicy( PolicyBase ):
  '''
  Evaluates the TransferQuality results given by the DIRACAccounting.TransferQuality
  command against a certain set of thresholds defined in the CS.
  '''


  def evaluate( self ):
    '''
    Evaluate policy on Data quality.

    :returns:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'TransferQuality:None'|'TransferQuality:xx%',
        }
    '''

    commandResult = super( TransferQualityPolicy, self ).evaluate()
    result  = {}

    if commandResult is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result
    
    if not commandResult[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = commandResult[ 'Message' ]
      return result
    
    commandResult = commandResult[ 'Value' ]
    if commandResult == None:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return result 
    
    commandResult = int( round( commandResult ) )
    result[ 'Reason' ] = 'TransferQuality: %d %% -> ' % commandResult

    policyParameters = Configurations.getPolicyParameters()

    if 'FAILOVER'.lower() in self.args[ 1 ].lower():

      if commandResult < policyParameters[ 'Transfer_QUALITY_LOW' ]:
        result[ 'Status' ] = 'Probing'
        strReason = 'Low'
      elif commandResult < policyParameters[ 'Transfer_QUALITY_HIGH' ]:
        result[ 'Status' ] = 'Active'
        strReason = 'Mean'
      else:
        result[ 'Status' ] = 'Active'
        strReason = 'High'

    else:

      if commandResult < policyParameters[ 'Transfer_QUALITY_LOW' ] :
        result[ 'Status' ] = 'Bad'
        strReason          = 'Low'
      elif commandResult < policyParameters[ 'Transfer_QUALITY_HIGH' ]:
        result[ 'Status' ] = 'Probing'
        strReason          = 'Mean'
      else:
        result[ 'Status' ] = 'Active'
        strReason          = 'High'

    result[ 'Reason' ] = result[ 'Reason' ] + strReason
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
