# $HeadURL$
''' TransferQualityPolicy
 
  The TransferQualityPolicy class is a policy class to check the transfer
  quality.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

from LHCbDIRAC.ResourceStatusSystem.Policy import Configurations

__RCSID__ = '$Id$'

class TransferQualityPolicy( PolicyBase ):

  def evaluate( self ):
    """
    Evaluate policy on Data quality.

    :returns:
        {
          'Status':Error|Unknown|Active|Probing|Banned,
          'Reason':'TransferQuality:None'|'TransferQuality:xx%',
        }
    """

    quality = super( TransferQualityPolicy, self ).evaluate()
    result  = {}

    if quality is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result
    
    if not quality[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = quality[ 'Message' ]
      return result
    
    quality = quality[ 'Value' ]
    if quality == None:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decision'
      return result 
    
    quality = int( round( quality ) )
    result[ 'Reason' ] = 'TransferQuality: %d %% -> ' % quality

    if 'FAILOVER'.lower() in self.args[ 1 ].lower():

      if quality < Configurations.pp[ 'Transfer_QUALITY_LOW' ]:
        result[ 'Status' ] = 'Probing'
        strReason = 'Low'
      elif quality < Configurations.pp[ 'Transfer_QUALITY_HIGH' ]:
        result[ 'Status' ] = 'Active'
        strReason = 'Mean'
      else:
      #elif quality >= Configurations.Transfer_QUALITY_HIGH :
        result[ 'Status' ] = 'Active'
        strReason = 'High'
      #else:
      #  result[ 'Status' ] = 'Active'
      #  strReason = 'Mean'

      #result[ 'Reason' ] = result[ 'Reason' ] + strReason

    else:

      if quality < Configurations.pp[ 'Transfer_QUALITY_LOW' ] :
        result[ 'Status' ] = 'Bad'
        strReason          = 'Low'
      elif quality < Configurations.pp[ 'Transfer_QUALITY_HIGH' ]:
        result[ 'Status' ] = 'Probing'
        strReason          = 'Mean'
      else:
#      elif quality >= Configurations.Transfer_QUALITY_HIGH :
        result[ 'Status' ] = 'Active'
        strReason          = 'High'
#      elif quality >= Configurations.Transfer_QUALITY_LOW and quality < Configurations.Transfer_QUALITY_HIGH:
#        result[ 'Status' ] = 'Probing'
#        strReason = 'Mean'

    result[ 'Reason' ] = result[ 'Reason' ] + strReason
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
