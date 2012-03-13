# $HeadURL$
''' DownHillPropagationPolicy

  The DownHillPropagationPolicy module is a policy module used to update the 
  status of an element, based on how its element in the upper part of the 
  hierarchy is behaving in the RSS.
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id$'

class DownHillPropagationPolicy( PolicyBase ):

  def evaluate(self):
    """
    Evaluate policy on "upper" element Status, using args (tuple).
    The status is propagated only when one of the two status is 'Banned'

    :returns:
        {
          `Status`:Error|Unknown|Active|Probing|Banned,
          `Reason`:'Node/Site status: Active|Probing|Banned'
        }
    """

    resourceStatus = super( DownHillPropagationPolicy, self ).evaluate()
    result = {}

    if resourceStatus is None:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = 'Command evaluation returned None'
      return result

    if not resourceStatus[ 'OK' ]:
      result[ 'Status' ] = 'Error'
      result[ 'Reason' ] = resourceStatus[ 'Message' ]
      return result

    resourceStatus = resourceStatus[ 'Value' ]
    
    if resourceStatus is None:
      result[ 'Status' ] = 'Unknown'
      result[ 'Reason' ] = 'No values to take a decission'
      return result

    result[ 'Status' ] = resourceStatus
    result[ 'Reason' ] = 'DownHill propagated status: %s' % resourceStatus
    return result

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF