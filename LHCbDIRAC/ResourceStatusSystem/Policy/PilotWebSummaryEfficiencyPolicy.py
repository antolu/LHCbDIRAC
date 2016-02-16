''' LHCbDIRAC.ResourceStatusSystem.Policy.PilotEfficiencyPolicy
  
   PilotWebSummaryEfficiencyPolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase
  
'''

#from DIRAC                                              import S_OK
#from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase
#
#__RCSID__ = '$Id: PilotWebSummaryEfficiencyPolicy.py 66379 2013-05-28 07:55:57Z ubeda $'
#
#class PilotWebSummaryEfficiencyPolicy( PolicyBase ):
#  '''
#    The PilotEfficiencyPolicy class is a policy that checks the efficiency of the 
#    jobs according to what is on WMS.
#
#    Evaluates the PilotEfficiency results given by the PilotCommand.PilotCommand
#  '''
#
#  @staticmethod
#  def _evaluate( commandResult ):
#    """
#    Evaluate policy on pilots stats, using args (tuple).
#
#    returns:
#        {
#          'Status': Active|Probing|Bad,
#          'Reason':'PilotsEff:low|PilotsEff:med|PilotsEff:good',
#        }
#    """
#
#    result = { 
#              'Status' : None,
#              'Reason' : None
#              }
#
#    if not commandResult[ 'OK' ]:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = commandResult[ 'Message' ]
#      return S_OK( result )
#
#    commandResult = commandResult[ 'Value' ]
#
#    if not commandResult:
#      result[ 'Status' ] = 'Unknown'
#      result[ 'Reason' ] = 'No values to take a decision'
#      return S_OK( result )
#
#    # The command returns a list of dictionaries, with only one if thre is something,
#    # otherwise an empty list.
#    commandResult = commandResult[ 0 ]
#      
#    if 'Status' not in commandResult:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = '"Status" key missing'
#      return S_OK( result )  
#    
#    if 'PilotJobEff' not in commandResult:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = '"PilotJobEff" key missing'
#      return S_OK( result )  
#
#    status      = commandResult[ 'Status' ]
#    pilotJobEff = commandResult[ 'PilotJobEff' ]
#
#    if status == 'Good':
#      result[ 'Status' ] = 'Active'
#    elif status == 'Fair':
#      result[ 'Status' ] = 'Active'
#    elif status == 'Poor':
#      result[ 'Status' ] = 'Degraded'
#    elif status == 'Idle':
#      result[ 'Status' ] = 'Unknown'
#    elif status == 'Bad':
#      result[ 'Status' ] = 'Banned'
#    else:
#      result[ 'Status' ] = 'Error'
#      result[ 'Reason' ] = 'Unknown status "%s"' % status
#      return S_OK( result )  
#
#    result[ 'Reason' ] = 'Pilots Efficiency: %s with status %s' % ( pilotJobEff, status )
#      
#    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF