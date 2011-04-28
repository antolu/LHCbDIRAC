########################################################################
# $HeadURL:
########################################################################

""" The OnSENodePropagation_Policy module is a policy module used to update the status of
    the resource SE, based on statistics of its StorageElements
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

class OnSENodePropagation_Policy(PolicyBase):

  def evaluate(self, args, commandIn = None, knownInfo = None):
    """ Evaluate policy on SE nodes status, using args (tuple).

        :params:
          :attr:`args`: a tuple
            `args[0]` should be a ValidRes (StorageElement only)

            `args[1]` should be the name of the SE node

          :attr:`commandIn`: optional command object

          :attr:`knownInfo`: optional information dictionary

        :returns:
            {
              `Status`:Active|Probing|Banned,
              `Reason`:'SE: A:%d/P:%d/B:%d'
            }
    """

    if type(args) != tuple:
      raise TypeError, where( self, self.evaluate )

    #get resource stats
    if knownInfo is not None and 'StorageElementStats' in knownInfo.keys():
      storageElementStats = knownInfo['StorageElementStats']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Command.RS_Command import StorageElementsStats_Command
        command = StorageElementsStats_Command()

      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand( command )
      storageElementStats = clientsInvoker.doCommand( ( 'Resource', args[1] ) )


    storageElementStats = storageElementStats['StorageElementStats']

    storageElementStatus = 'Active'

    if storageElementStats['Total'] != 0:
      if storageElementStats['Active'] == 0:
        if storageElementStats['Probing'] == 0:
          storageElementStatus = 'Banned'
        else:
          storageElementStatus = 'Probing'

    result = {}

    if storageElementStatus == 'Active':
      result['Status'] = 'Active'
      result['Reason'] = 'SEs: A:%d/P:%d/B:%d' % ( storageElementStats['Active'],
                                                   storageElementStats['Probing'],
                                                   storageElementStats['Banned'] )
    elif storageElementStatus == 'Probing':
      result['Status'] = 'Probing'
      result['Reason'] = 'SEs: A:%d/P:%d/B:%d' % ( storageElementStats['Active'],
                                                   storageElementStats['Probing'],
                                                   storageElementStats['Banned'] )
    elif storageElementStatus == 'Banned':
      result['Status'] = 'Banned'
      result['Reason'] = 'SEs: A:%d/P:%d/B:%d' % ( storageElementStats['Active'],
                                                   storageElementStats['Probing'],
                                                   storageElementStats['Banned'] )
    return result
