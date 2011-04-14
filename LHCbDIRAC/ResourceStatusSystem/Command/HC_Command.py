from DIRAC.ResourceStatusSystem.Command.Command import *

from LHCbDIRAC.ResourceStatusSystem.Client.HCClient import HCClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceStatusAgentClient import ResourceStatusAgentClient

from DIRAC import gLogger

from datetime import datetime,timedelta

class HC_Command(Command):
  
  def doCommand(self, site = None):
    """ 
      Gets from HCAgent table the last test for that site.
      Depending on the status and/or 'age', decide what to
      do. 
    """
    super(HC_Command, self).doCommand()

    result = None

    '''
      This is a nice example of spaghetti code.
      Think how to configure the agent. Maybe in the CS. 
    '''
    
    TEST_DURATION = 1.5

    rsa = ResourceStatusAgentClient()    
    
    res = rsa.getLastTest( self.args[1], 'HClastfinished' )
    
    if res:
      
      res = res[-1]

      counter     = res[7]
      counterTime = res[8]
      now         = datetime.now()
    
      influence   = 2 * counter * TEST_DURATION
      
      endInfluence = counterTime + timedelta( hours = influence )
        
      if ( counterTime < now ) and ( now < endInfluence ):
           
        hc = HCClient()
                    
        sum = hc.getSummarizedResults( res[0], detailed = 1 )
        if sum[0]:
          gLogger.info( 'HC_command %s ' % str(sum) )
          result = sum[1]['response']['summary'][0] 
        else:
          gLogger.info( 'Something went wrong' )
          result = []
        
        return {'Result':result}
      
    res = rsa.getTestListBySite( self.args[1], True )
    
    if res:
      res = res[-1]
      if ('HCongoing' in res[3]) or ('HCscheduled' in res[3]):
        gLogger.info('HCCommand %s' % res[3])
        result = []   

    return {'Result':result}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
