#''' HCCommand
#
#  Command to interact with the XMLRPC server of HammerCloud.
#
#'''
#
#from DIRAC                                      import gLogger, S_OK, S_ERROR
#from DIRAC.ResourceStatusSystem.Command.Command import Command
#
#from LHCbDIRAC.ResourceStatusSystem.Client.HCClient                 import HCClient
#from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#
#__RCSID__ = '$Id: HCCommand.py 58924 2012-11-27 10:37:30Z ubeda $'
#
#class HCCommand( Command ):
#  
#  def doCommand( self, site = None ):
#    """ 
#      Gets from HCAgent table the last test for that site.
#      Depending on the status and/or 'age', decide what to
#      do. 
#    """
#    super( HCCommand, self ).doCommand()
#
#    result = []
#
#    '''
#      This is a nice example of spaghetti code.
#      Think how to configure the agent. Maybe in the CS. 
#    '''
#    
#    rm  = ResourceManagementClient()       
#    res = rm.getLastHCTest( self.args[1], 'HClastfinished' )
#    
#    if res:
#      
#      res = res[-1]
#           
#      hc = HCClient()
#                    
#      sum = hc.getSummarizedResults( res[0], detailed = 1 )
#      if sum[0]:
#        gLogger.debug( 'HC_command %s ' % str(sum) )
#        result = S_OK( sum[1]['response']['summary'][0] ) 
#      else:
#        gLogger.info( 'Something went wrong' )
#        result = S_ERROR( sum ) 
#
#    return { 'Result' : result }
#  
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF