"""
LHCbDIRAC/ResourceStatusSystem/Agent/LHCbPRProxyAgent.py
"""

__RCSID__ = ""

# First, pythonic stuff
import os
import stat

# Second, DIRAC stuff
from DIRAC                                      import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                import AgentModule

# Third, LHCbDIRAC stuff
# ...

AGENT_NAME = 'ResourceStatus/LHCbPRProxyAgent'

class LHCbPRProxyAgent( AgentModule ):
  '''
  Class LHCbPRProxyAgent. This agent is in charge of getting a fresh proxy from
  DIRAC. In this case, it sets the proxy as LHCbPR.
  See CS/Operations/Shift/LHCbPR

  This is used by the LHCbPR framework, to get a new proxy every day,
  without having to use any hardcoded password ( which means, this agent will
  run on the same machine as LHCbPR ).

  The agent overwrites the parent methods:

  - initialize
  - execute
  '''

################################################################################

  def initialize( self ):
    '''
    Method executed when the agent is launched.
    It sets the proxy as LHCbPR, which is stored under
      /opt/dirac/pro/work/ResourceStatus/LHCbPRProxyAgent
    '''

    try:

      self.am_setOption( 'shifterProxy', 'LHCbPR' )
      return S_OK()

    except Exception:
      errorStr = "LHCbPRProxyAgent initialization"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def execute( self ):
    """
    At every execution this method will try to print the environment variable
    X509_USER_PROXY, which should contain the path to the proxy. If is empty,
    something went wrong.

    This agent should get a new proxy whenever it is close to expire.
    Appart from that, which is done internally, it is a pretty dummy loop.
    """

    try:
      diracCred = '/opt/dirac/pro/work/ResourceStatus/LHCbPRProxyAgent/.shifterCred'
      lhcbprCred = '/tmp/x509up_u45054'
      os.environ[ 'X509_USER_PROXY' ] = '/opt/dirac/pro/work/ResourceStatus/LHCbPRProxyAgent/.shifterCred'
      import shutil
      shutil.copyfile( diracCred, lhcbprCred )
      os.chmod( lhcbprCred, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)

      self.log.info( 'Loop' )
      self.log.info( os.environ[ 'X509_USER_PROXY' ] )
      return S_OK()

    except KeyError, x:
      self.log.exception( x )
      return S_ERROR( x )


# ...............................................................................
# EOF
