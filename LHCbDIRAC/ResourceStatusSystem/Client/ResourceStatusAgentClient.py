"""
ResourceStatusAgentClient class is a client for requesting info from the ResourceStatusAgentService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

class ResourceStatusAgentClient:

#############################################################################

  def __init__(self, serviceIn = None, timeout = None):
    """ Constructor of the ResourceStatusAgentClient class
    """
    if serviceIn == None:
      self.rsa = RPCClient("ResourceStatus/ResourceStatusAgent", timeout = timeout)
    else:
      self.rsa = serviceIn

#############################################################################

  def getTestList( self ):
    """
    """

    res = self.rsa.getTestList()
    if not res['OK']:
      raise RSSException, where(self, self.getTestList) + " " + res['Message']

    return res['Value']

#############################################################################

  def getTestListBySite( self, siteName, last ):
    """
    """

    res = self.rsa.getTestListBySite( siteName, last )
    if not res['OK']:
      raise RSSException, where(self, self.getTestListBySite) + " " + res['Message']

    return res['Value']

#############################################################################

  def getLastTest( self, siteName, reason ):
    """
    """

    res = self.rsa.getLastTest( siteName, reason )
    if not res['OK']:
      raise RSSException, where(self, self.getLastTest) + " " + res['Message']

    return res['Value']

#############################################################################

  def getTest( self, testID ):
    """
    """

    res = self.rsa.getTest( testID )
    if not res['OK']:
      raise RSSException, where(self, self.getTest) + " " + res['Message']

    return res['Value']

#############################################################################
