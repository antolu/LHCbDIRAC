########################################################################
# $Id: InputDataResolution.py,v 1.1 2008/01/31 14:49:48 paterson Exp $
# File :   InputDataResolution.py
# Author : Stuart Paterson
########################################################################

""" The input data resolution module is a VO-specific plugin that
    allows to define VO input data policy in a simple way using existing
    utilities in DIRAC or extension code supplied by the VO.

    The arguments dictionary from the Job Wrapper includes the file catalogue
    result and in principle has all necessary information to resolve input data
    for applications.

"""

__RCSID__ = "$Id: InputDataResolution.py,v 1.1 2008/01/31 14:49:48 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol      import InputDataByProtocol
from DIRAC.WorkloadManagementSystem.Client.PoolXMLSlice             import PoolXMLSlice
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re

COMPONENT_NAME='LHCbInputDataResolution'

class InputDataResolution:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)

  #############################################################################
  def execute(self):
    """Given the arguments from the Job Wrapper, this function calls existing
       utilities in DIRAC to resolve input data according to LHCb VO policy.
    """
    self.log.info('Attempting to resolve input data requirement by available site protocols')
    protocolAccess = InputDataByProtocol(self.arguments)
    result = protocolAccess.execute()
    if not result['OK']:
      return result

    #For LHCb, as long as one TURL exists, this can be conveyed to the application
    failedReplicas = result['Failed']
    if result['Failed']:
      self.log.info('InputDataByProtocol failed to obtain a TURL for the following files:\n%s' %(string.join(failedReplicas,'\n')))

    if not result['Successful']:
      return S_ERROR('InputDataByProtocol returned no TURLs for requested input data')

    appCatalog = PoolXMLSlice('pool_xml_catalog.xml')
    check = appCatalog.execute(result['Successful'])
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
