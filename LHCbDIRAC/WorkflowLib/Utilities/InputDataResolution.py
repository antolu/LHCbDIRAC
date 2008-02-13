########################################################################
# $Id: InputDataResolution.py,v 1.3 2008/02/13 08:57:16 paterson Exp $
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

__RCSID__ = "$Id: InputDataResolution.py,v 1.3 2008/02/13 08:57:16 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol      import InputDataByProtocol
from DIRAC.WorkloadManagementSystem.Client.PoolXMLSlice             import PoolXMLSlice
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re,string

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

    #!TODO: Must define file types in order to pass to POOL XML catalogue.  In the longer
    #term this will be derived from file catalog metadata information but for now is based
    #on the file extension types.
    resolvedData = result['Successful']
    tmpDict = {}
    for lfn,mdata in resolvedData.items():
      tmpDict[lfn]=mdata
      if re.search('.raw$',lfn):
        tmpDict[lfn]['pfntype']='MDF'
        self.log.verbose('Adding PFN file type %s for LFN:%s' %('MDF',lfn))
      else:
        tmpDict[lfn]['pfntype']='ROOT_All'
        self.log.verbose('Adding PFN file type %s for LFN:%s' %('ROOT_All',lfn))

    resolvedData = tmpDict

    #!TODO: Below is temporary behaviour to prepend root: to resolved TURL(s) for case when not a ROOT file
    #This instructs the Gaudi applications to use root to access different file types e.g. for MDF.
    #In the longer term this should be derived from file catalog metadata information.
    tmpDict = {}
    for lfn,mdata in resolvedData.items():
      tmpDict[lfn]=mdata
      if re.search('.raw$',lfn):
        #correctedTURL = 'root:%s' %(val)
        tmpDict[lfn].update({'turl':'root:%s' %(resolvedData[lfn]['turl'])})
        self.log.verbose('Prepending root: to TURL for %s' %lfn)

    resolvedData = tmpDict
    appCatalog = PoolXMLSlice('pool_xml_catalog.xml')
    check = appCatalog.execute(resolvedData)
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
