########################################################################
# $Id: InputDataResolution.py,v 1.15 2009/07/14 10:01:31 rgracian Exp $
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

__RCSID__ = "$Id: InputDataResolution.py,v 1.15 2009/07/14 10:01:31 rgracian Exp $"

from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
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
    result = self.__resolveInputData()
    if not result['OK']:
      self.log.warn('InputData resolution failed with result:\n%s' %(result))

    #For local running of this module we can expose an option to ignore missing files
    ignoreMissing = False
    if self.arguments.has_key('IgnoreMissing'):
      ignoreMissing = self.arguments['IgnoreMissing']

    #For LHCb original policy was as long as one TURL exists, this can be conveyed to the application
    #this breaks due to the stripping so the policy has been changed.
    failedReplicas = result['Failed']
    if failedReplicas and not ignoreMissing:
      self.log.error('Failed to obtain access to the following files:\n%s' %(string.join(failedReplicas,'\n')))
      return S_ERROR('Failed to access all of requested input data')


    if not result['Successful']:
      return S_ERROR('Could not access any requested input data')

    #TODO: Must define file types in order to pass to POOL XML catalogue.  In the longer
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

    #TODO: Below is temporary behaviour to prepend root: to resolved TURL(s) for case when not a ROOT file
    #This instructs the Gaudi applications to use root to access different file types e.g. for MDF.
    #In the longer term this should be derived from file catalog metadata information.
    tmpDict = {}
    for lfn,mdata in resolvedData.items():
      tmpDict[lfn]=mdata
      if re.search('.raw$',lfn):
        tmpDict[lfn].update({'turl':'root:%s' %(resolvedData[lfn]['turl'])})
        self.log.verbose('Prepending root: to TURL for %s' %lfn)
        #self.log.verbose('Would have been prepending root: to TURL for %s (DISABLED)' %lfn)
        #self.log.verbose('Manually appending ?filetype=raw: to TURL for %s' %lfn)
        #tmpDict[lfn].update({'turl':'%s?filetype=raw' %(resolvedData[lfn]['turl'])})

    resolvedData = tmpDict
    catalogName = 'pool_xml_catalog.xml'
    if self.arguments['Configuration'].has_key('CatalogName'):
      catalogName = self.arguments['Configuration']['CatalogName']

    self.log.verbose('Catalog name will be: %s' %catalogName)
    resolvedData = tmpDict
    appCatalog = PoolXMLSlice(catalogName)
    check = appCatalog.execute(resolvedData)
    if not check['OK']:
      return check
    return result

  #############################################################################
  def __resolveInputData(self):
    """This method controls the execution of the DIRAC input data modules according
       to the LHCb VO policy defined in the configuration service.
    """
    if self.arguments['Configuration'].has_key('SiteName'):
      site = self.arguments['Configuration']['SiteName']
    else:
      site = gConfig.getValue('/LocalSite/Site','')
      if not site:
        return S_ERROR('Could not resolve site from /LocalSite/Site')

    self.log.verbose('Attempting to resolve input data policy for site %s' %site)
    inputDataPolicy = gConfig.getOptionsDict('/Operations/InputDataPolicy')
    if not inputDataPolicy:
      return S_ERROR('Could not resolve InputDataPolicy from /Operations/InputDataPolicy')

    options = inputDataPolicy['Value']
    if options.has_key(site):
      policy = options[site]
      policy = [x.strip() for x in string.split(policy,',')]
      self.log.info('Found specific input data policy for site %s:\n%s' %(site,string.join(policy,',\n')))
    elif options.has_key('Default'):
      policy = options['Default']
      policy = [x.strip() for x in string.split(policy,',')]
      self.log.info('Applying default input data policy for site %s:\n%s' %(site,string.join(policy,',\n')))

    dataToResolve = None #if none, all supplied input data is resolved
    allDataResolved = False
    successful = {}
    failedReplicas=[]
    for modulePath in policy:
      if not allDataResolved:
        result = self.__runModule(modulePath,dataToResolve)
        if not result['OK']:
          self.log.warn('Problem during %s execution' %modulePath)
          return result

        if result.has_key('Failed'):
          failedReplicas=result['Failed']

        if failedReplicas:
          self.log.info('%s failed for the following files:\n%s' %(modulePath,string.join(failedReplicas,'\n')))
          dataToResolve = failedReplicas
        else:
          self.log.info('All replicas resolved after %s execution' %(modulePath))
          allDataResolved=True

        successful.update(result['Successful'])
        self.log.verbose(successful)

    result = S_OK()
    result['Successful']=successful
    result['Failed']=failedReplicas
    return result

  #############################################################################
  def __runModule(self,modulePath,remainingReplicas):
    """This method provides a way to run the modules specified by the VO that
       govern the input data access policy for the current site.  For LHCb the
       standard WMS modules are applied in a different order depending on the site.
    """
    self.log.info('Attempting to run %s' %(modulePath))
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule(modulePath,self.arguments)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute(remainingReplicas)
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
