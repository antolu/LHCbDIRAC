########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/CondDBAgent.py,v 1.4 2008/06/09 11:51:41 paterson Exp $
# File :   CondDBAgent.py
# Author : Stuart Paterson
########################################################################

""" The LHCb Conditions DB Agent processes jobs with CondDB tag requirements.

    Tags are of the form:
    - /lhcb/database/tags/<NAME>/<TAG>

    Some examples are:
    - /lhcb/database/tags/DDDB/DC06
    - /lhcb/database/tags/LHCBCOND/DC06

    If a requested tag is not found, the jobs are held in a waiting state prior
    to job scheduling for a configurable time period.  The jobs are then failed
    if the requested tag does not become available.
"""

__RCSID__ = "$Id: CondDBAgent.py,v 1.4 2008/06/09 11:51:41 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Time                             import fromString,toEpoch
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,restoreProxy,setDIRACGroup,getProxyTimeLeft,setupProxyFile
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

OPTIMIZER_NAME = 'CondDB'

class CondDBAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True,system='LHCb')

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for CondDBAgent.
    """
    result = Optimizer.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',5*60) #seconds
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',24) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/CondDBAgent/shiftProdProxy')
    self.tagWaitTime = gConfig.getValue(self.section+'/MaxTagWaitTime',12) #hours
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.site_se_mapping = {}
    mappingKeys = gConfig.getOptions('/Resources/SiteLocalSEMapping')
    for site in mappingKeys['Value']:
      seStr = gConfig.getValue('/Resources/SiteLocalSEMapping/%s' %(site))
      self.log.verbose('Site: %s, SEs: %s' %(site,seStr))
      self.site_se_mapping[site] = [ x.strip() for x in string.split(seStr,',')]

    try:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.fileCatalog = LcgFileCatalogCombinedClient()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.fatal(msg)
      self.log.fatal(str(x))
      result = S_ERROR(msg)
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','')
    if not prodDN:
      self.log.warn('Production shift manager DN not defined (/Operations/Production/ShiftManager)')
      return S_OK('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s' %(prodDN))
    result = self.__getProdProxy(prodDN)
    if not result['OK']:
      self.log.warn('Could not set up proxy for shift manager %s %s' %(prodDN))
      return S_OK('Production shift manager proxy could not be set up')

    self.log.verbose("Checking JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job)
    if not retVal['OK']:
      self.log.warn("Missing JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('JDL not found in JobDB')

    jdl = retVal['Value']
    if not jdl:
      self.log.warn("Null JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Null JDL returned from JobDB')

    result = self.__checkCondDBTags(job,jdl)
    if not result['OK']:
      param = result['Message']
      self.log.info('CondDB tags not yet available for job %s with message -  %s' %(job,param))
      result = self.__checkLastUpdate(job)
      if not result['OK']:
        self.log.warn(result['Message'])
        report = self.setJobParam(job,self.optimizerName,param)
        if not report['OK']:
          self.log.warn(report['Message'])
        return S_ERROR('CondDB Tag Not Available')

      return S_OK()

    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.warn(result['Message'])
    return result

  #############################################################################
  def __checkLastUpdate(self,job):
    """This method checks whether any pending jobs have waited longer than the
       maximum allowed time.
    """
    result = self.jobDB.getJobAttributes(job,['LastUpdateTime'])
    if not result['OK'] or not result['Value']:
      self.log.warn(result)
      return S_ERROR('Could not get LastUpdateTime for job %s' %job)

    self.log.verbose(result)
    if not result['Value']['LastUpdateTime'] or result['Value']['LastUpdateTime']=='None':
      self.log.verbose('LastUpdateTime is null for job %s' %job)
      return S_ERROR('Could not get LastUpdateTime for job %s' %job)

    lastUpdate = toEpoch(fromString(result['Value']['LastUpdateTime']))
    currentTime = time.mktime(time.gmtime())

    elapsedTime = currentTime-lastUpdate
    self.log.verbose('(CurrentTime-LastUpdate) = %s secs' %(elapsedTime))
    limit = self.tagWaitTime*60*60
    if elapsedTime > limit:
      self.log.info('Job %s has waited %s secs for tags (exceeding limit of %s hours)' %(job,elapsedTime,self.tagWaitTime))
      return S_ERROR('Max Tag Waiting time is exceeded for job %s' %(job))

    return S_OK()

  #############################################################################
  def __checkCondDBTags(self,job,jdl):
    """This method establishes the LFNs for CondDB tags according to the convention and
       determines any site candidates already specified.
    """
    lfnConvention = gConfig.getValue(self.section+'/LFNConvention','/lhcb/database/tags/<NAME>/<TAG>')
    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.warn("Illegal JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Illegal Job JDL')

    if not classadJob.lookupAttribute('CondDBTags'):
      self.log.warn('No CondDB tag requirement found for job %s' %(job))
      return S_ERROR('Illegal Job Path')

    lfns = []
    condDBtagsList = classadJob.get_expression('CondDBTags').replace('{','').replace('}','').replace('"','').replace(',','').split()
    for tag in condDBtagsList:
      tmp = tag.split('.')
      if not len(tmp)==2:
        return S_ERROR('Illegal CondDB Tag Requirement')
      lfns.append(lfnConvention.replace('<NAME>',tmp[0]).replace('<TAG>',tmp[1]))

    if not lfns:
      return S_ERROR('Illegal CondDB Tag Requirement')

    self.log.info('CondDB LFNs to check for job %s are:\n%s' %(job,string.join(lfns,',\n')))
    destinationSite = classadJob.get_expression('Site').replace('"','')
    result = self.__getSitesForTags(job,lfns,destinationSite)
    return result

  #############################################################################
  def __getSitesForTags(self,job,lfns,destinationSite=None):
    """This method checks the CondDB tags in the file catalogue.
    """
    start = time.time()
    replicas = self.fileCatalog.getReplicas(lfns)
    self.log.verbose(replicas)
    timing = time.time() - start
    self.log.info('LFC Lookup Time: %.2f seconds ' % (timing) )
    if not replicas['OK']:
      self.log.warn(replicas['Message'])
      return replicas

    badLFNCount = 0
    badLFNs = []
    catalogResult = replicas['Value']

    if catalogResult.has_key('Failed'):
      for lfn,cause in catalogResult['Failed'].items():
        badLFNCount+=1
        badLFNs.append('LFN:%s Problem: %s' %(lfn,cause))

    if catalogResult.has_key('Successful'):
      for lfn,reps in catalogResult['Successful'].items():
        if not reps:
          badLFNs.append('LFN:%s Problem: Null replica value' %(lfn))
          badLFNCount+=1

    if badLFNCount:
      self.log.info('Found %s problematic CondDB LFN(s) for job %s' % (badLFNCount,job) )
      param = string.join(badLFNs,'\n')
      return S_ERROR(param)

    tags = catalogResult['Successful']

    fileSEs = {}
    siteSEMapping = self.site_se_mapping
    for lfn,replicas in tags.items():
      siteList = []
      for se in replicas.keys():
        sites = self.__getSitesForSE(se)
        if sites:
          siteList += sites
      fileSEs[lfn] = siteList

    siteCandidates = []
    i = 0
    for file,sites in fileSEs.items():
      if not i:
        siteCandidates = sites
      else:
        tempSite = []
        for site in siteCandidates:
          if site in sites:
            tempSite.append(site)
        siteCandidates = tempSite
      i += 1

    if not len(siteCandidates):
      return S_ERROR('No Candidate Sites Available')

    condDBSites = gConfig.getValue(self.section+'/CondDBSites','LCG.CERN.ch,LCG.CNAF.it,LCG.IN2P3.fr,LCG.GRIDKA.de,LCG.PIC.es,LCG.NIKHEF.nl,LCG.RAL.uk')
    if re.search(',',condDBSites):
      condDBSites = [i.strip() for i in condDBSites.split(',')]

    #Check that tags are available at destination site and pass job.
    if destinationSite:
      if destinationSite in condDBSites:
        self.log.info('Destination site %s is a CondDB site candidate' %(destinationSite))
        condDBSites = [destinationSite]
      else:
        self.log.info('Specified destination %s is not a CondDB site will check all active Tier-1s' %(destinationSite))

    #Assume tags must be available for active Tier-1s before releasing job.
    result = self.__checkSitesInMask(siteCandidates,condDBSites)
    return result

  #############################################################################
  def __checkSitesInMask(self,siteCandidates,condDBSites):
    """Checks that the site candidate list contains all the CondDBSites that are
       in the site mask.
    """
    result = self.jobDB.getSiteMask()
    self.log.debug(result)
    if not result['OK'] or not result['Value']:
      self.log.warn('Failed to get mask from JobDB with result:')
      self.log.warn(result)
      return S_ERROR('Resolving Site Mask')

    mask = result['Value']
    activeSites = []
    for site in condDBSites:
      if site in mask:
        activeSites.append(site)

    if not activeSites:
      return S_ERROR('Destination site(s) not present in site mask:\n%s' %(string.join(condDBSites,', ')))

    tagMissingSites=[]
    for site in activeSites:
      if not site in siteCandidates:
        tagMissingSites.append(site)

    if tagMissingSites:
      return S_ERROR('Requested CondDB tags not available at the following sites:\n%s' %(string.join(tagMissingSites,', ')))

    return S_OK()

  #############################################################################
  def __getSitesForSE(self,se):
    """Returns a list of sites via the site SE mapping for a given SE.
    """
    sites = []
    for site,ses in self.site_se_mapping.items():
      if se in ses:
        sites.append(site)

    return sites

  #############################################################################
  def __getProdProxy(self,prodDN):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
    self.log.info("Determining the length of proxy for DN %s" %prodDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      self.log.info("No proxy found")
      obtainProxy = True
    else:
      res = setupProxyFile(self.proxyLocation)
      if not res["OK"]:
        self.log.error("Could not determine the time left for proxy", res['Message'])
        res = S_OK(0) # force update of proxy

      proxyValidity = int(res['Value'])
      self.log.info('%s proxy found to be valid for %s seconds' %(prodDN,proxyValidity))
      if proxyValidity <= self.minProxyValidity:
        obtainProxy = True

    if obtainProxy:
      self.log.info('Attempting to renew %s proxy' %prodDN)
      res = self.wmsAdmin.getProxy(prodDN,prodGroup,self.proxyLength)
      if not res['OK']:
        self.log.error('Could not retrieve proxy from WMS Administrator', res['Message'])
        return S_OK()
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        self.log.error('Could not create environment for proxy.', res['Message'])
        return S_OK()

      setDIRACGroup(prodGroup)
      self.log.info('Successfully renewed %s proxy' %prodDN)

    #os.system('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
