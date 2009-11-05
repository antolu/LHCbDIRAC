########################################################################
# $HeadURL$
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

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Time                             import fromString,toEpoch
from DIRAC.Core.Utilities.SiteSEMapping                    import getSitesForSE
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

class CondDBAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """Initialize specific parameters for CondDBAgent.
    """
    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "ProductionManager" )

    self.tagWaitTime = self.am_getOption( 'MaxTagWaitTime', 12  )

    result = S_OK()
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
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """
    result = self.__checkCondDBTags( job, classAdJob )
    if not result['OK']:
      param = result['Message']
      self.log.info('CondDB tags not yet available for job %s with message -  %s' %(job,param))
      result = self.__checkLastUpdate(job)
      if not result['OK']:
        self.log.warn(result['Message'])
        report = self.setJobParam( job, self.optimizerName, param )
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
  def __checkCondDBTags( self, job, classAdJob ):
    """This method establishes the LFNs for CondDB tags according to the convention and
       determines any site candidates already specified.
    """
    if not classAdJob.lookupAttribute('CondDBTags'):
      self.log.warn('No CondDB tag requirement found for job %s' %(job))
      return S_ERROR('Illegal Job Path')

    lfns = []
    condDBtagsList = classAdJob.get_expression('CondDBTags').replace('{','').replace('}','').replace('"','').replace(',','').split()
    for tag in condDBtagsList:
      tmp = tag.split('.')
      if not len(tmp)==2:
        return S_ERROR('Illegal CondDB Tag Requirement')
      lfns.append(lfnConvention.replace('<NAME>',tmp[0]).replace('<TAG>',tmp[1]))

    if not lfns:
      return S_ERROR('Illegal CondDB Tag Requirement')

    self.log.info( 'CondDB LFNs to check for job %s are:\n%s' % ( job, "\n".join( lfns ) ) )
    destinationSite = classAdJob.get_expression('Site').replace('"','')
    result = self.__getSitesForTags( job, lfns, destinationSite )
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
    for lfn,replicas in tags.items():
      siteList = []
      for se in replicas.keys():
        sites = getSitesForSE(se)
        if sites['OK']:
          siteList += sites['Value']
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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
