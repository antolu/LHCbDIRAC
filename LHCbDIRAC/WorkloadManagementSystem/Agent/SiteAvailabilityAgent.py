########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/LHCbSystem/Agent/SiteAvailabilityAgent.py $
# File :   SiteAvailabilityAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb site availability agent examines the site mask prior to job
      scheduling and holds jobs in 'Checking' 'Site Availability' when a
      single destination is unavailable. 
      
      Needs to be able to access the JobDB.
      
      *UNDER DEVELOPMENT*
"""

__RCSID__ = "$Id: SiteAvailabilityAgent.py 18191 2009-11-11 16:46:41Z paterson $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule

from DIRAC                                                 import S_OK, S_ERROR, List, Time


class SiteAvailabilityAgent( OptimizerModule ):

  #############################################################################
  def initializeOptimizer(self):
    """ Initialization of the Agent.
    """
    self.timeDelta = self.am_getOption('AllowedDelay',7*24) #hours 
    # e.g. after one week of waiting fail the jobs
    self.failedMinorStatus = self.am_getOption('FailedStatus','Site Not Available')
    self.dataAgentName        = self.am_getOption('InputDataAgent','InputData')    
    #i.e. will appear as Checking, SiteAvailability
    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """ The main agent execution method
    """
    # First, get Site and BannedSites from the Job
    result = self.__getJobSiteRequirement( job, classAdJob  )
    bannedSites = result['BannedSites']
    sites = result['Sites']

    #Now check whether the job has an input data requirement
    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.warn('Failed to get input data from JobDB for %s' % (job))
      self.log.error(result['Message'])
      return S_ERROR( 'Failed to get input data from JobDB' )

    if not result['Value']:
      return self.setNextOptimizer(job)

    inputData = []
    for i in result['Value']:
      if i: inputData.append(i)

    if not inputData:
      #With no input data requirement, job can proceed directly to task queue
      self.log.verbose('Job %s has no input data requirement' % (job))
      return self.setNextOptimizer(job)

    self.log.verbose('Job %s has an input data requirement ' % (job))

    #Check all optimizer information
    result = self.__checkOptimizerInfo(job)
    if not result['OK']:
      self.log.info('Job %s has a problem: "%s", will pass to job scheduling for taking action' %(job,result['Message']))
      return self.setNextOptimizer(job)

    optInfo = result['Value']

    #Compare site candidates with current mask
    siteCandidates = optInfo['SiteCandidates'].keys()
    self.log.info('Input Data Site Candidates: %s' % (siteCandidates))
    result = self.__checkSitesInMask(job,siteCandidates)
    if not result['OK']:
      self.log.info('Job %s has no possible candidate sites, will pass to job scheduling for taking action' %(job))
      return self.setNextOptimizer(job)
    siteCandidates = result['Value']


    #Compare site candidates with Site / BannedSites in JDL
    if sites:
      # Remove requested sites if data is not available there
      for site in list(sites):
        if site not in siteCandidates:
          sites.remove( site )
      if not sites:
        return S_ERROR( 'Chosen site is not eligible' )

      # Remove requested sites if they are in the provided mask
      for site in list(sites):
        if site in bannedSites:
          sites.remove( site )
      if not sites:
        return S_ERROR( 'No eligible sites for job' )

      siteCandidates = sites
    else:
      for site in bannedSites:
        if site in siteCandidates:
          siteCandidates.remove(site)

      if not siteCandidates:
        return S_ERROR( 'No eligible sites for job' )
 
 
    
    result = self.__getDestinationSites(job)
    if not result['OK']:
      return result

    sites = result['Value']
    if not sites:
      return S_ERROR('No Site Candidates Found')

    #need to check that the job has not been waiting for too long
    #do this last in case site has become available
    result = self.__checkLastUpdateTime(job)
    if not result['OK']:
      self.log.info('Updating job %s to Failed %s as has been waiting for longer than %s hours' %(job,self.failedMinorStatus,self.timeDelta))
      return S_ERROR(self.failedMinorStatus)

    return self.setNextOptimizer(job)

  #############################################################################
  def __checkOptimizerInfo(self,job):
    """This method aggregates information from optimizers to return a list of
       site candidates and all information regarding input data.
    """
    dataResult = self.getOptimizerJobInfo(job,self.dataAgentName)
    if dataResult['OK'] and len(dataResult['Value']):
      self.log.verbose(dataResult)
      if 'SiteCandidates' in dataResult['Value']:
        return S_OK(dataResult['Value'])

      msg = 'No possible site candidates'
      self.log.info(msg)
      return S_ERROR(msg)

    msg = 'File Catalog Access Failure'
    self.log.info(msg)
    return S_ERROR(msg)

  #############################################################################
  def __getJobSiteRequirement(self,job,classAdJob):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """
    result = self.jobDB.getJobAttribute(job,'Site')
    if not result['OK']:
      site = []
    else:
      site = List.fromChar( result['Value'] )

    result = S_OK()

    bannedSites = classAdJob.getAttributeString('BannedSites')
    bannedSites = bannedSites.replace( '}', '' ).replace( '{', '' )
    bannedSites = List.fromChar( bannedSites )

    if not 'ANY' in site and not 'Unknown' in site:
      if len(site)==1:
        self.log.info('Job %s has single chosen site %s specified in JDL' %(job,site[0]))
      result['Sites']=site
    else:
      result['Sites']= []

    if bannedSites:
      self.log.info('Job %s has JDL requirement to ban %s' %(job,bannedSites))
      result['BannedSites']=bannedSites
    else:
      result['BannedSites']=[]

    return result
      
  #############################################################################
  def __checkSitesInMask(self,job,siteCandidates):
    """Returns list of site candidates that are in current mask.
    """
    result = self.jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR('Could not get site mask')

    sites = []
    candidates = []
    allowedSites = result['Value']
    for candidate in siteCandidates:
      if not candidate in allowedSites:
        self.log.verbose('%s is a candidate site for job %s but not in mask' %(candidate,job))
        candidates.append(candidate)
      else:
        sites.append(candidate)

    if not sites and not candidates:
      return S_ERROR('No possible candidate sites')

    return S_OK({'active':sites,'banned':candidates})

  #############################################################################
  def __checkLastUpdateTime(self,job):
    """Returns S_OK() if the LastUpdateTime is within the time delta and 
       S_ERROR if this is outside the acceptable range. 
    """
    selectDelay = self.timeDelta
    result = self.jobDB.getJobAttributes(job,'LastUpdateTime')
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not get attributes for job %s' %job)
    if not result['Value'] or result['Value']=='None':
      self.log.verbose('LastUpdateTime is null for job %s:\n%s\nNo actions will be taken' %(job,result))
      return S_OK()
    
    lastUpdate = Time.fromString(result['Value'])
    delta = Time.hour * selectDelay
    interval = Time.timeInterval(lastUpdate,delta)
    now = Time.dateTime()
    if not interval.includes(now):
      return S_ERROR('Job has been waiting longer than select delay')
      
    self.log.info('Job %s was last updated less than %s hours ago' %(job,selectDelay))
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#  