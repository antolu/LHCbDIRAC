########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Client/DiracSAM.py,v 1.1 2008/07/21 18:34:34 paterson Exp $
# File :   DiracSAM.py
# Author : Stuart Paterson
########################################################################

"""LHCb SAM Dirac Class

   The Dirac SAM class inherits generic VO functionality from the Dirac API base class.

"""

__RCSID__ = "$Id: DiracSAM.py,v 1.1 2008/07/21 18:34:34 paterson Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Interfaces.API.Dirac                     import *
from DIRAC.LHCbSystem.Testing.SAM.Client.LHCbSAMJob import LHCbSAMJob
from DIRAC.Core.Utilities.SiteCEMapping             import getCESiteMapping,getSiteForCE
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

class DiracSAM(Dirac):

  #############################################################################
  def __init__(self):
    """Instantiates the Workflow object and some default parameters.
    """
    Dirac.__init__(self)
    self.gridType = 'LCG'
    self.bannedSites = gConfig.getValue('/Operations/SAM/BannedSites',[])
    self.samRole = gConfig.getValue('/Operations/SAM/DefaultRole','lhcb_admin')
    self.log = gLogger.getSubLogger( "DiracSAM" )

  #############################################################################
  def submitAllSAMJobs(self):
    """Submit SAM tests to all possible CEs as defined in the CS.
    """
    result = getCESiteMapping(self.gridType)
    if not result['OK']:
      return result
    ceSiteMapping = {}
    self.log.verbose('Banned SAM sites are: %s' %(string.join(self.bannedSites,', ')))
    for ce,site in result['Value'].items():
      if not site in self.bannedSites:
        ceSiteMapping[ce] = site

    self.log.info('Preparing jobs for %s CEs' %(len(ceSiteMapping.keys())))
    for ce in ceSiteMapping.keys():
      result = self.submitSAMJob(ce)
      if not result['OK']:
        self.log.info('Submission of SAM job to CE %s failed with message:\n%s' %(ce,result['Message']))

    return S_OK()

  #############################################################################
  def submitSAMJob(self,ce,removeLock=False,deleteSharedArea=False,enable=True,logFlag=True,publishFlag=True,mode=None):
    """Submit a SAM test job to an individual CE.
    """
    job = None
    try:
      job = LHCbSAMJob()
      job.setDestinationCE(ce)
      self.log.verbose('Flag to remove lock on shared area is %s' %(removeLock))
      job.setSharedAreaLock(forceDeletion=removeLock,enableFlag=enable)
      job.checkSystemConfiguration(enableFlag=enable)
      job.checkSiteQueues(enableFlag=enable)
      self.log.verbose('Flag to force deletion of shared area is %s' %(deleteSharedArea))
      job.installSoftware(forceDeletion=deleteSharedArea,enableFlag=enable)
      job.testApplications(enableFlag=enable)
      job.finalizeAndPublish(logUpload=logFlag,publishResults=publishFlag,enableFlag=enable)
    except Exception,x:
      self.log.warn('Creating SAM job failed with exception: %s' %x)
      return S_ERROR(str(x))
    self.log.verbose('Job JDL is: \n%s' %job._toJDL())

    if not job:
      return S_ERROR('Could not create job for CE %s' %ce)

    return self.submit(job,mode)

  #############################################################################
  def _promptUser(self,message):
    """Internal function to prompt user before submitting all SAM test jobs.
    """
    self.log.info('%s %s' %(message,'[yes/no] : '))
    response = raw_input('%s %s' %(message,'[yes/no] : '))
    responses = ['yes','y','n','no']
    if not response.strip() or response=='\n':
      self.log.info('Possible responses are: %s' %(string.join(responses,', ')))
      response = raw_input('%s %s' %(message,'[yes/no] : '))

    if not response.strip().lower() in responses:
      self.log.info('Problem interpreting input "%s", assuming negative response.' %(response))
      return S_ERROR(response)

    if response.strip().lower()=='y' or response.strip().lower()=='yes':
      return S_OK(response)
    else:
      return S_ERROR(response)

  #############################################################################
  def __checkCurrentProxy(self):
    """Check that the current proxy has the correct DIRAC role.
    """
    #Under development
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#