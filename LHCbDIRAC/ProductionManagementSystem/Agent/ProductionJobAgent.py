########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionJobAgent.py,v 1.13 2009/04/18 18:27:00 rgracian Exp $
########################################################################

"""  The Production Job Agent automatically submits production jobs after
     they have been created.  This interacts with WMS Admin interface to
     setup the current shift production manager credential and uses the
     Dirac Production interface to submit the jobs.
"""

__RCSID__ = "$Id: ProductionJobAgent.py,v 1.13 2009/04/18 18:27:00 rgracian Exp $"

from DIRAC.Core.Base.Agent                                import Agent
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from DIRAC.Interfaces.API.DiracProduction                 import DiracProduction
from DIRAC.Core.Utilities.Shifter                         import setupShifterProxyInEnv
from DIRAC                                                import S_OK, S_ERROR, gConfig, gMonitor

import os, time, string

AGENT_NAME = 'ProductionManagement/ProductionJobAgent'

class ProductionJobAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.jobsToSubmitPerProduction = gConfig.getValue(self.section+'/JobsToSubmitPerProduction',50)
    self.productionStatus = gConfig.getValue(self.section+'/SubmitStatus','automatic')
    self.enableFlag = None
    gMonitor.registerActivity("SubmittedJobs","Automatically submitted jobs","Production Monitoring","Jobs", gMonitor.OP_ACUM)
    return result

  #############################################################################
  def execute(self):
    """The ProductionJobAgent execution method.
    """
    self.enableFlag = gConfig.getValue(self.section+'EnableFlag','True')
    if not self.enableFlag == 'True':
      self.log.info('ProductionJobAgent is disabled by configuration option %s/EnableFlag' %(self.section))
      return S_OK('Disabled via CS flag')

    result = setupShifterProxyInEnv( "ProductionManager" )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't get shifter's proxy: %s" % result[ 'Message' ] )

    diracProd=DiracProduction()
    result = diracProd.getActiveProductions()
    self.log.verbose(result)
    if not result['OK']:
      self.log.warn('Failed to get list of active productions')
      self.log.warn(result)
      return result
    activeProductions = result['Value']
    if not activeProductions:
      self.log.info('No active productions discovered, no work to do')
      return S_OK('Loop completed')

    for production,status in activeProductions.items():
      if status.lower()==self.productionStatus:
        self.log.info('Attempting to submit %s jobs for production %s' %(self.jobsToSubmitPerProduction,production))
        start = time.time()
        result = diracProd.submitProduction(production,self.jobsToSubmitPerProduction)
        timing = time.time() - start
        if not result['OK']:
          self.log.warn(result['Message'])
        else:
          jobsDict = result['Value']
          if jobsDict.has_key('Successful'):
            jobsList = jobsDict['Successful']
            submittedJobs = len(jobsList)
            gMonitor.addMark("SubmittedJobs",int(submittedJobs))
            if submittedJobs:
              self.log.info('Production %s submission time: %.2f seconds for %s jobs' % (production,timing,submittedJobs))
            else:
              self.log.info('No jobs to submit for production %s' %(production))
      else:
        self.log.verbose('Nothing to do for productionID %s with status %s' %(production,status))

    return S_OK('Productions submitted')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
