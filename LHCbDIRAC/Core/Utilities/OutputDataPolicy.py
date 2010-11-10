""" OutputDataPolicy generates the output data that will be created by a workflow task

    DIRAC assumes an execute() method will exist during usage.
"""
__RCSID__   = "$Id: OutputDataPolicy.py 19570 2010-01-07 08:42:02Z joel $"
__VERSION__ = "$Revision: 1.40 $"

import DIRAC
from DIRAC                                          import gLogger
from DIRAC.Interfaces.API.Job                       import Job
from LHCbDIRAC.Core.Utilities.ProductionData        import preSubmissionLFNs

class OutputDataPolicy:

  def __init__(self,paramDict):
    self.paramDict = paramDict

  def execute(self):
    jobDescription = self.paramDict['Job']
    prodID = self.paramDict['TransformationID']
    jobID = self.paramDict['TaskID']
    inputData = self.paramDict['InputData']
    
    job = Job(jobDescription)    
    result = preSubmissionLFNs(job._getParameters(),job.createCode(),
                               productionID=prodID,jobID=jobID,inputData=inputData)
    if not result['OK']:
      gLogger.error(result)
    return result
