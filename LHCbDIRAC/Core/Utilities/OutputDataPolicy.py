""" OutputDataPolicy generates the output data that will be created by a workflow task

    DIRAC assumes an execute() method will exist during usage.
"""
__RCSID__ = "$Id$"

from DIRAC                                          import gLogger
from DIRAC.Interfaces.API.Job                       import Job
from LHCbDIRAC.Core.Utilities.ProductionData        import preSubmissionLFNs

class OutputDataPolicy:

  def __init__( self, paramDict ):
    self.paramDict = paramDict

  def execute( self ):
    jobDescription = self.paramDict['Job']
    prodID = self.paramDict['TransformationID']
    jobID = self.paramDict['TaskID']

    job = Job( jobDescription )
    result = preSubmissionLFNs( job._getParameters(), job.createCode(),
                               productionID = prodID, jobID = jobID )
    if not result['OK']:
      gLogger.error( result )
    return result
