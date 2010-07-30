########################################################################
# $HeadURL$
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently added to the existing input data requirement of the job.

      Initially the Ancestor Files Agent uses the previous Bookkeeping
      'genCatalog' utility but this will be updated in due course.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd

from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles      import getAncestorFiles 

from DIRAC                                                 import gConfig, S_OK, S_ERROR

import re, time

class AncestorFilesAgent( OptimizerModule ):

  #############################################################################
  def checkJob( self, job, classadJob ):
    """ The main agent execution method
    """
    result = self.__checkAncestorDepth( job, classadJob )
    if not result['OK']:
      return result

    return self.setNextOptimizer(job)

  #############################################################################
  def __checkAncestorDepth( self, job, classadJob ):
    """This method checks the input data with ancestors. The original job JDL
       is always extracted to obtain the input data, therefore rescheduled jobs
       will not recursively search for ancestors of ancestors etc.
    """
    inputData = []
    if classadJob.lookupAttribute( 'InputData' ):
      inputData = classadJob.getListFromExpression( 'InputData' )

    if not classadJob.lookupAttribute( 'AncestorDepth' ):
      self.log.warn( 'No AncestorDepth requirement found for job %s' %( job ) )
      return S_ERROR( 'AncestorDepth Not Found' )

    ancestorDepth = classadJob.getAttributeInt( 'AncestorDepth' )

    if ancestorDepth==0:
      return S_OK( 'Null AncestorDepth specified' )

    self.log.info( 'Job %s has %s input data files and specified ancestor depth of %s' % ( job,
                                                                                           len( inputData ),
                                                                                           ancestorDepth ) )
    result = self.__getInputDataWithAncestors( job, inputData, ancestorDepth )
    if not result['OK']:
      return result

    newInputData = result['Value']

    classadJob.insertAttributeVectorString( 'InputData', newInputData )
    newJDL = classadJob.asJDL()
    result = self.__setJobInputData( job, newJDL, newInputData )
    return result

  ############################################################################
  def __getInputDataWithAncestors( self, job, inputData, ancestorDepth ):
    """Extend the list of LFNs with the LFNs for their ancestor files
       for the generation depth specified in the job JDL.
    """
    inputData = [ i.replace('LFN:','') for i in inputData ]
    start = time.time()
    try:
      result = getAncestorFiles( inputData, ancestorDepth )
    except Exception,x:
      self.log.warn('getAncestors failed with exception:\n%s' %x)
      return S_ERROR(self.failedMinorStatus)

    self.log.info('BK lookup time %.2f s' %(time.time()-start))
    self.log.debug(result)
    if not result['OK']:
      report = self.setJobParam( job, self.am_getModuleParam( 'optimizerName' ), result['Message'] )
      self.log.warn(result['Message'])
      return S_ERROR('No Ancestors Found For Input Data')

    newInputData = result['Value']
    totalAncestors = len(newInputData)-len(inputData)
    param = '%s ancestor files retrieved from BK for depth %s' %(totalAncestors,ancestorDepth)
    report = self.setJobParam(job, self.am_getModuleParam( 'optimizerName' ), param)
    if not report['OK']:
      self.log.warn(report['Message'])

    return result

  #############################################################################
  def __setJobInputData( self, job, jdl, inputData ):
    """Sets the new job input data requirement including ancestor files.
    """
    inputData = [ i.replace('LFN:','') for i in inputData]
    inputData = map( lambda x: 'LFN:'+x, inputData)

    result = self.jobDB.setInputData( job, inputData )
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Setting New Input Data')

    result = self.jobDB.setJobJDL( job, jdl )
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Setting New JDL')

    return S_OK('Job updated')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
