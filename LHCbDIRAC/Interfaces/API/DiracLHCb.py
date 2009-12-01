########################################################################
# $HeadURL$
# Author: Andrew C. Smith
########################################################################

"""
   LHCb API Class

   The LHCb API exposes LHCb specific functionality in addition to the standard DIRAC API.

"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

__RCSID__ = "$Id$"

from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List                      import breakListIntoChunks, sortList
from DIRAC.Interfaces.API.Dirac                     import Dirac
from LHCbDIRAC.Core.Utilities.ClientTools           import mergeRootFiles,getRootFileGUID

import os, glob, fnmatch

COMPONENT_NAME='DiracLHCb'

class DiracLHCb(Dirac):

  #############################################################################
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the DIRAC API.
    """
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)

  def addRootFile(self,lfn,fullPath,diracSE,printOutput=False):
    res = getRootFileGUID(fullPath)
    if not res['OK']:
      return self.__errorReport(res['Message'],"Failed to obtain root file GUID.")
    res = self.addFile(lfn,fullPath,diracSE,fileGuid=res['Value'],printOutput=printOutput)
    return res

  def rootMergeRepository(self,outputFileName,inputFileMask='*.root',location='Sandbox',requestedStates=['Done']):
    """ Create a merged ROOT file using root files retrived in the sandbox or output data
    
       Example Usage:

       >>> print dirac.rootMergeRepository('MyMergedRootFile.root',inputFileMask='DVHistos.root',location='Sandbox', requestedStates = ['Done'])
       {'OK': True, 'Value': ''}
     
       @param outputFileName: The target merged file
       @type outputFileName: string
       @param inputFileMask: Mask to be used when locating input files. Can support wildcards like 'Tuple*.root'
       @type inputFileMask: string
       @param location: The input files present either in the 'Sandbox' (retrieved with getOutputSandbox) or 'OutputFiles' (getJobOutputData)
       @type location: string
       @param requestedStates: List of jobs states to be considered
       @type requestedStates: list of strings
       @return: S_OK,S_ERROR
    """
    if not self.jobRepo:
      self.log.warn("No repository is initialised") 
      return S_OK()  

    # Get the input files to be used
    jobs = self.jobRepo.readRepository()['Value']
    inputFiles = []
    for jobID in sortList(jobs.keys()):
      jobDict = jobs[jobID]
      if jobDict.has_key('State') and jobDict['State'] in requestedStates:
        if location == 'OutputFiles':
          jobFiles = eval(jobDict[location])
          for jobFile in jobFiles:
            fileName = os.path.basename(jobFile)
            if fnmatch.fnmatch(fileName,inputFileMask):
              if os.path.exists(jobFile):
                inputFiles.append(jobFile)
              else:
                self.log.warn("Repository output file does not exist locally",jobFile)
        elif location == 'Sandbox':
          globStr = "%s/%s" % (jobDict[location],inputFileMask)
          print glob.glob(globStr)
          inputFiles.extend(glob.glob(globStr))
        else:
          return self.__errorReport("Location of .root should be 'Sandbox' or 'OutputFiles'.")

    # Perform the root merger
    res = mergeRootFiles(outputFileName,inputFiles,daVinciVersion='')
    if not res['OK']:
      return self.__errorReport(res['Message'],"Failed to perform final ROOT merger")
    return S_OK()

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error
    self.log.warn(error)
    return S_ERROR(message)
