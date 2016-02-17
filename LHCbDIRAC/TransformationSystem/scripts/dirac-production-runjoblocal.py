#!/usr/bin/env python
'''
  dirac-production-runjoblocal
  
  Module created to run failed jobs locally on a CVMFS-configured machine.
  It creates the necessary environment, downloads the necessary files, modifies the necessary 
  files and runs the job
  
  Usage:
    dirac-production-runjoblocal (job ID) (Data imput mode) -  No parenthesis
    
'''
__RCSID__ = "$Id$"


import DIRAC
import LHCbDIRAC
import os
import sys
import errno
#from DIRAC.Core.Utilities import DError
from DIRAC.Core.Base      import Script
from DIRAC                import S_OK, S_ERROR

Script.registerSwitch( 'D:', 'Download='    , 'Defines data acquisition as DownloadInputData'   )
Script.registerSwitch( 'P:', 'Protocol='    , 'Defines data acquisition as InputDataByProtocol' )
Script.parseCommandLine( ignoreErrors = False )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                      '\nUsage:',
                                      'dirac-production-runjoblocal [Data imput mode] [job ID]'
                                      '\nArguments:',
                                      '  Download (Job ID): Defines data aquisition as DownloadInputData',
                                      '  Protocol (Job ID): Defines data acquisition as InputDataByProtocol\n'] ) )

_downloadinputdata = False
_jobID = None

for switch in Script.getUnprocessedSwitches():
  if switch [ 0 ] in ( 'D', 'Download' ):
    _downloadinputdata = True
    _jobID = switch[1]
  if switch [ 0 ] in ( 'P', 'Protocol' ):
    _downloadinputdata = False
    _jobID = switch[1]

def __runSystemDefaults(jobID = None):
  """
  Creates the environment for running the job and returns
  the path for the other functions.
  
  """
  tempdir = "LHCbjob" + str(jobID) + "temp"
  os.environ['VO_LHCB_SW_DIR'] = "/cvmfs/lhcb.cern.ch"
  try:
    os.mkdir(tempdir)
    if not sys.exc_info()[1][0]:
      S_OK("Temporary directory created.")    
    pass
  except:    
    if sys.exc_info()[1][0] == 17:
      S_OK("Temporary directory already exists.")
    elif sys.exc_info()[1][0] == 30:
      print sys.exc_info()[1], "Unable to create temporary directory"
#      DError(errno.EROFS, "Unable to create temporary directory")

    
  basepath = os.getcwd()
  return basepath + "/" + tempdir

def __downloadJobDescriptionXML(jobID, basepath):
  """
  Downloads the jobDescription.xml file into the temporary directory
  created.
  
  """
  from DIRAC.Interfaces.API.Dirac import Dirac
  jdXML = Dirac()
  jdXML.getInputSandbox(jobID, basepath)

def __modifyJobDescription(jobID, basepath, downloadinputdata):
  """
  Modifies the jobDescription.xml to, instead of DownloadInputData, it 
  uses InputDataByProtocol
  
  """
  if not downloadinputdata:
    from xml.etree import ElementTree as et
    archive = et.parse(basepath + "/InputSandbox" + str(jobID) + "/jobDescription.xml")
    for element in archive.getiterator():
      if element.text == "DIRAC.WorkloadManagementSystem.Client.DownloadInputData":
        element.text = "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
        archive.write(basepath + "/InputSandbox" + str(jobID) + "/jobDescription.xml")
        return S_OK("Job parameter changed from DownloadInputData to InputDataByProtocol.")

def __downloadPilotScripts(basepath):
  """
  Downloads the scripts necessary to configure the pilot
  
  """
  #include retry function
  out = os.system("wget -P " + basepath +  "/ http://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilotscripts/LHCbPilotCommands.py")
  if not out:
    S_OK("LHCbPilotCommands.py script successfully download.\n")
  else:
    print "LHCbPilotCommands.py script download error.\n"
    #DError(errno.ENETUNREACH, "LHCbPilotCommands.py script download error.\n" )
  out = os.system("wget -P " + basepath +  "/ http://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilotscripts/dirac-pilot.py")
  if not out:
    S_OK("dirac-pilot.py script successfully download.\n")
  else:
    print "dirac-pilot.py script download error.\n"
    #DError(errno.ENETUNREACH, "dirac-pilot.py script download error.\n" )
  out = os.system("wget -P " + basepath +  "/ http://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilotscripts/pilotCommands.py")
  if not out:
    S_OK("pilotCommands.py script successfully download.\n")
  else:
    print "pilotCommands.py script download error.\n"
    #DError(errno.ENETUNREACH, "pilotCommands.py script download error.\n" )
  out = os.system("wget -P " + basepath +  "/ http://lhcbproject.web.cern.ch/lhcbproject/Operations/VM/pilotscripts/pilotTools.py")
  if not out:
    S_OK("pilotTools.py script successfully download.\n")
  else:
    print "pilotTools.py script download error.\n"
    #DError(errno.ENETUNREACH, "pilotTools.py script download error.\n" )
    
    
def __configurePilot(basepath):
  """
  Configures the pilot.
  
  """
  out = os.system("python " + basepath + "/dirac-pilot.py -S LHCb-Production -l LHCb -C dips://lbvobox18.cern.ch:9135/Configuration/Server -N ce.debug.ch -Q default -n DIRAC.JobDebugger.ch -M 1 -E LHCbPilot -X LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements -dd")
  if not out:
    dir = str(os.getcwd())
    os.rename(dir + '/.dirac.cfg', dir + '/.dirac.cfg.old')
    os.system("cp " + dir + "/pilot.cfg " + dir + "/.dirac.cfg")
    return S_OK("Pilot successfully configured.")
  
#   else:
#     some DErrno message

def __runJobLocally(jobID, basepath):
  """
  Runs the job!
  
  """
  from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
  localJob = LHCbJob(basepath + "/InputSandbox" + str(jobID) + "/jobDescription.xml")
  localJob.setInputSandbox(os.getcwd()+"/pilot.cfg")
  localJob.setConfigArgs(os.getcwd()+"/pilot.cfg")
  os.chdir(basepath)
  localJob.runLocal()
  
if __name__ == "__main__":
  dir = os.getcwd()
  try:
    _path = __runSystemDefaults(_jobID)
      
    __downloadJobDescriptionXML(_jobID, _path)
      
    __modifyJobDescription(_jobID, _path, _downloadinputdata)
    
    __downloadPilotScripts(_path)
    
    __configurePilot(_path)
    
    __runJobLocally(_jobID, _path)
    
  finally:
    os.chdir(dir)
    os.rename(dir + '/.dirac.cfg.old', dir + '/.dirac.cfg')