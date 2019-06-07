#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
'''
  dirac-production-runjoblocal

  Module created to run failed jobs locally on a CVMFS-configured machine.
  It creates the necessary environment, downloads the necessary files, modifies the necessary
  files and runs the job

  Usage:
    dirac-production-runjoblocal (job ID) (Data imput mode) -  No parenthesis

'''

import os
import shutil

from DIRAC import S_OK
from DIRAC.Core.Base import Script

Script.registerSwitch('D:', 'Download=', 'Defines data acquisition as DownloadInputData')
Script.registerSwitch('P:', 'Protocol=', 'Defines data acquisition as InputDataByProtocol')
Script.parseCommandLine(ignoreErrors=False)

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  '\nUsage:',
                                  'dirac-production-runjoblocal [Data imput mode] [job ID]'
                                  '\nArguments:',
                                  '  Download (Job ID): Defines data aquisition as DownloadInputData',
                                  '  Protocol (Job ID): Defines data acquisition as InputDataByProtocol\n']))

from DIRAC.Core.Utilities.File import mkDir

__RCSID__ = "$Id:$"

_downloadinputdata = False
_jobID = None

for switch in Script.getUnprocessedSwitches():
  if switch[0] in ('D', 'Download'):
    _downloadinputdata = True
    _jobID = switch[1]
  if switch[0] in ('P', 'Protocol'):
    _downloadinputdata = False
    _jobID = switch[1]


def __runSystemDefaults(jobID=None):
  """
  Creates the environment for running the job and returns
  the path for the other functions.

  """
  tempdir = "LHCbjob" + str(jobID) + "temp"
  os.environ['VO_LHCB_SW_DIR'] = "/cvmfs/lhcb.cern.ch"
  mkDir(tempdir)

  basepath = os.getcwd()
  return basepath + os.path.sep + tempdir + os.path.sep


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
    archive = et.parse(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
    for element in archive.getiterator():
      if element.text == "DIRAC.WorkloadManagementSystem.Client.DownloadInputData":
        element.text = "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
        archive.write(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
        return S_OK("Job parameter changed from DownloadInputData to InputDataByProtocol.")


def __downloadPilotScripts(basepath):
  """
  Downloads the scripts necessary to configure the pilot

  """
  # include retry function
  out = os.system(
      "wget -P " + basepath
      + " https://gitlab.cern.ch/lhcb-dirac/LHCbPilot/raw/master/LHCbPilot/LHCbPilotCommands.py")
  if not out:
    S_OK("LHCbPilotCommands.py script successfully download.\n")
  else:
    print "LHCbPilotCommands.py script download error.\n"

  out = os.system(
      "wget -P " + basepath
      + " https://raw.githubusercontent.com/DIRACGrid/Pilot/master/Pilot/dirac-pilot.py")
  if not out:
    S_OK("dirac-pilot.py script successfully download.\n")
  else:
    print "download error.\n"

  out = os.system(
      "wget -P " + basepath
      + " https://raw.githubusercontent.com/DIRACGrid/Pilot/master/Pilot/pilotCommands.py")
  if not out:
    S_OK("pilotCommands.py script successfully download.\n")
  else:
    print "download error.\n"

  out = os.system(
      "wget -P " + basepath
      + " https://raw.githubusercontent.com/DIRACGrid/Pilot/master/Pilot/pilotTools.py")
  if not out:
    S_OK("pilotTools.py script successfully download.\n")
  else:
    print "download error.\n"


def __configurePilot(basepath):
  """
  Configures the pilot.

  """
  out = os.system("python " + basepath + "dirac-pilot.py -S LHCb-Production -l LHCb -C dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server -N ce.debug.ch -Q default -n DIRAC.JobDebugger.cern -M 1 -E LHCbPilot -X LHCbConfigureBasics,LHCbConfigureSite,LHCbConfigureArchitecture,LHCbConfigureCPURequirements -dd")
  if not out:
    directory = os.path.expanduser('~') + os.path.sep
    os.rename(directory + '.dirac.cfg', directory + '.dirac.cfg.old')
    shutil.copyfile(directory + 'pilot.cfg', directory + '.dirac.cfg')
    return S_OK("Pilot successfully configured.")

#   else:
#     some DErrno message


def __runJobLocally(jobID, basepath):
  """
  Runs the job!

  """
  from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
  localJob = LHCbJob(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
  localJob.setInputSandbox(os.getcwd() + "pilot.cfg")
  localJob.setConfigArgs(os.getcwd() + "pilot.cfg")
  os.chdir(basepath)
  localJob.runLocal()


if __name__ == "__main__":
  usedDir = os.path.expanduser('~') + os.path.sep
  try:
    _path = __runSystemDefaults(_jobID)

    __downloadJobDescriptionXML(_jobID, _path)

    __modifyJobDescription(_jobID, _path, _downloadinputdata)

    __downloadPilotScripts(_path)

    __configurePilot(_path)

    __runJobLocally(_jobID, _path)

  finally:
    os.chdir(usedDir)
    os.rename(usedDir + '.dirac.cfg.old', usedDir + '.dirac.cfg')
