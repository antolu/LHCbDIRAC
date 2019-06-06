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
""" This simply invokes DIRAC APIs for creating 2 jobDescription.xml files,
    one with an application that will end with status 0, and a second with status != 0
"""

#  pylint: disable=wrong-import-position, protected-access

import os
from DIRAC import rootPath

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob


# With a script that returns 0
try:
  scriptSHLocation = find_all('script-OK.sh', rootPath,
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]
except IndexError:  # we are in Jenkins
  scriptSHLocation = find_all('exe-script.py', os.environ['WORKSPACE'],
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList=['LHCbScript'])
jobXMLFile = 'jobDescriptionLHCb-OK.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())

# # With a script that returns 0 - multiple steps
j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList=['LHCbScript', 'CreateDataFile'])
jobXMLFile = 'jobDescriptionLHCb-multiSteps-OK.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())


# # With a script that returns 111
try:
  scriptSHLocation = find_all('script.sh', rootPath,
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]
except IndexError:  # we are in Jenkins
  scriptSHLocation = find_all('script.sh', os.environ['WORKSPACE'],
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList=['LHCbScript'])
jobXMLFile = 'jobDescriptionLHCb-FAIL.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())

# # With a script that returns 111 - multiple steps
j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList=['LHCbScript', 'CreateDataFile'])
jobXMLFile = 'jobDescriptionLHCb-multiSteps-FAIL.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())


# # With a script that returns 1502
try:
  scriptSHLocation = find_all('script-RESC.sh', rootPath,
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]
except IndexError:  # we are in Jenkins
  scriptSHLocation = find_all('script-RESC.sh', os.environ['WORKSPACE'],
                              '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation)

jobXMLFile = 'jobDescriptionLHCb-FAIL1502.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())
