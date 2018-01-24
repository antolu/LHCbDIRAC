""" This simply invokes DIRAC APIs for creating 2 jobDescription.xml files,
    one with an application that will end with status 0, and a second with status != 0
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob


# With a script that returns 0
scriptSHLocation = find_all('script-OK.sh', '..', '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList = ['LHCbScript'])
jobXMLFile = 'jobDescriptionLHCb-OK.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())

# # With a script that returns 0 - multiple steps
j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList = ['LHCbScript', 'CreateDataFile'])
jobXMLFile = 'jobDescriptionLHCb-multiSteps-OK.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())


# # With a script that returns 111
scriptSHLocation = find_all('script.sh', '..', '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList = ['LHCbScript'])
jobXMLFile = 'jobDescriptionLHCb-FAIL.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())

# # With a script that returns 111 - multiple steps
j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation, modulesNameList = ['LHCbScript', 'CreateDataFile'])
jobXMLFile = 'jobDescriptionLHCb-multiSteps-FAIL.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())


# # With a script that returns 1502
scriptSHLocation = find_all('script-RESC.sh', '..', '/DIRAC/WorkloadManagementSystem/JobWrapper')[0]

j = LHCbJob()
j.setExecutable('sh %s' % scriptSHLocation)

jobXMLFile = 'jobDescriptionLHCb-FAIL1502.xml'
with open(jobXMLFile, 'w+') as fd:
  fd.write(j._toXML())
