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
# dirac job created by ganga
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
j = LHCbJob()
dirac = DiracLHCb()

# default commands added by ganga
j.setName( "helloWorld-test" )
j.setInputSandbox( ['/afs/cern.ch/user/f/fstagni/userJobs/_inputHello.tgz', '/afs/cern.ch/user/f/fstagni/userJobs/hello-script.py'] )

j.setExecutable( "exe-script.py", "", "Ganga_Executable.log" )

# <-- user settings
j.setCPUTime( 172800 )
try:
  tier1s = DMSHelpers().getTiers( tier = ( 0, 1 ) )
except AttributeError:
  tier1s = ['LCG.CERN.cern', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
            'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.RRCKI.ru', 'LCG.SARA.nl']
j.setBannedSites( tier1s )
# user settings -->


# print j.workflow

# submit the job to dirac
result = dirac.submitJob(j)
print result
