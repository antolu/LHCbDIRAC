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
result = dirac.submit( j )
print result
