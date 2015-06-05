""" Collection of user jobs for testing purposes
"""

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl',
          'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl', 'LCG.RRCKI.ru']

# Common functions

def baseToAllJobs():

  print "**********************************************************************************************************"
  print "\n Submitting job ", __name__

  J = LHCbJob()
  J.setName( __name__ )
  J.setCPUTime( 17800 )
  return J


def endOfAllJobs( J ):
  result = DiracLHCb().submit( J )
  print __name__, "job submission result:", result
  if result['OK']:
    jobID = int( result['Value'] )
    print __name__, "submitted with job ID:", jobID

  print "**********************************************************************************************************"




# List of jobs


def helloWorldTestT2s():
  
  J = baseToAllJobs()
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setBannedSites( tier1s )
  endOfAllJobs( J )
