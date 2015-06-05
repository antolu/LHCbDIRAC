""" Collection of user jobs for testing purposes
"""

from TestDIRAC.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb


tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl',
          'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl', 'LCG.RRCKI.ru']

# Common functions

def baseToAllJobs( jName ):

  print "**********************************************************************************************************"
  print "\n Submitting job ", jName

  J = LHCbJob()
  J.setName( jName )
  J.setCPUTime( 17800 )
  return J


def endOfAllJobs( J ):
  result = DiracLHCb().submit( J )
  print "Job submission result:", result
  if result['OK']:
    jobID = int( result['Value'] )
    print "Submitted with job ID:", jobID

  return result

  print "**********************************************************************************************************"




# List of jobs


def helloWorldTestT2s():
  
  J = baseToAllJobs( 'helloWorldTestT2s' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setBannedSites( tier1s )
  return endOfAllJobs( J )

def helloWorldTestCERN():

  J = baseToAllJobs( 'helloWorld-test-CERN' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( 'LCG.CERN.ch' )
  return endOfAllJobs( J )

def helloWorldTestSLC6():

  J = baseToAllJobs( 'helloWorld-test-SLC6' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc6' )
  return endOfAllJobs( J )

def helloWorldTestSLC5():

  J = baseToAllJobs( 'helloWorld-test-SLC5' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc5' )
  return endOfAllJobs( J )

def jobWithOutput():

  J = baseToAllJobs( 'jobWithOutput' )
  J.setInputSandbox( [find_all( 'testFileUpload.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUpload.txt'] )
  return endOfAllJobs( J )

def jobWithOutputAndPrepend():

  J = baseToAllJobs( 'jobWithOutputAndPrepend' )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUploadNewPath.txt'], filePrepend = 'testFilePrepend' )
  return endOfAllJobs( J )

def jobWithOutputAndPrependWithUnderscore():

  J = baseToAllJobs( 'jobWithOutputAndPrependWithUnderscore' )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUpload_NewPath.txt'], filePrepend = 'testFilePrepend' )
  return endOfAllJobs( J )

