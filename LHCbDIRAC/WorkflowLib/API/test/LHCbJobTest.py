########################################################################
# $HeadURL$
# File  : TestJob.py
# Author: Stuart Paterson
########################################################################


from WorkflowLib.API.LHCbJob                            import LHCbJob
from DIRAC.Interfaces.API.Dirac                          import Dirac
import unittest,types,time,sys

import os
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()

#from DIRAC.Interfaces.API.DIRAC                            import *
from DIRAC                                                 import S_OK, S_ERROR

#############################################################################

class LHCbJobTests:

  def __init__(self):
    #self.__printInfo()
    pass

  def main(self):
    print 'LHCb Job Tests starting'
    result  = self.basicTest()

  def basicTest(self):
    j = LHCbJob()
    j.setCPUTime(50000)
    j.setSystemConfig('slc4_ia32_gcc34')
    j.setApplication('DaVinci','v19r7','DaVinciv19r7.opts','srmDVTest.log')
#    j.setExecutable('/bin/echo hello')
#    j.setExecutable('/bin/echo helloagain')
#    j.setApplication('Boole','v30r11')
#    j.setExecutable('/bin/echo helloagainforathirdtime')
    j.setOwner('paterson')
    j.setType('user')
    j.setName('MySRMTest')
    #j.setAncestorDepth(1)
    #j.setInputSandbox(['/Users/stuart/dirac/workspace/DIRAC3/DIRAC/Interfaces/API/test/DV.opts'])
  #  j.setOutputSandbox(['std.out','std.err'])
    #j.setInputData(['/lhcb/production/DC06/phys-v2-lumi5/00001680/DST/0000/00001680_00000490_5.dst'])
    j.setInputData(['/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/0000/00001650_00000054_5.dst','/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/0000/00001650_00000055_5.dst'])
   # j.setOutputData(['my.dst','myfile.log'])
    #j.setOption("""ApplicationMgr.EvtMax = -1""")
    j.setDestination('LCG.CERN.ch')
    j.setPlatform('LCG')
  #  j.setSoftwareTags('VO-lhcb-DaVinci-v19r5')
    #j.bootstrap()
   # j._dumpParameters(showType='Parameter')
   # print j._toXML()
    #print j._toJDL()

   # xml = j._toXML()
#    testFile = '/Users/stuart/Desktop/jobDescription.xml'
   # testFile = os.getcwd()+'/jobDescription.xml'
#    if os.path.exists(testFile):
#      os.remove(testFile)
 #   xmlfile = open(testFile,'w')
  #  xmlfile.write(xml)
   # xmlfile.close()

    dirac = Dirac()
    for i in xrange(1):
      print j._toJDL()
      jobid = dirac.submit(j)
      print 'Returned: ',jobid

#    print j.createCode()


  #############################################################################
  #############################################################################

class LHCbJobTestCase(unittest.TestCase):
  """ Base class for the Job test cases
  """
  def test_runJob(self):
    j = LHCbJobTests()
    j.main()

if __name__ == '__main__':
  print 'Starting Unit Test for LHCb Job'
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(LHCbJobTestCase)
  print 'Unit test finished'
 # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GetSystemInfoTestCase))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

