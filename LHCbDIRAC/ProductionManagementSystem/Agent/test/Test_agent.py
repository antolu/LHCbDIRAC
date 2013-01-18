import mock
if mock.__version__ < '1.0.1':
  print 'Invalid Mock version %s !' % mock.__version__
  import sys
  sys.exit( 0 )
  
import unittest2
#from DIRAC.ResourceStatusSystem.Agent.mock.AgentModule import AgentModule as mockAgentModule
from DIRAC import gLogger

class AgentTestCase( unittest2.TestCase ):
  """ Base class for the Agent test cases
  """
  def setUp( self ):

    #import DIRAC.Core.Base.AgentModule as moduleMocked
    #moduleMocked.AgentModule = mockAgentModule
    agentModule = mock.patch( 'DIRAC.Core.Base.AgentModule', autospec = True )
    agentModule.start()

    from LHCbDIRAC.ProductionManagementSystem.Agent.ProductionStatusAgent import ProductionStatusAgent
    self.psa = ProductionStatusAgent( '', '' )
    self.psa.log = gLogger

  def tearDown( self ):
    
    # Stop patchers
    mock.patch.stopall()

#############################################################################
# ProductionStatusAgent.py
#############################################################################

class ProductionStatusSuccess( AgentTestCase ):

  def test__evaluateProgress( self ):
    prodReqSummary = {
                      8733: {'bkTotal': 1090805L, 'master': 0, 'reqTotal': 2000000L},
                      8744: {'bkTotal': 2090805L, 'master': 0, 'reqTotal': 2000000L},
                      9050: {'bkTotal': 1600993L, 'master': 9048, 'reqTotal': 500000L},
                      9237: {'bkTotal': 0L, 'master': 9235, 'reqTotal': 500000L}
                      }

    progressSummary = {
                        8733L: {20940L: {'Events': 33916336L, 'Used': 0},
                                20941L: {'Events': 1245467L, 'Used': 0},
                                20942L: {'Events': 1090805L, 'Used': 1}},
                        8744L: {20140L: {'Events': 33916336L, 'Used': 0},
                                20141L: {'Events': 1245467L, 'Used': 0},
                                20142L: {'Events': 2090805L, 'Used': 1}},
                        9050L: {21034L: {'Events': 1616993L, 'Used': 0},
                                21035L: {'Events': 1600993L, 'Used': 1}},
                        9237L: {21080L: {'Events': 0L, 'Used': 0},
                                21081L: {'Events': 0L, 'Used': 1}}
                      }

    doneAndUsed, doneAndNotUsed, notDoneAndUsed, notDoneAndNotUsed = self.psa._evaluateProgress( prodReqSummary,
                                                                                                 progressSummary )
    self.assertEqual( doneAndUsed, {21035L:9050, 20142L:8744} )
    self.assertEqual( doneAndNotUsed, { 21034L:9050, 20140L:8744, 20141L:8744} )
    self.assertEqual( notDoneAndUsed, {20942L:8733} )
    self.assertEqual( notDoneAndNotUsed, {20940L:8733, 20941L:8733} )

  def test__reqsMap( self ):
    prodReqSummary = {
                      8733: {'bkTotal': 1090805L, 'master': 0, 'reqTotal': 2000000L},
                      8744: {'bkTotal': 2090805L, 'master': 0, 'reqTotal': 2000000L},
                      9050: {'bkTotal': 1600993L, 'master': 9048, 'reqTotal': 500000L},
                      9051: {'bkTotal': 1600993L, 'master': 9048, 'reqTotal': 500000L},
                      9237: {'bkTotal': 0L, 'master': 9235, 'reqTotal': 500000L}
                      }

    progressSummary = {
                        8733L: {20940L: {'Events': 33916336L, 'Used': 0},
                                20941L: {'Events': 1245467L, 'Used': 0},
                                20942L: {'Events': 1090805L, 'Used': 1}},
                        8744L: {20140L: {'Events': 33916336L, 'Used': 0},
                                20141L: {'Events': 1245467L, 'Used': 0},
                                20142L: {'Events': 2090805L, 'Used': 1}},
                        9050L: {21034L: {'Events': 1616993L, 'Used': 0},
                                21035L: {'Events': 1600993L, 'Used': 1}},
                        9051L: {21036L: {'Events': 1616993L, 'Used': 0},
                                21037L: {'Events': 1600993L, 'Used': 1}},
                        9237L: {21080L: {'Events': 0L, 'Used': 0},
                                21081L: {'Events': 0L, 'Used': 1}}
                      }

    res = self.psa._getReqsMap( prodReqSummary, progressSummary )
    self.assertEqual( res, {8733:{8733:[20940, 20941, 20942]},
                            8744:{8744:[20140, 20141, 20142]},
                            9048:{9050:[21034, 21035], 9051:[21036, 21037]},
                            9235:{9237:[21080, 21081]}} )


#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest2.defaultTestLoader.loadTestsFromTestCase( AgentTestCase )
  suite.addTest( unittest2.defaultTestLoader.loadTestsFromTestCase( ProductionStatusSuccess ) )
  testResult = unittest2.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
