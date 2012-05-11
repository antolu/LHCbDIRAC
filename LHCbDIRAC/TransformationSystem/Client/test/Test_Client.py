import unittest
from mock import Mock

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks

def getSitesForSE( ses ):
  if ses == 'pippo':
    return {'OK':True, 'Value':['Site2', 'Site3']}
  else:
    return {'OK':True, 'Value':['Site3']}


class ClientTestCase( unittest.TestCase ):

  def setUp( self ):
    tc = Mock()
    sc = Mock()
    jmc = Mock()

    self.l_wft = LHCbWorkflowTasks( tc, submissionClient = sc, jobMonitoringClient = jmc )

  def tearDown( self ):
    pass

class TaskManagerSuccess( ClientTestCase ):
  def test_prepareTransformationTasks( self ):
    pass

  def test__handleDestination( self ):
    res = self.l_wft._handleDestination( {'Site':'', 'TargetSE':''} )
    self.assertEqual( res, ['ANY'] )
    res = self.l_wft._handleDestination( {'Site':'ANY', 'TargetSE':''} )
    self.assertEqual( res, ['ANY'] )
    res = self.l_wft._handleDestination( {'TargetSE':'Unknown'} )
    self.assertEqual( res, ['ANY'] )
    res = self.l_wft._handleDestination( {'Site':'Site1, Site2', 'TargetSE':''} )
    self.assertEqual( res, ['Site1', 'Site2'] )
    res = self.l_wft._handleDestination( {'Site':'Site1, Site2', 'TargetSE':'pippo'}, getSitesForSE )
    self.assertEqual( res, ['Site2'] )
    res = self.l_wft._handleDestination( {'Site':'Site1, Site2', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, [] )
    res = self.l_wft._handleDestination( {'Site':'Site1, Site2, Site3', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site3'] )
    res = self.l_wft._handleDestination( {'Site':'ANY', 'TargetSE':'pippo, pluto'}, getSitesForSE )
    self.assertEqual( res, ['Site3'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TaskManagerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
