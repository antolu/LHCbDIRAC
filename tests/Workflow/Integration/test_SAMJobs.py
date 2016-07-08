# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()
#
# import unittest
#
# from DIRAC import gLogger, gConfig
#
# # from LHCbDIRAC.ResourceStatusSystem.Client.DiracSAM import DiracSAM
#
# from tests.Utilities.IntegrationTest import IntegrationTest
#
# class SAMJobTestCase( IntegrationTest ):
#   """ Base class for the SAMJob test cases
#   """
#   def setUp( self ):
#     super( SAMJobTestCase, self ).setUp()
#
#     self.diracSAM = DiracSAM()
#     self.subLogger = gLogger.getSubLogger( __file__ )
#
#     self.ce = gConfig.getValue( 'LocalSite/GridCE', 'ce201.cern.ch' )
#     self.subLogger.debug( self.ce )
#
# # class SAMSuccess( SAMJobTestCase ):
# #   def test_Integration_SAM( self ):
# #
# #     res = self.diracSAM.submitNewSAMJob( ce = self.ce, runLocal = True )
# #     self.assertTrue( res['OK'] )
#
# if __name__ == '__main__':
#   suite = unittest.defaultTestLoader.loadTestsFromTestCase( SAMJobTestCase )
#   # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SAMSuccess ) )
#   testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )