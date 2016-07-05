""" doc here
"""

import unittest

import math
import operator
from PIL import Image

from LHCbDIRAC.AccountingSystem.private.Plotters.DataStoragePlotter import DataStoragePlotter
from LHCbDIRAC.AccountingSystem.private.Plotters.StoragePlotter import StoragePlotter

def compare( file1Path, file2Path ):
  '''
    Function used to compare two plots

    returns 0.0 if both are identical
  '''
  # Crops image to remove the "Generated on xxxx UTC" string
  image1 = Image.open( file1Path ).crop( ( 0, 0, 800, 570 ) )
  image2 = Image.open( file2Path ).crop( ( 0, 0, 800, 570 ) )
  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt( reduce( operator.add,
                           map( lambda a, b: ( a - b ) ** 2, h1, h2 ) ) / len( h1 ) )
  return rms

# ...............................................................................


class PlotterTestCase( unittest.TestCase ):
  '''
  '''

  moduleTested = None
  classsTested = None


  def setUp( self ):
    '''
      Setup the test case
    '''

    pass

#    import LHCbDIRAC.AccountingSystem.private.Plotters.PopularityPlotter as moduleTested
#
#     self.moduleTested = self.mockModuleTested( moduleTested )
#     self.classsTested = self.moduleTested.PopularityPlotter

#   def tearDown( self ):
#     '''
#       Tear down the test case
#     '''
#
#     del self.moduleTested
#     del self.classsTested

# ...............................................................................

class DataStoragePlotterUnitTest( PlotterTestCase ):

  def test_plotCatalogSpace( self ):
    ''' test the method "_plotCatalogSpace"
    '''

    plotName = 'DataStoragePlotter_plotCatalogSpace'
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'EventType' : 'Full stream' }
                    }
    plotInfo = { 'graphDataDict' : { 'Full stream' : { 1355616000L: 4.9003546130956819,
                                                       1355702400L: 4.9050379437065059 }
                                   },
                 'data'          : { 'Full stream' : { 1355616000L: 4900354613.0956821,
                                                       1355702400L: 4905037943.7065058 }
                                   },
                 'unit'          : 'PB',
                 'granularity'   : 86400}

    obj = DataStoragePlotter( None, None )
    res = obj._plotCatalogSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotCatalogFiles( self ):
    ''' test the method "_plotCatalogFiles"
    '''

    plotName = 'DataStoragePlotter_plotCatalogFiles'
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'EventType' : 'Full stream' }
                    }
    plotInfo = { 'graphDataDict' : { 'Full stream' : { 1355616000L : 420.47885754501203,
                                                       1355702400L : 380.35170637810842 }
                                   },
                 'data'          : { 'Full stream' : { 1355616000L : 420.47885754501203,
                                                       1355702400L : 380.35170637810842 }
                                   },
                 'unit'          : 'files',
                 'granularity'   : 86400}

    obj = DataStoragePlotter( None, None )
    res = obj._plotCatalogFiles( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotPhysicalSpace( self ):
    ''' test the method "_plotPhysicalSpace"
    '''

    plotName = 'DataStoragePlotter_plotPhysicalSpace'
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'EventType' : 'Full stream' }}
    plotInfo = { 'graphDataDict' : { 'Full stream' : { 1355616000L : 14.754501202,
                                                       1355702400L : 15.237810842 }
                                   },
                 'data'          : { 'Full stream' : { 1355616000L : 14.754501202,
                                                       1355702400L : 15.237810842 }
                                   },
                 'unit'          : 'MB',
                 'granularity'   : 86400}

    obj = DataStoragePlotter( None, None )
    res = obj._plotPhysicalSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotPhysicalFiles( self ):
    ''' test the method "_plotPhysicalFiles"
    '''

    plotName = 'DataStoragePlotter_plotPhysicalFiles'
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'EventType' : 'Full stream' }
                    }
    plotInfo = { 'graphDataDict' : { 'Full stream' : { 1355616000L : 42.47885754501202,
                                                       1355702400L : 38.35170637810842 }},
                 'data'          : { 'Full stream' : { 1355616000L : 42.47885754501202,
                                                       1355702400L : 38.35170637810842 }},
                 'unit'          : 'files',
                 'granularity'   : 86400}

    obj = DataStoragePlotter( None, None )
    res = obj._plotPhysicalFiles( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )


class StoragePlotterUnitTest( PlotterTestCase ):

  def test_plotCatalogSpace( self ):
    ''' test the method "_plotCatalogSpace"
    '''

    plotName = 'StoragePlotter_plotCatalogSpace'
    reportRequest = { 'grouping'       : 'Directory',
                      'groupingFields' : ( '%s', [ 'Directory' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] }}
    plotInfo = { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.9353885242469104,
                                                      1355702400L : 4.8438444870748203 },
                                      '/lhcb/LHCb' : { 1355616000L : 3.93538852424691,
                                                       1355702400L : 3.8438444870748198 }},
                  'data'          : { '/lhcb/data' : { 1355616000L : 4935388.5242469106,
                                                       1355702400L : 4843844.4870748203 },
                                      '/lhcb/LHCb' : { 1355616000L : 3935388.5242469101,
                                                       1355702400L : 3843844.4870748199 }},
                  'unit'          : 'TB',
                  'granularity'   : 86400}

    obj = StoragePlotter( None, None )
    res = obj._plotCatalogSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotCatalogFiles( self ):
    ''' test the method "_plotCatalogFiles"
    '''

    plotName = 'StoragePlotter_plotCatalogFiles'
    reportRequest = { 'grouping'       : 'Directory',
                      'groupingFields' : ( '%s', [ 'Directory' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] } }
    plotInfo = { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4935388.5242469106,
                                                      1355702400L : 4843844.4870748203 },
                                     '/lhcb/LHCb' : { 1355616000L : 3935388.5242469101,
                                                      1355702400L : 3843844.4870748199 }},
                 'data'          : { '/lhcb/data' : { 1355616000L : 4935388524246.9102,
                                                      1355702400L : 4843844487074.8203 },
                                     '/lhcb/LHCb' : { 1355616000L : 3935388524246.9102,
                                                      1355702400L : 3843844487074.8198 }},
                 'unit'          : 'Mfiles',
                 'granularity'   : 86400}

    obj = StoragePlotter( None, None )
    res = obj._plotCatalogFiles( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotPhysicalSpace( self ):
    ''' test the method "_plotPhysicalSpace"
    '''

    plotName = 'StoragePlotter_plotPhysicalSpace'
    reportRequest = { 'grouping'       : 'StorageElement',
                      'groupingFields' : ( '%s', [ 'StorageElement' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'StorageElement' : [ 'CERN-ARCHIVE', 'CERN-DST' ] }}
    plotInfo = { 'graphDataDict' : { 'CERN-ARCHIVE' : { 1355616000L : 2.34455676781291,
                                                        1355702400L : 2.5445567678129102 },
                                     'CERN-DST'     : { 1355616000L : 0.34455676781290995,
                                                        1355702400L : 0.54455676781290996 }},
                 'data'          : { 'CERN-ARCHIVE' : { 1355616000L : 2344556.76781291,
                                                        1355702400L : 2544556.76781291 },
                                     'CERN-DST'     : { 1355616000L : 344556.76781290997,
                                                        1355702400L : 544556.76781291002 }},
                 'unit'          : 'TB',
                 'granularity'   : 86400}

    obj = StoragePlotter( None, None )
    res = obj._plotPhysicalSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_plotPhysicalFiles( self ):
    ''' test the method "_plotPhysicalFiles"
    '''

    plotName = 'StoragePlotter_plotPhysicalFiles'
    reportRequest = { 'grouping'       : 'StorageElement',
                      'groupingFields' : ( '%s', [ 'StorageElement' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'StorageElement' : [ 'CERN-ARCHIVE', 'CERN-DST', 'CERN-BUFFER' ] }}
    plotInfo = { 'graphDataDict' : { 'CERN-BUFFER'  : { 1355616000L : 250.65890999999999,
                                                        1355702400L : 261.65890999999999 },
                                     'CERN-ARCHIVE' : { 1355616000L : 412.65890999999999,
                                                        1355702400L : 413.65890999999999 },
                                     'CERN-DST'     : { 1355616000L : 186.65890999999999,
                                                        1355702400L : 187.65890999999999 }},
                 'data'          : { 'CERN-BUFFER'  : { 1355616000L : 250658.91,
                                                        1355702400L : 261658.91 },
                                     'CERN-ARCHIVE' : { 1355616000L : 412658.90999999997,
                                                        1355702400L : 413658.90999999997 },
                                     'CERN-DST'     : { 1355616000L : 186658.91,
                                                        1355702400L : 187658.91 }},
                 'unit'          : 'kfiles',
                 'granularity'   : 86400}

    obj = StoragePlotter( None, None )
    res = obj._plotPhysicalSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )

    res = compare( '%s.png' % plotName, 'LHCbDIRAC/tests/Integration/AccountingSystem/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )
