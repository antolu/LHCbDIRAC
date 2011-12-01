__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage import DataStorage
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class DataStoragePlotter( BaseReporter ):

  _typeName = "DataStorage"
  _typeKeyFields = [ dF[0] for dF in DataStorage().definitionKeyFields ]
  _noSEtypeKeyFields = [ dF[0] for dF in DataStorage().definitionKeyFields if dF[0] != 'StorageElement' ]
  _noSEGrouping = ( ",".join( "%s" for f in _noSEtypeKeyFields ), _noSEtypeKeyFields )

  ###
  _reportCatalogSpaceName = "LFN size"
  def _reportCatalogSpace( self, reportRequest ):
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'LogicalSize', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                DataStoragePlotter._noSEGrouping,
                                { 'convertToGranularity' : 'sum', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "bytes" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotCatalogSpace( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "LFN space usage grouped by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###

  _reportCatalogFilesName = "LFN files"
  def _reportCatalogFiles( self, reportRequest ):
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'LogicalFiles', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                DataStoragePlotter._noSEGrouping,
                                { 'convertToGranularity' : 'sum', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "files" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotCatalogFiles( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "Number of LFNs by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###
  _reportPhysicalSpaceName = "PFN size"
  def _reportPhysicalSpace( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'PhysicalSize', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "bytes" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotPhysicalSpace( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "PFN space usage by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###
  _reportPhysicalFilesName = "PFN files"
  def _reportPhysicalFiles( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'PhysicalFiles', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "files" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotPhysicalFiles( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "Number of PFNs by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###
