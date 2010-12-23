
from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage import UserStorage
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class UserStoragePlotter( BaseReporter ):

  _typeName = "UserStorage"
  _typeKeyFields = [ dF[0] for dF in UserStorage().definitionKeyFields ]

  ###

  _reportCatalogSpaceName = "Catalog registered space usage"
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

  def _plotCatalogSpace( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Space registered as seen by the catalog by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###

  _reportCatalogFilesName = "Catalog registered files"
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

  def _plotCatalogFiles( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Files registered as seen by the catalog by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###

  _reportPhysicalSpaceName = "Physical registered space usage"
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
    metadata = { 'title' : 'PFN Space registered as seen by the catalog by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###

  _reportPhysicalFilesName = "Physical registered files"
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
    metadata = { 'title' : 'PFN files registered as seen by the catalog by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

