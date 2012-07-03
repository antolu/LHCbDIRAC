__RCSID__ = "$Id: $"

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.AccountingSystem.Client.Types.Popularity import Popularity
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.Core.Utilities import Time

class PopularityPlotter( BaseReporter ):

  _typeName = "Popularity"
  _typeKeyFields = [ dF[0] for dF in Popularity().definitionKeyFields ]
  #_noSEtypeKeyFields = [ dF[0] for dF in Popularity().definitionKeyFields if dF[0] != 'StorageElement' ]
  _noSEtypeKeyFields = [ dF[0] for dF in Popularity().definitionKeyFields ]
  _noSEGrouping = ( ", ".join( "%s" for f in _noSEtypeKeyFields ), _noSEtypeKeyFields )

  ###
  _reportDataUsageName = "Data Usage"
  def _reportDataUsage( self, reportRequest ):
    #if reportRequest[ 'grouping' ] == "StorageElement":
    #  return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'Usage', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                PopularityPlotter._noSEGrouping,
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

  def _plotDataUsage( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "Data Usage grouped by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ###

  _reportNormalizedDataUsageName = "Normalized Data Usage"
  def _reportNormalizedDataUsage( self, reportRequest ):
    #if reportRequest[ 'grouping' ] == "StorageElement":
    #  return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)/SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'NormalizedUsage', 'entriesInBucket'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                PopularityPlotter._noSEGrouping,
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

  def _plotNormalizedDataUsage( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : "Normalized Data Usage grouped by %s" % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

