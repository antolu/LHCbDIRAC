''' LHCbDIRAC.AccountingSystem.private.Plotters.PopularityPlotter

   PopularityPlotter.__bases__:
     DIRAC.AccountingSystem.private.Plotters.BaseReporter.BaseReporter
  
'''

from DIRAC                                                import S_OK
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

from LHCbDIRAC.AccountingSystem.Client.Types.Popularity import Popularity

__RCSID__ = '$Id$'

class PopularityPlotter( BaseReporter ):
  '''
    PopularityPlotter as extension of BaseReporter
  '''
  
  _typeName          = "Popularity"
  _typeKeyFields     = [ dF[0] for dF in Popularity().definitionKeyFields ]
  _noSEtypeKeyFields = [ dF[0] for dF in Popularity().definitionKeyFields ]
  _noSEGrouping      = ( ", ".join( "%s" for f in _noSEtypeKeyFields ), _noSEtypeKeyFields )

  #.............................................................................
  # data Usage
  
  _reportDataUsageName = "Data Usage"
  def _reportDataUsage( self, reportRequest ):
    '''
      Reports the data usage from the Accounting DB.
    '''
    
    _selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields  = ( _selectString + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "files" )
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
     
    return S_OK( { 'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                  } )

  def _plotDataUsage( self, reportRequest, plotInfo, filename ):
    '''
      Plots the data Usage statistics.
    '''
    
    granularity = plotInfo[ 'granularity' ]
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = {
                 'title'     : "Data Usage grouped by %s" % reportRequest[ 'grouping' ],
                 'starttime' : startEpoch,
                 'endtime'   : endEpoch,
                 'span'      : granularity,
                 'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # normalized data Usage

  _reportNormalizedDataUsageName = "Normalized Data Usage"
  def _reportNormalizedDataUsage( self, reportRequest ):
    '''
      Reports the normalized data usage from the Accounting DB.
    '''
    
    _selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectString + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "files" )
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits 
    
    return S_OK( { 'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                  } )

  def _plotNormalizedDataUsage( self, reportRequest, plotInfo, filename ):
    '''
      Plots the normalized data usage.
    '''
    
    granularity = plotInfo[ 'granularity' ]
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                 'title'     : "Normalized Data Usage grouped by %s" % reportRequest[ 'grouping' ],
                 'starttime' : startEpoch,
                 'endtime'   : endEpoch,
                 'span'      : granularity,
                 'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    
    return self._generateStackedLinePlot( filename, dataDict, metadata )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF