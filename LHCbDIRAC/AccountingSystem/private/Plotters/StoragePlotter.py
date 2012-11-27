''' StoragePlotter

'''

from DIRAC                                                import S_OK, S_ERROR
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

from LHCbDIRAC.AccountingSystem.Client.Types.Storage      import Storage

__RCSID__ = "$Id$"

class StoragePlotter( BaseReporter ):
  '''
    StoragePlotter as extension of BaseReporter
  '''
  
  _typeName      = "Storage"
  _typeKeyFields = [ dF[0] for dF in Storage().definitionKeyFields ]

  ##############################################################################
  #
  # Catalog Space
  #
  
  _reportCatalogSpaceName = "LFN size"
  def _reportCatalogSpace( self, reportRequest ):
    '''
    Reports about LFN size and catalog space from the accounting ( only grouped
    by StorageElement ). 
    '''
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxVal   = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxVal, "bytes" )
    
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
     
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotCatalogSpace( self, reportRequest, plotInfo, filename ):
    '''
    Plots about LFN size and catalog space . 
    '''
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    
    metadata = { 
                'title'     : "LFN space usage by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # Catalog Files
  #

  _reportCatalogFilesName = "LFN files"
  def _reportCatalogFiles( self, reportRequest ):
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxVal   = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxVal, "files" )
    
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName
                  } )

  def _plotCatalogFiles( self, reportRequest, plotInfo, filename ):
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
        
    metadata = { 
                'title'     : "Number of LFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # Physical Space
  #

  _reportPhysicalSpaceName = "PFN size"
  def _reportPhysicalSpace( self, reportRequest ):
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s/%s)",
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
    
    accumMaxVal   = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxVal, "bytes" )
    
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotPhysicalSpace( self, reportRequest, plotInfo, filename ):

    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    
    metadata = { 
                'title'     : "PFN space usage by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # Physical Files
  #

  _reportPhysicalFilesName = "PFN files"
  def _reportPhysicalFiles( self, reportRequest ):
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s/%s)",
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
    
    accumMaxVal   = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxVal, "files" )
    
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotPhysicalFiles( self, reportRequest, plotInfo, filename ):
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
        
    metadata = { 
                'title'     : "Number of PFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # PFN vs LFN File Multiplicity
  #

  _reportPFNvsLFNFileMultiplicityName = "PFN/LFN file ratio"
  def _reportPFNvsLFNFileMultiplicity( self, reportRequest ):
    
    _logicalField  = "LogicalFiles"
    _physicalField = "PhysicalFiles"
    
    return self._multiplicityReport( reportRequest, _logicalField, _physicalField )

  def _plotPFNvsLFNFileMultiplicity( self, reportRequest, plotInfo, filename ):

    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    
    metadata = { 
                'title'     : "Ratio of PFN/LFN files by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # PFN vs LFN Size Multiplicity
  #

  _reportPFNvsLFNSizeMultiplicityName = "PFN/LFN size ratio"
  def _reportPFNvsLFNSizeMultiplicity( self, reportRequest ):
    
    _logicalField  = "LogicalSize"
    _physicalField = "PhysicalSize"
    
    return self._multiplicityReport( reportRequest, _logicalField, _physicalField )

  def _plotPFNvsLFNSizeMultiplicity( self, reportRequest, plotInfo, filename ):

    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    
    metadata = { 
                'title'     : "Ratio of PFN/LFN space used by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    plotInfo[ 'graphDataDict' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'graphDataDict' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  ##############################################################################
  #
  # Helper functions
  #

  def _multiplicityReport( self, reportRequest, logicalField, physicalField ):
    
    #Step 1 get the total LFNs for each bucket
    selectFields = ( "%s, %s, %s, SUM(%s)/SUM(%s)",
                     #[ 'User', 'startTime', 'bucketLength', logicalField, 'entriesInBucket' ]
                     [ 'Directory', 'startTime', 'bucketLength', logicalField, 'entriesInBucket' ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                 reportRequest[ 'endTime' ],
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 ( '%s', [ 'Directory' ] ),
                                 { 'convertToGranularity' : 'average', 'checkNone' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    bucketTotals = self._getBucketTotals( dataDict )
    
    #Step 2 get the total PFNs
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s/%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    physicalField, 'entriesInBucket'
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
    print dataDict
    self.stripDataField( dataDict, 0 )
    
    #Step 3 divide the PFNs by the total amount of LFNs
    finalData = {}
    for k in dataDict:
      for bt in dataDict[ k ]:
        if bt in bucketTotals:
          if k not in finalData:
            finalData[ k ] = {}
          finalData[ k ][ bt ] = dataDict[ k ][ bt ] / bucketTotals[ bt ]
    return S_OK( { 'data' : finalData, 'graphDataDict' : finalData,
                   'granularity' : granularity, 'unit' : 'PFN / LFN' } )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF