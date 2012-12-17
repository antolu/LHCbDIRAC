''' LHCbDIRAC.AccountingSystem.private.Plotters.DataStoragePlotter

   DataStoragePlotter.__bases__:
     DIRAC.AccountingSystem.private.Plotters.BaseReporter.BaseReporter

'''

from DIRAC                                                import S_OK, S_ERROR
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage  import DataStorage

__RCSID__ = "$Id$"

#FIXME: the _reportMethods can be refactored !
#FIXME: the _plotMethods can be refactored !

class DataStoragePlotter( BaseReporter ):
  '''
    DataStoragePlotter as extension of BaseReporter
  '''

  _typeName          = "DataStorage"
  _typeKeyFields     = [ dF[0] for dF in DataStorage().definitionKeyFields ]
  _noSEtypeKeyFields = [ dF[0] for dF in DataStorage().definitionKeyFields if dF[0] != 'StorageElement' ]
  _noSEGrouping      = ( ", ".join( "%s" for f in _noSEtypeKeyFields ), _noSEtypeKeyFields )

  ##############################################################################
  #
  # Catalog Space
  # 

  _reportCatalogSpaceName = "LFN size"
  def _reportCatalogSpace( self, reportRequest ):
    '''
    Reports about the LFN size and the catalog space from the accounting ( grouped
    by StorageElement ).
    '''
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    
    selectFields = ( selectString + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "bytes" )
     
    #3rd variable unused ( maxValue ) 
    baseDataDict, graphDataDict, __, unitName = suitableUnits
     
    return S_OK( { 
                   'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                   } )

  def _plotCatalogSpace( self, reportRequest, plotInfo, filename ):
    '''
    Plots about the LFN size and the catalog space.
    '''
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    dataDict  = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "LFN space usage grouped by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
               }
    
    dataDict = self._fillWithZero( span, startTime, endTime, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  ##############################################################################
  #
  # Catalog Files 
  #

  _reportCatalogFilesName = "LFN files"
  def _reportCatalogFiles( self, reportRequest ):
    '''
    Reports about the LFN files and the catalog files from the accounting ( grouped
    by StorageElement ).
    '''
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectString + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "files" )
    
    #3rd variable unused ( maxValue )
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                   'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                   } )

  def _plotCatalogFiles( self, reportRequest, plotInfo, filename ):
    '''
    Plots about the LFN files and the catalog files.
    '''
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    dataDict  = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "Number of LFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
               }
    
    dataDict = self._fillWithZero( span, startTime, endTime, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  ##############################################################################
  #
  # Physical Space
  # 

  _reportPhysicalSpaceName = "PFN size"
  def _reportPhysicalSpace( self, reportRequest ):
    '''
    Reports about the PFN size and the physical space from the accounting.
    '''
    
    selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectString + ", %s, %s, SUM(%s/%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "bytes" )
    
    #3rd variable unused ( maxValue )
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotPhysicalSpace( self, reportRequest, plotInfo, filename ):
    '''
    Plots about the PFN size and the physical space.
    '''
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    dataDict  = plotInfo[ 'graphDataDict' ]  
    
    metadata = { 
                'title'     : "PFN space usage by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( span, startTime, endTime, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  ##############################################################################
  #
  # Physical Files 
  #

  _reportPhysicalFilesName = "PFN files"
  def _reportPhysicalFiles( self, reportRequest ):
    '''
    Reports about the PFN files and the physical files from the accounting.
    '''
    
    selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectString + ", %s, %s, SUM(%s/%s)",
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
    
    accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, accumMaxValue, "files" )
    
    #3rd variable unused ( maxValue )
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                   'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                  } )

  def _plotPhysicalFiles( self, reportRequest, plotInfo, filename ):
    '''
    Plots about the PFN files and the physical files.
    '''

    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    dataDict  = plotInfo[ 'graphDataDict' ]  
    
    metadata = { 
                'title'     : "Number of PFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startTime,
                'endtime'   : endTime,
                'span'      : span,
                'ylabel'    : plotInfo[ 'unit' ] 
               }
    
    dataDict = self._fillWithZero( span, startTime, endTime, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF