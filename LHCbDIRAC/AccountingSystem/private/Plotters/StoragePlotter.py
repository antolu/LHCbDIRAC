''' LHCbDIRAC.AccountingSystem.private.Plotters.StoragePlotter

   StoragePlotter.__bases__:
     DIRAC.AccountingSystem.private.Plotters.BaseReporter.BaseReporter

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

  #.............................................................................
  # catalog Space
  
  _reportCatalogSpaceName = "LFN size"
  def _reportCatalogSpace( self, reportRequest ):
    '''
    Reports about LFN size and catalog space from the accounting ( only grouped
    by StorageElement ).
    
    :param reportRequest: <dict>
      { 'grouping'       : 'Directory',
        'groupingFields' : ( '%s', [ 'Directory' ] ),
        'startTime'      : 1355663249.0,
        'endTime'        : 1355749690.0,
        'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] } 
      }
      
    returns S_OK / S_ERROR
      { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.9353885242469104, 
                                             1355702400L : 4.8438444870748203 }, 
                            '/lhcb/LHCb' : { 1355616000L : 3.93538852424691, 
                                             1355702400L : 3.8438444870748198 }
                           }, 
        'data'          : { '/lhcb/data' : { 1355616000L : 4935388.5242469106, 
                                             1355702400L : 4843844.4870748203 }, 
                            '/lhcb/LHCb' : { 1355616000L : 3935388.5242469101, 
                                             1355702400L : 3843844.4870748199 }
                           }, 
        'unit'          : 'TB', 
        'granularity'   : 86400 
       }    
    '''
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    selectField  = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectField + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
     
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
    
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "LFN space usage by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # catalog Files

  _reportCatalogFilesName = "LFN files"
  def _reportCatalogFiles( self, reportRequest ):
    
    if reportRequest[ 'grouping' ] == "StorageElement":
      return S_ERROR( "Grouping by storage element when requesting lfn info makes no sense" )
    
    selectField  = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectField + ", %s, %s, SUM(%s)/SUM(%s)",
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
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName
                  } )

  def _plotCatalogFiles( self, reportRequest, plotInfo, filename ):
    
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
        
    metadata = { 
                'title'     : "Number of LFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # Physical Space

  _reportPhysicalSpaceName = "PFN size"
  def _reportPhysicalSpace( self, reportRequest ):
    
    selectField  = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectField + ", %s, %s, SUM(%s/%s)",
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
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotPhysicalSpace( self, reportRequest, plotInfo, filename ):

    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "PFN space usage by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # physical Files

  _reportPhysicalFilesName = "PFN files"
  def _reportPhysicalFiles( self, reportRequest ):
    
    selectField  = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( selectField + ", %s, %s, SUM(%s/%s)",
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
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def _plotPhysicalFiles( self, reportRequest, plotInfo, filename ):
    
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
        
    metadata = { 
                'title'     : "Number of PFNs by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # PFN vs LFN File Multiplicity

  _reportPFNvsLFNFileMultiplicityName = "PFN/LFN file ratio"
  def _reportPFNvsLFNFileMultiplicity( self, reportRequest ):
    
    logicalField  = "LogicalFiles"
    physicalField = "PhysicalFiles"
    
    return self._multiplicityReport( reportRequest, logicalField, physicalField )

  def _plotPFNvsLFNFileMultiplicity( self, reportRequest, plotInfo, filename ):

    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "Ratio of PFN/LFN files by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # PFN vs LFN Size Multiplicity

  _reportPFNvsLFNSizeMultiplicityName = "PFN/LFN size ratio"
  def _reportPFNvsLFNSizeMultiplicity( self, reportRequest ):
    
    logicalField  = "LogicalSize"
    physicalField = "PhysicalSize"
    
    return self._multiplicityReport( reportRequest, logicalField, physicalField )

  def _plotPFNvsLFNSizeMultiplicity( self, reportRequest, plotInfo, filename ):

    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = { 
                'title'     : "Ratio of PFN/LFN space used by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )

  #.............................................................................
  # helper methods

  def _multiplicityReport( self, reportRequest, logicalField, physicalField ):
    
    #Step 1 get the total LFNs for each bucket
    selectFields = ( "%s, %s, %s, SUM(%s)/SUM(%s)",
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
    
    #2nd element ( granularity ) is unused
    dataDict, __ = retVal[ 'Value' ]
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
    #FIXME: remove this print !
    print dataDict
    self.stripDataField( dataDict, 0 )
    
    #Step 3 divide the PFNs by the total amount of LFNs
    finalData = {}

    #FIXME: TO BE replaced by a faster implementation ( see below )
    for k in dataDict:
      for bt in dataDict[ k ]:
        if bt in bucketTotals:
          if k not in finalData:
            finalData[ k ] = {}
          finalData[ k ][ bt ] = dataDict[ k ][ bt ] / bucketTotals[ bt ]

#    for key, bucketTotal in dataDict.iteritems():
#      for bt in bucketTotal.itervalues():
#        if bt in bucketTotals:
#          if key not in finalData:
#            finalData[ key ] = {}
#          finalData[ key ][ bt ] = bucketTotal[ bt ] / bucketTotals[ bt ]

    return S_OK( { 'data'          : finalData, 
                   'graphDataDict' : finalData,
                   'granularity'   : granularity, 
                   'unit'          : 'PFN / LFN' 
                  } )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF