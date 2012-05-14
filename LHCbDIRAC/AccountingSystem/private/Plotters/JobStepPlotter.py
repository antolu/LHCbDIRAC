# $HeadURL $
''' JobStepPlotter

'''

from DIRAC import S_OK
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

from LHCbDIRAC.AccountingSystem.Client.Types.JobStep      import JobStep

__RCSID__ = "$Id: "

class JobStepPlotter( BaseReporter ):
  '''
    JobStepPlotter as an extension of BaseReporter
  '''

  _typeName      = "JobStep"
  _typeKeyFields = [ dF[0] for dF in JobStep().definitionKeyFields ]

################################################################################
## CPU Efficiency

  _reportCPUEfficiencyName = "CPU efficiency"
  def _reportCPUEfficiency( self, reportRequest ):
    
    _selectField = self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ])
    selectFields = ( _selectField + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'CPUTime', 'ExecTime'
                                   ]
                   )

    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                 reportRequest[ 'endTime' ],
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 { 'checkNone'                   : True,
                                   'convertToGranularity'        : 'sum',
                                   'calculateProportionalGauges' : False,
                                   'consolidationFunction' : self._efficiencyConsolidation })
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    if len( dataDict ) > 1:
      #Get the total for the plot
      selectFields = ( "'Total', %s, %s, SUM(%s),SUM(%s)",
                       [ 'startTime', 'bucketLength',
                         'CPUTime', 'ExecTime'
                       ]
                     )

      retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                   reportRequest[ 'endTime' ],
                                   selectFields,
                                   reportRequest[ 'condDict' ],
                                   reportRequest[ 'groupingFields' ],
                                   { 'scheckNone'                  : True,
                                     'convertToGranularity'        : 'sum',
                                     'calculateProportionalGauges' : False,
                                     'consolidationFunction' : self._efficiencyConsolidation  })
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
        
    return S_OK({ 
                 'data'        : dataDict, 
                 'granularity' : granularity 
                 })

  def _plotCPUEfficiency( self, reportRequest, plotInfo, filename ):
 
    metadata = { 
                'title'     : 'CPU efficiency by %s' % reportRequest[ 'grouping' ],
                'starttime' : reportRequest[ 'startTime' ],
                'endtime'   : reportRequest[ 'endTime' ],
                'span'      : plotInfo[ 'granularity' ] 
                }
    
    return self._generateQualityPlot( filename, plotInfo[ 'data' ], metadata )

################################################################################
## CPU Usage

  _reportCPUUsageName = "CPU time"
  def _reportCPUUsage( self, reportRequest ):
    
    _field = 'CPUTime'
    _unit  = 'time'
    
    return self.__reportNormPlot( reportRequest, _field, _unit )

  def _plotCPUUsage( self, reportRequest, plotInfo, filename ):
    
    _title = 'CPU usage'
    
    return self.__plotNormPlot( reportRequest, plotInfo, filename, _title )

################################################################################
## Pie CPU Time

  _reportPieCPUTimeName = 'Pie plot of CPU time'
  def _reportPieCPUTime( self, reportRequest ): 
    
    return self.__reportPiePlot( reportRequest, 'CPUTime' )

  def _plotPieCPUTime( self, reportRequest, plotInfo, filename ):
    
    _title = 'CPU time' 
    _label = 'CPUTime'
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Cumulative CPU Time

  _reportCumulativeCPUTimeName = "Cumulative CPU time"
  def _reportCumulativeCPUTime( self, reportRequest ):
    
    _field = 'CPUTime'
    _unit  = 'time'
    
    return self.__reportCumulativePlot( reportRequest, _field, _unit  )

  def _plotCumulativeCPUTime( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative CPU time'
    
    return self.__plotCumulative( reportRequest, plotInfo, filename, _title )

################################################################################
## Norm CPU Time

  _reportNormCPUTimeName = "NormCPU time"
  def _reportNormCPUTime( self, reportRequest ):
    
    _field = 'NormCPUTime'
    _unit  = 'time'
    
    return self.__reportNormPlot( reportRequest, _field, _unit )

  def _plotNormCPUTime( self, reportRequest, plotInfo, filename ):
    
    _title = 'Normalized CPU'
    
    return self.__plotNormPlot( reportRequest, plotInfo, filename, _title )

################################################################################
## Cumulative Norm CPU Time

  _reportCumulativeNormCPUTimeName = "Cumulative normalized CPU time"
  def _reportCumulativeNormCPUTime( self, reportRequest ):
    
    _field = 'NormCPUTime'
    _unit  = 'time'
    
    return self.__reportCumulativePlot( reportRequest, _field, _unit )

  def _plotCumulativeNormCPUTime( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative Normalized CPU time'
    
    return self.__plotCumulative( reportRequest, plotInfo, filename, _title )

################################################################################
## Pie Norm CPU Time

  _reportPieNormCPUTimeName = 'Pie plot of NormCPU time'
  def _reportPieNormCPUTime( self, reportRequest ):
        
    return self.__reportPiePlot( reportRequest, 'NormCPUTime' )
  
  def _plotPieNormCPUTime( self, reportRequest, plotInfo, filename ):
    
    _title = 'Average Normalized CPU time'
    _label = 'NormCPUTime'
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Input Data

  _reportInputDataName = "Input Data"
  def _reportInputData( self, reportRequest ):
    
    _field = 'InputData'
    _unit  = 'files' 
        
    return self.__reportNormPlot( reportRequest, _field, _unit )

  def _plotInputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Input data'
    
    return self.__plotNormPlot( reportRequest, plotInfo, filename, _title )

################################################################################
## Cumulative Input Data

  _reportCumulativeInputDataName = "Cumulative Input Data"
  def _reportCumulativeInputData( self, reportRequest ):
    
    _field = 'InputData'
    _unit  = 'files'
    
    return self.__reportCumulativePlot( reportRequest, _field, _unit )

  def _plotCumulativeInputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative Input Data'
    
    return self.__plotCumulative( reportRequest, plotInfo, filename, _title )

################################################################################
## Pie Input Data

  _reportPieInputDataName = 'Pie plot of Input Data'
  def _reportPieInputData( self, reportRequest ):
    
    return self.__reportPiePlot( reportRequest, 'InputData' )

  def _plotPieInputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Pie plot of Input data'
    _label = 'InputData'
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Output Data

  _reportOutputDataName = "Output Data"
  def _reportOutputData( self, reportRequest ):
    
    _field = 'OutputData'
    _units = 'files'
    
    return self.__reportNormPlot( reportRequest, _field, _units )

  def _plotOutputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Output data'
    
    return self.__plotNormPlot( reportRequest, plotInfo, filename, _title )

################################################################################
## Cumulative Output Data

  _reportCumulativeOutputDataName = "Cumulative OutputData"
  def _reportCumulativeOutputData( self, reportRequest ):
    
    _field = 'OutputData'
    _unit  = 'files'
    
    return self.__reportCumulativePlot( reportRequest, _field, _unit )

  def _plotCumulativeOutputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative Output Data'
    
    return self.__plotCumulative( reportRequest, plotInfo, filename, _title )

################################################################################
## Pie Output Data

  _reportPieOutputDataName = 'Pie plot of Output Data'
  def _reportPieOutputData( self, reportRequest ):
    
    return self.__reportPiePlot( reportRequest, 'OutputData' )

  def _plotPieOutputData( self, reportRequest, plotInfo, filename ):
    
    _title = 'Pie plot of  Output data'
    _label = 'OutputData'
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Input Events

  _reportInputEventsName = "Input Events"
  def _reportInputEvents( self, reportRequest ):
    return self.__reportNumberOfField( reportRequest, 'InputEvents' )

  def _plotInputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'InputEvents'
    _label = 'Input Events'
    
    return self.__plotNumberOfField( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Cumulative Input Events

  _reportCumulativeInputEventsName = "Cumulative Input Events"
  def _reportCumulativeInputEvents( self, reportRequest ):
    return self.__reportCumulativeNumberOfField( reportRequest, 'InputEvents' )

  def _plotCumulativeInputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative Input Events '
    _label = 'Input Events'  
    
    return self.__plotCumulativeNumberOfField( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Pie Input Events

  _reportPieInputEventsName = 'Pie plot of Input Events'
  def _reportPieInputEvents( self, reportRequest ):
    return self.__reportPiePlot( reportRequest, 'InputEvents' )

  def _plotPieInputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'Pie plot of Input Events'
    _label = 'InputEvents'
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Output Events

  _reportOutputEventsName = "Output Events"
  def _reportOutputEvents( self, reportRequest ):
    return self.__reportNumberOfField( reportRequest, 'OutputEvents' )

  def _plotOutputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'OutputEvents'
    _label = 'Output Events'
    
    return self.__plotNumberOfField( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Cumulative Output Events

  _reportCumulativeOutputEventsName = "Cumulative Output Events"
  def _reportCumulativeOutputEvents( self, reportRequest ):
    return self.__reportCumulativeNumberOfField( reportRequest, 'OutputEvents' )

  def _plotCumulativeOutputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'Cumulative Output Events '
    _label = 'Output Events'
    
    return self.__plotCumulativeNumberOfField( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Pie Output Events

  _reportPieOutputEventsName = 'Pie plot of Output Events'
  def _reportPieOutputEvents( self, reportRequest ):
    return self.__reportPiePlot( reportRequest, 'OutputEvents' )

  def _plotPieOutputEvents( self, reportRequest, plotInfo, filename ):
    
    _title = 'Pie plot of Output Events'
    _label = 'OutputEvents' 
    
    return self.__plotPie( reportRequest, plotInfo, filename, _title, _label )

################################################################################
## Input Events Per Output Events

  _reportInputEventsPerOutputEventsName = 'Input/Output Events'
  def _reportInputEventsPerOutputEvents( self, reportRequest ):
    return self.__report2D( reportRequest, 'InputEvents', 'OutputEvents' )

  def _plotInputEventsPerOutputEvents( self, reportRequest, plotInfo, filename ):
    return self.__plot2D( reportRequest, plotInfo, filename, "Input/Output Events" )

################################################################################
## CPU Time Per Output Events

  _reportCPUTimePerOutputEventsName = 'CPUTime/Output Events'
  def _reportCPUTimePerOutputEvents( self, reportRequest ):
    return self.__report2D( reportRequest, 'CPUTime', 'OutputEvents' )

  def _plotCPUTimePerOutputEvents( self, reportRequest, plotInfo, filename ):
    return self.__plot2D( reportRequest, plotInfo, filename, "CPUTime/Output Events" )

################################################################################
## CPU Time Per Input Events

  _reportCPUTimePerInputEventsName = 'CPUTime/Input Events'
  def _reportCPUTimePerInputEvents( self, reportRequest ):
    return self.__report2D( reportRequest, 'CPUTime', 'InputEvents' )

  def _plotCPUTimePerInputEvents( self, reportRequest, plotInfo, filename ):
    return self.__plot2D( reportRequest, plotInfo, filename, "CPUTime/Input Events" )

################################################################################
## HELPER methods

  def __reportNormPlot( self, reportRequest, field, unit ):
    
    
    _selectField = self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ])
    selectFields = ( _selectField + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    field
                                   ]
                   )
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    
    retVal = self._getTimedData( startTime,
                                 endTime,
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 {})
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    
    __accumMaxVal = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableRateUnit( dataDict, __accumMaxVal, unit )
    
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
     
    return S_OK({ 
                 'data'          : baseDataDict, 
                 'graphDataDict' : graphDataDict,
                 'granularity'   : granularity, 
                 'unit'          : unitName 
                 })

  def __plotNormPlot( self, reportRequest, plotInfo, filename, title ):
    
    metadata = { 
                'title'     : '%s by %s' % ( title, reportRequest[ 'grouping' ] ),
                'starttime' : reportRequest[ 'startTime' ],
                'endtime'   : reportRequest[ 'endTime' ],
                'span'      : plotInfo[ 'granularity' ],
                'ylabel'    : plotInfo[ 'unit' ] 
                }
    
    return self._generateStackedLinePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  def __reportPiePlot(self, reportRequest, field):
    #selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", SUM(%s/%s)",
    #                 reportRequest[ 'groupingFields' ][1] + [ field, 'entriesInBucket'
    #                               ]
    #               )
    
    _selectField = self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ])
    selectFields = ( _selectField + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ field ] )
    
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                   reportRequest[ 'endTime' ],
                                   selectFields,
                                   reportRequest[ 'condDict' ],
                                   reportRequest[ 'groupingFields' ],
                                   {})
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    #bins = self._getBins(self._typeName, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ])
    #numBins = len(bins)
    #for key in dataDict:
    #  dataDict[ key ] = float(dataDict[ key ] / numBins)
    return S_OK({ 'data' : dataDict  })

  def __plotPie( self, reportRequest, plotInfo, filename, title, label ):
    metadata = { 
                'title'     : '%s by %s' % ( title, reportRequest[ 'grouping' ] ),
                'ylabel'    : label,
                'starttime' : reportRequest[ 'startTime' ],
                'endtime'   : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

  def __reportCumulativePlot( self, reportRequest, field, unit ):
    
    _selectField = self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ])
    selectFields = ( _selectField + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', field ]
                   )
    
    starTime = reportRequest[ 'startTime' ]
    endTime  = reportRequest[ 'endTime' ]
    
    retVal = self._getTimedData( starTime,
                                 endTime,
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 {})
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    dataDict = self._accumulate( granularity, startTime, endTime, dataDict )
    
    __accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits = self._findSuitableUnit( dataDict, __accumMaxValue, unit )
        
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
     
    return S_OK({ 
                 'data'          : baseDataDict, 
                 'graphDataDict' : graphDataDict,
                 'granularity'   : granularity, 
                 'unit'          : unitName 
                 })

  def __plotCumulative( self, reportRequest, plotInfo, filename, title ):
    
    metadata = { 
                'title'       : '%s by %s' % (title, reportRequest[ 'grouping' ]),
                'starttime'   : reportRequest[ 'startTime' ],
                'endtime'     : reportRequest[ 'endTime' ],
                'span'        : plotInfo[ 'granularity' ],
                'ylabel'      : plotInfo[ 'unit' ],
                'sort_labels' : 'last_value' 
               }
    
    return self._generateCumulativePlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  def __reportNumberOfField( self, reportRequest, field ):
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField  + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', 
                                                             field ]
                   )
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    
    retVal = self._getTimedData( startTime,
                                 endTime,
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 {} )
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    
    __accumMaxValue = self._getAccumulationMaxValue( dataDict )
    suitableUnits   = self._findSuitableRateUnit( dataDict, __accumMaxValue, "files" ) 
        
    baseDataDict, graphDataDict, __maxValue, unitName = suitableUnits
    
    return S_OK( { 
                  'data'          : baseDataDict, 
                  'graphDataDict' : graphDataDict,
                  'granularity'   : granularity, 
                  'unit'          : unitName 
                  } )

  def __plotNumberOfField( self, reportRequest, plotInfo, filename , title, label ):
    
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]
    
    metadata = { 
                'title'         : '%s  by %s' % (title, reportRequest[ 'grouping' ]),
                'starttime'     : startTime,
                'endtime'       : endTime,
                'span'          : span,
                'skipEdgeColor' : True,
                'ylabel'        : label  }
    
    plotInfo[ 'data' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'data' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )

  def __reportCumulativeNumberOfField( self, reportRequest, field ):
    
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    field
                                   ]
                   )
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    
    retVal = self._getTimedData( startTime,
                                 endTime,
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 { 'convertToGranularity' : 'average', 'checkNone' : True })
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict)
    dataDict = self._accumulate( granularity, startTime, endTime, dataDict)

    return S_OK({ 
                 'data'        : dataDict, 
                 'granularity' : granularity
                 })

  def __plotCumulativeNumberOfField( self, reportRequest, plotInfo, filename , title, label ):
    
    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    span      = plotInfo[ 'granularity' ]  
    
    metadata = { 
                'title'         : '%s  by %s' % ( title, reportRequest[ 'grouping' ] ),
                'starttime'     : startTime,
                'endtime'       : endTime,
                'span'          : span,
                'skipEdgeColor' : True,
                'ylabel'        : label 
                }
    
    plotInfo[ 'data' ] = self._fillWithZero( span, startTime, endTime, plotInfo[ 'data' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )

  def __reportAverageNumberOfField( self, reportRequest, field ):
    #selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ",SUM(%s/%s)",
    #                 reportRequest[ 'groupingFields' ][1] + [Field, 'entriesInBucket'
    #                               ]
    #               )
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ",SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ field ] )

    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                   reportRequest[ 'endTime' ],
                                   selectFields,
                                   reportRequest[ 'condDict' ],
                                   reportRequest[ 'groupingFields' ],
                                   {})
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict = retVal[ 'Value' ]
    #bins = self._getBins(self._typeName, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ])
    #numBins = len(bins)
    #for key in dataDict:
    #  dataDict[ key ] = float(dataDict[ key ] / numBins)
    return S_OK({ 'data' : dataDict  })

  def __plotAverageNumberOfField( self, reportRequest, plotInfo, filename, title, label ):

    metadata = { 'title'     : '%s by %s' % ( title, reportRequest[ 'grouping' ] ),
                 'ylabel'    : label,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime'   : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data' ], metadata )

  def __report2D( self, reportRequest, field1, field2 ):
    
    _selectField = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields = ( _selectField + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    field1, field2 ] )

    startTime = reportRequest[ 'startTime' ]
    endTime   = reportRequest[ 'endTime' ]
    
    retVal = self._getTimedData( startTime,
                                 endTime,
                                 selectFields,
                                 reportRequest[ 'condDict' ],
                                 reportRequest[ 'groupingFields' ],
                                 { 'checkNone' : True,
                                   'convertToGranularity' : 'sum',
                                   'calculateProportionalGauges' : True 
                                 } )
    if not retVal[ 'OK' ]:
      return retVal
    
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    
    dataDict = self._fillWithZero( granularity, startTime, endTime, dataDict )
    return S_OK( { 
                  'data'        : dataDict, 
                  'granularity' : granularity 
                  } )

  def __plot2D( self, reportRequest, plotInfo, filename, label ):

    metadata = { 
                'title'     : 'Jobs by %s' % reportRequest[ 'grouping' ],
                'starttime' : reportRequest[ 'startTime' ],
                'endtime'   : reportRequest[ 'endTime' ],
                'span'      : plotInfo[ 'granularity' ],
                'ylabel'    : label 
                }
    
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF