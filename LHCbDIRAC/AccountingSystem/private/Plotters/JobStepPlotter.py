from DIRAC import S_OK, S_ERROR
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

#############################################################################
class JobStepPlotter(BaseReporter):

  #############################################################################
  _typeName = "JobStep"
  _typeKeyFields = [ dF[0] for dF in JobStep().definitionKeyFields ]

  #############################################################################
  _reportCPUEfficiencyName = "CPU efficiency"
  def _reportCPUEfficiency(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'CPUTime', 'ExecTime'
                                   ]
                   )

    retVal = self._getTimedData(reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : False,
                                  'consolidationFunction' : self._efficiencyConsolidation })
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    if len(dataDict) > 1:
      #Get the total for the plot
      selectFields = ("'Total', %s, %s, SUM(%s),SUM(%s)",
                        [ 'startTime', 'bucketLength',
                          'CPUTime', 'ExecTime'
                        ]
                     )

      retVal = self._getTimedData(reportRequest[ 'startTime' ],
                                  reportRequest[ 'endTime' ],
                                  selectFields,
                                  reportRequest[ 'condDict' ],
                                  reportRequest[ 'groupingFields' ],
                                  { 'scheckNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : False,
                                  'consolidationFunction' : self._efficiencyConsolidation  })
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField(totalDict, 0)
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    return S_OK({ 'data' : dataDict, 'granularity' : granularity })

  #############################################################################
  def _plotCPUEfficiency(self, reportRequest, plotInfo, filename):
    metadata = { 'title' : 'CPU efficiency by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateQualityPlot(filename, plotInfo[ 'data' ], metadata)

  #############################################################################
  _reportCPUUsageName = "CPU time"
  def _reportCPUUsage(self, reportRequest):
    return self.__reportNormPlot(reportRequest, 'CPUTime', 'time')

  #############################################################################
  def _plotCPUUsage(self, reportRequest, plotInfo, filename):
    return self.__plotNormPlot(reportRequest, plotInfo, filename, 'CPU usage')

  _reportPieCPUTimeName = 'Pie plot of CPU time'
  def _reportPieCPUTime(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'CPUTime')

  #############################################################################
  def _plotPieCPUTime(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'CPU time', 'CPUTime')

  #############################################################################
  _reportCumulativeCPUTimeName = "Cumulative CPU time"
  def _reportCumulativeCPUTime(self, reportRequest):
    return self.__reportCumulativePlot(reportRequest, 'CPUTime', 'time')

  #############################################################################
  def _plotCumulativeCPUTime(self, reportRequest, plotInfo, filename):
    return self.__plotCumulative(reportRequest, plotInfo, filename, 'Cumulative CPU time')


  #####Normalized CPU
  #############################################################################
  _reportNormCPUTimeName = "NormCPU time"
  def _reportNormCPUTime(self, reportRequest):
    return self.__reportNormPlot(reportRequest, 'NormCPUTime', 'time')

  #############################################################################
  def _plotNormCPUTime(self, reportRequest, plotInfo, filename):
    return self.__plotNormPlot(reportRequest, plotInfo, filename, 'Normalized CPU ')

  #############################################################################
  _reportCumulativeNormCPUTimeName = "Cumulative normalized CPU time"
  def _reportCumulativeNormCPUTime(self, reportRequest):
    return self.__reportCumulativePlot(reportRequest, 'NormCPUTime', 'time')

  #############################################################################
  def _plotCumulativeNormCPUTime(self, reportRequest, plotInfo, filename):
    return self.__plotCumulative(reportRequest, plotInfo, filename, 'Cumulative Normalized CPU time')

  #############################################################################
  _reportPieNormCPUTimeName = 'Pie plot of NormCPU time'
  def _reportPieNormCPUTime(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'NormCPUTime')

  #############################################################################
  def _plotPieNormCPUTime(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'Average Normalized CPU time', 'NormCPUTime')

  #####InputData
  #############################################################################

  _reportInputDataName = "Input Data"
  def _reportInputData(self, reportRequest):
    return self.__reportNormPlot(reportRequest, 'InputData', 'files')

  #############################################################################
  def _plotInputData(self, reportRequest, plotInfo, filename):
    return self.__plotNormPlot(reportRequest, plotInfo, filename, 'Input data')

  #############################################################################
  _reportCumulativeInputDataName = "Cumulative Input Data"
  def _reportCumulativeInputData(self, reportRequest):
    return self.__reportCumulativePlot(reportRequest, 'InputData', 'files')

  #############################################################################
  def _plotCumulativeInputData(self, reportRequest, plotInfo, filename):
    return  self.__plotCumulative(reportRequest, plotInfo, filename, 'Cumulative Input Data ')

  #############################################################################
  _reportPieInputDataName = 'Pie plot of Input Data'
  def _reportPieInputData(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'InputData')

  #############################################################################
  def _plotPieInputData(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'Pie plot of Input data', 'InputData')

  #####OutputData
  #############################################################################
  _reportOutputDataName = "Output Data"
  def _reportOutputData(self, reportRequest):
    return self.__reportNormPlot(reportRequest, 'OutputData', 'files')

  #############################################################################
  def _plotOutputData(self, reportRequest, plotInfo, filename):
    return self.__plotNormPlot(reportRequest, plotInfo, filename, 'Output data')

  #############################################################################
  _reportCumulativeOutputDataName = "Cumulative OutputData"
  def _reportCumulativeOutputData(self, reportRequest):
    return self.__reportCumulativePlot(reportRequest, 'OutputData', 'files')

  #############################################################################
  def _plotCumulativeOutputData(self, reportRequest, plotInfo, filename):
    return self.__plotCumulative(reportRequest, plotInfo, filename, 'Cumulative Output Data ')

  #############################################################################
  _reportPieOutputDataName = 'Pie plot of Output Data'
  def _reportPieOutputData(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'OutputData')

  #############################################################################
  def _plotPieOutputData(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'Pie plot of  Output data', 'OutputData')

  #####InputEvents
  #############################################################################
  _reportInputEventsName = "Input Events"
  def _reportInputEvents(self, reportRequest):
    return self.__reportNumberOfField(reportRequest, 'InputEvents')

  #############################################################################
  def _plotInputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotNumberOfField(reportRequest, plotInfo, filename, 'InputEvents', 'Input Events')

  #############################################################################
  _reportCumulativeInputEventsName = "Cumulative Input Events"
  def _reportCumulativeInputEvents(self, reportRequest):
    return self.__reportCumulativeNumberOfField(reportRequest, 'InputEvents')

  #############################################################################
  def _plotCumulativeInputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeNumberOfField(reportRequest, plotInfo, filename, 'Cumulative Input Events ', 'Input Events')

  #############################################################################
  _reportPieInputEventsName = 'Pie plot of Input Events'
  def _reportPieInputEvents(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'InputEvents')

  #############################################################################
  def _plotPieInputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'Pie plot of Input Events', 'InputEvents')

  #####OutputEvents
  #############################################################################
  _reportOutputEventsName = "Output Events"
  def _reportOutputEvents(self, reportRequest):
    return self.__reportNumberOfField(reportRequest, 'OutputEvents')

  #############################################################################
  def _plotOutputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotNumberOfField(reportRequest, plotInfo, filename, 'OutputEvents', 'Output Events')

  #############################################################################
  _reportCumulativeOutputEventsName = "Cumulative Output Events"
  def _reportCumulativeOutputEvents(self, reportRequest):
    return self.__reportCumulativeNumberOfField(reportRequest, 'OutputEvents')

  #############################################################################
  def _plotCumulativeOutputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeNumberOfField(reportRequest, plotInfo, filename, 'Cumulative Output Events ', 'Output Events')

  #############################################################################
  _reportPieOutputEventsName = 'Pie plot of Output Events'
  def _reportPieOutputEvents(self, reportRequest):
    return self.__reportPiePlot(reportRequest, 'OutputEvents')

  #############################################################################
  def _plotPieOutputEvents(self, reportRequest, plotInfo, filename):
    return self.__plotPie(reportRequest, plotInfo, filename, 'Pie plot of Output Events', 'OutputEvents')

  #############################################################################
  _reportInputEventsPerOutputEventsName = 'Input/Output Events'
  def _reportInputEventsPerOutputEvents(self, reportRequest):
    return self.__report2D(reportRequest, 'InputEvents', 'OutputEvents')

  #############################################################################
  def _plotInputEventsPerOutputEvents(self, reportRequest, plotInfo, filename):
    return self.__plot2D(reportRequest, plotInfo, filename, "Input/Output Events")

  #############################################################################
  _reportCPUTimePerOutputEventsName = 'CPUTime/Output Events'
  def _reportCPUTimePerOutputEvents(self, reportRequest):
    return self.__report2D(reportRequest, 'CPUTime', 'OutputEvents')

  #############################################################################
  def _plotCPUTimePerOutputEvents(self, reportRequest, plotInfo, filename):
    return self.__plot2D(reportRequest, plotInfo, filename, "CPUTime/Output Events")

  #############################################################################
  _reportCPUTimePerInputEventsName = 'CPUTime/Input Events'
  def _reportCPUTimePerInputEvents(self, reportRequest):
    return self.__report2D(reportRequest, 'CPUTime', 'InputEvents')

  #############################################################################
  def _plotCPUTimePerInputEvents(self, reportRequest, plotInfo, filename):
    return self.__plot2D(reportRequest, plotInfo, filename, "CPUTime/Input Events")

  #############################################################################
  #HELPER methods
  #############################################################################
  def __reportNormPlot(self, reportRequest, field, unit):
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    field
                                   ]
                   )
    retVal = self._getTimedData(reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {})
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    dataDict, maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit(dataDict,
                                                                                  self._getAccumulationMaxValue(dataDict),
                                                                                  unit)
    return S_OK({ 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName })

  #############################################################################
  def __plotNormPlot(self, reportRequest, plotInfo, filename, title):
    metadata = { 'title' : '%s by %s' % (title, reportRequest[ 'grouping' ]),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ] }
    return self._generateStackedLinePlot(filename, plotInfo[ 'graphDataDict' ], metadata)

  #############################################################################
  def __reportPiePlot(self, reportRequest, field):
    #selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", SUM(%s/%s)",
    #                 reportRequest[ 'groupingFields' ][1] + [ field, 'entriesInBucket'
    #                               ]
    #               )
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ field
                                   ]
                   )
    retVal = self._getSummaryData(reportRequest[ 'startTime' ],
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

  #############################################################################
  def __plotPie(self, reportRequest, plotInfo, filename, title, label):
    metadata = { 'title' : '%s by %s' % (title, reportRequest[ 'grouping' ]),
                 'ylabel' : label,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot(filename, plotInfo[ 'data'], metadata)


  #############################################################################
  def __reportCumulativePlot(self, reportRequest, field, unit):
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', field ]
                   )
    retVal = self._getTimedData(reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {})
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict)
    dataDict = self._accumulate(granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              unit)
    return S_OK({ 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName })
  #############################################################################
  def __plotCumulative(self, reportRequest, plotInfo, filename, title):
    metadata = { 'title' : '%s by %s' % (title, reportRequest[ 'grouping' ]),
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ],
                 'sort_labels' : 'last_value' }
    return self._generateCumulativePlot(filename, plotInfo[ 'graphDataDict' ], metadata)


  #############################################################################
  def __reportNumberOfField(self, reportRequest, Field):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', Field ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "files" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  #############################################################################
  def __plotNumberOfField(self, reportRequest, plotInfo, filename , title, label):
    metadata = { 'title' : '%s  by %s' % (title, reportRequest[ 'grouping' ]) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : label  }
    plotInfo[ 'data' ] = self._fillWithZero(plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ])
    return self._generateStackedLinePlot(filename, plotInfo[ 'data' ], metadata)

  #############################################################################
  def __reportCumulativeNumberOfField(self, reportRequest, Field):
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    Field
                                   ]
                   )
    retVal = self._getTimedData(reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'convertToGranularity' : 'average', 'checkNone' : True })
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict)
    dataDict = self._accumulate(granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict)

    return S_OK({ 'data' : dataDict, 'granularity' : granularity})

  #############################################################################
  def __plotCumulativeNumberOfField(self, reportRequest, plotInfo, filename , title, label):
    metadata = { 'title' : '%s  by %s' % (title, reportRequest[ 'grouping' ]) ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : label  }
    plotInfo[ 'data' ] = self._fillWithZero(plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ])
    return self._generateStackedLinePlot(filename, plotInfo[ 'data' ], metadata)

  #############################################################################
  def __reportAverageNumberOfField(self, reportRequest, Field):
    #selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ",SUM(%s/%s)",
    #                 reportRequest[ 'groupingFields' ][1] + [Field, 'entriesInBucket'
    #                               ]
    #               )
    selectFields = (self._getSelectStringForGrouping(reportRequest[ 'groupingFields' ]) + ",SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [Field
                                   ]
                   )

    retVal = self._getSummaryData(reportRequest[ 'startTime' ],
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

  #############################################################################
  def __plotAverageNumberOfField(self, reportRequest, plotInfo, filename, title, label):
    metadata = { 'title' : '%s by %s' % (title, reportRequest[ 'grouping' ]),
                 'ylabel' : label,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot(filename, plotInfo[ 'data'], metadata)

  #############################################################################
  def __report2D( self, reportRequest, field1, field2 ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    field1, field2
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : True } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  #############################################################################
  def __plot2D( self, reportRequest, plotInfo, filename, label ):
    metadata = { 'title' : 'Jobs by %s' % reportRequest[ 'grouping' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : label }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'data' ], metadata )
