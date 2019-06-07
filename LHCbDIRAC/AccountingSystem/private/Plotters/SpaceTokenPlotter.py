###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" LHCbDIRAC.AccountingSystem.private.Plotters.SpaceTokenPlotter

   SpaceTokenPlotter.__bases__:
     DIRAC.AccountingSystem.private.Plotters.BaseReporter.BaseReporter
  
"""


from DIRAC                                                import S_OK, gLogger
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

from LHCbDIRAC.AccountingSystem.Client.Types.SpaceToken   import SpaceToken


__RCSID__ = "$Id$"


class SpaceTokenPlotter( BaseReporter ):
  """ SpaceTokenPlotter as extension of BaseReporter
  
  """
  
  _typeName      = "SpaceToken"
  _typeKeyFields = [ dF[0] for dF in SpaceToken().definitionKeyFields ]

  #.............................................................................
  # Generic Reporter

  def reporter( self, reportRequest, spaceType, groupingFields = False ):
    """ reporter
    
    Generic method used by all the _report<something> methods. Only one method
    is actually making use of the groupingFields attribute ( `_reportFreeUsedSpace` ). 
    
    """
    
    reportRequest[ 'condDict' ][ 'SpaceType' ] = spaceType
    if groupingFields:
      reportRequest[ 'condDict' ][ 'grouping' ]  = [ 'SpaceType' ]
      reportRequest[ 'groupingFields' ]          = ( [ '%s','%s' ], 
                                                     reportRequest[ 'groupingFields' ][1] + [ 'SpaceType' ] )
      
    selectString = self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] )
    selectFields  = ( selectString + ", %s, %s, SUM(%s/%s)",
                      reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                                               'Space', 'entriesInBucket'
                                                             ]
                    )
    
    if groupingFields:
      # This is a little hack..
      reportRequest[ 'groupingFields' ] = ( '%s, %s', reportRequest[ 'groupingFields' ][ 1 ] )
    
    gLogger.warn( reportRequest )
    
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
    
    #3rd value, maxValue is not used
    baseDataDict, graphDataDict, __, unitName = suitableUnits
     
    return S_OK( { 'data'          : baseDataDict, 
                   'graphDataDict' : graphDataDict,
                   'granularity'   : granularity, 
                   'unit'          : unitName 
                  } )


  #.............................................................................
  # Generic Plotter

  def plotter( self, reportRequest, plotInfo, filename ):
    """ plotter
    
    Generic plotter method used by all _plot<something> methods.
    
    """
    
    startEpoch  = reportRequest[ 'startTime' ]
    endEpoch    = reportRequest[ 'endTime' ]
    granularity = plotInfo[ 'granularity' ]
    dataDict    = plotInfo[ 'graphDataDict' ]
    
    metadata = {'title'     : "Space grouped by %s" % reportRequest[ 'grouping' ],
                'starttime' : startEpoch,
                'endtime'   : endEpoch,
                'span'      : granularity,
                'ylabel'    : plotInfo[ 'unit' ]}
    
    dataDict = self._fillWithZero( granularity, startEpoch, endEpoch, dataDict )
    return self._generateStackedLinePlot( filename, dataDict, metadata )
  
  
  #.............................................................................
    
  _plotFreeUsedSpace       = plotter
  _reportFreeUsedSpaceName = "Free and Used Space"
  def _reportFreeUsedSpace( self, reportRequest ):
    """ _reportFreeUsedSpace
    
    """
    
    return self.reporter( reportRequest, [ 'Free', 'Used' ], groupingFields = True )
  

  #.............................................................................

  _plotTotalSpace       = plotter
  _reportTotalSpaceName = "Total Space"
  def _reportTotalSpace( self, reportRequest ):
    """ _reportTotalSpace
    
    """
    
    return self.reporter( reportRequest, [ 'Total' ] )


  #.............................................................................

  _plotGuaranteedSpace       = plotter
  _reportGuaranteedSpaceName = "Guaranteed Space"
  def _reportGuaranteedSpace( self, reportRequest ):
    """ _reportGuaranteedSpace
    
    """
    
    return self.reporter( reportRequest, [ 'Guaranteed' ] )


  #.............................................................................

  _plotFreeSpace       = plotter
  _reportFreeSpaceName = "Free Space"
  def _reportFreeSpace( self, reportRequest ):
    """ _reportFreeSpace
    
    """
    
    return self.reporter( reportRequest, [ 'Free' ] )


  #.............................................................................

  _plotUsedSpace       = plotter
  _reportUsedSpaceName = "Used Space"
  def _reportUsedSpace( self, reportRequest ):
    """ _reportUsedSpace
    
    """   

    return self.reporter( reportRequest, [ 'Used' ] )


#...............................................................................
#EOF
