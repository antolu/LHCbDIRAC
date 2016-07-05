""" Just a module with some utilities
"""

import os
import tarfile
import math

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Resources
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getCPUTime
from LHCbDIRAC.Core.Utilities.XMLTreeParser import XMLTreeParser

__RCSID__ = "$Id$"


def tarFiles( outputFile, files = None, compression = 'gz', deleteInput = False ):
  """ just make a tar
  """
  if files is None:
    files = []

  try:
    tar = tarfile.open( outputFile, 'w:' + compression )
    for fileIn in files:
      tar.add( fileIn )
    tar.close()
  except tarfile.CompressionError:
    return S_ERROR( 'Compression not available' )

  if deleteInput:
    for fileIn in files:
      try:
        os.remove( fileIn )
      except OSError:
        pass

  return S_OK()

#############################################################################

def lowerExtension():
  """
    Lowers the file extension of the produced files (on disk!).
    E.g.: fileName.EXTens.ION -> fileName.extens.ion
  """

  filesInDir = [x for x in os.listdir( '.' ) if not os.path.isdir( x )]

  lowers = []

  for fileIn in filesInDir:
    splitted = fileIn.split( '.' )
    if len( splitted ) > 1:
      lowered = ''
      for toBeLowered in splitted[1:]:
        lowered = lowered + '.' + toBeLowered.lower()
        final = splitted[0] + lowered
    else:
      final = splitted[0]
    lowers.append( ( fileIn, final ) )

  for fileIn in lowers:
    os.rename( fileIn[0], fileIn[1] )

#############################################################################

def getEventsToProduce( CPUe, CPUTime = None, CPUNormalizationFactor = None,
                        maxNumberOfEvents = None, jobMaxCPUTime = None ):
  """ Returns the number of events to produce considering the CPU time available.
      CPUTime and CPUNormalizationFactor are taken from the LocalSite configuration if not provided.
      No checks are made on the values passed !

      Limits can be set.
  """

  if CPUNormalizationFactor is None:
    CPUNormalizationFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 1.0 )

  if CPUTime is None:
    CPUTime = getCPUTime( CPUNormalizationFactor )
  if jobMaxCPUTime:
    gLogger.verbose( "CPUTimeLeft from the WN perspective is %d, the job maximum CPUTime (in seconds) is %d" % ( CPUTime,
                                                                                                                 jobMaxCPUTime ) )
    CPUTime = min( CPUTime, jobMaxCPUTime )

  gLogger.verbose( "CPUTime = %d, CPUNormalizationFactor = %f, CPUe = %d" % ( CPUTime,
                                                                              CPUNormalizationFactor,
                                                                              CPUe ) )

  eventsToProduce = int( math.floor( CPUTime * CPUNormalizationFactor ) / float( CPUe ) )
  gLogger.verbose( "Without limits, we can produce %d events" % eventsToProduce )

  gLogger.info( "We can produce %d events" % eventsToProduce )
  willProduce = int( eventsToProduce * 0.8 )
  gLogger.info( "But we take a conservative approach, so 80%% of those: %d" % willProduce )

  if maxNumberOfEvents:
    gLogger.verbose( "Limit for MaxNumberOfEvents: %d" % maxNumberOfEvents )
    willProduce = min( willProduce, maxNumberOfEvents )

  if willProduce < 1:
    raise RuntimeError( "No time left to produce events" )

  return willProduce

#############################################################################

def getCPUNormalizationFactorAvg():
  """
    Returns the average HS06 CPU normalization factor for the LCG sites (all CEs, all queues).
    Raises an Exception if it can not.
  """

  factorsSum = 0.0
  nQueues = 0

  sites = gConfig.getSections( 'Resources/Sites/LCG' )
  if not sites['OK']:
    raise RuntimeError( sites['Message'] )
  else:
    sites = sites['Value']

  queuesRequest = Resources.getQueues( sites )
  if not queuesRequest['OK']:
    raise RuntimeError( queuesRequest['Message'] )
  else:
    queuesRequest = queuesRequest['Value']

  for site in queuesRequest.values():
    for ce in site.values():
      if 'Queues' in ce:
        for queue in ce['Queues'].values():
          if 'SI00' in queue:
            # convert from SI00 to HS06
            factorsSum += float ( queue['SI00'] ) / 250
            nQueues += 1

  if nQueues == 0:
    raise RuntimeError( "No queues to get CPU normalization factor from" )
  else:
    CPUNormalizationFactorAvg = float( factorsSum / nQueues )

  return CPUNormalizationFactorAvg

###############################################################################

def getProductionParameterValue( productionXML, parameterName ):
  """ Get a parameter value from a production XML description
  """

  # lets assume no parameters are different only by case, as it would be a bad idea
  parameterName = parameterName.lower()

  parser = XMLTreeParser()
  parser.parseString( productionXML )
  tree = parser.tree.pop()

  for parameterElement in tree.childrens( 'Parameter' ):
    if parameterElement.attributes['name'].lower() == parameterName:
      valueElement = parameterElement.children
      if valueElement is []:
        return None
      valueElement = valueElement[0]

      return valueElement.value

  return None

###############################################################################
