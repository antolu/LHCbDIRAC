""" Just a module with some utilities
"""

__RCSID__ = "$Id"

import os, tarfile, math

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers   import Resources
from DIRAC.Core.Utilities.SiteCEMapping         import getQueueInfo
from LHCbDIRAC.Core.Utilities.XMLTreeParser     import XMLTreeParser

def tarFiles( outputFile, files = [], compression = 'gz', deleteInput = False ):
  """ just make a tar
  """

  try:
    tar = tarfile.open( outputFile, 'w:' + compression )
    for fileIn in files:
      tar.add( fileIn )
    tar.close()
  except tarfile.CompressionError:
    return S_ERROR( 'Compression not available' )

  if deleteInput:
    for fileIn in files:
      os.remove( fileIn )

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
                          maxNumberOfEvents = None, maxCPUTime = None ):
  """ Returns the number of events to produce considering the CPU time available.
      CPUTime and CPUNormalizationFactor are taken from the LocalSite configuration if not provided.
      No checks are made on the values passed !

      Limits can be set.
  """

  if CPUNormalizationFactor is None:
    CPUNormalizationFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 1.0 )

  if CPUTime is None:
    CPUTime = getCPUTime( CPUNormalizationFactor )
  if maxCPUTime:
    gLogger.verbose( "CPUTimeLeft for the queue is %d, MaxCPUTime (in seconds) is set to %d" % ( CPUTime, maxCPUTime ) )
    CPUTime = min( CPUTime, maxCPUTime )

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

def getCPUTime( CPUNormalizationFactor ):
  """ Trying to get CPUTime (in seconds) from the CS. The default is a (low) 10000s
  """
  CPUTime = gConfig.getValue( '/LocalSite/CPUTimeLeft', 0 )

  if CPUTime:
    # This is in HS06sseconds
    # We need to convert in real seconds
    CPUTime = CPUTime / int( CPUNormalizationFactor )
  else:
    # now we know that we have to find the CPUTimeLeft by looking in the CS
    gridCE = gConfig.getValue( '/LocalSite/GridCE' )
    CEQueue = gConfig.getValue( '/LocalSite/CEQueue' )
    if not CEQueue:
      # we have to look for a CEQueue in the CS
      # FIXME: quite hacky. We should better profit from something generic
      gLogger.warn( "No CEQueue in local configuration, looking to find one in CS" )
      siteName = gConfig.getValue( '/LocalSite/Site' )
      queueSection = '/Resources/Sites/%s/%s/CEs/%s/Queues' % ( siteName.split( '.' )[0], siteName, gridCE )
      res = gConfig.getSections( queueSection )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      queues = res['Value']
      CPUTimes = []
      for queue in queues:
        CPUTimes.append( gConfig.getValue( queueSection + '/' + queue + '/maxCPUTime', 10000 ) )
      cpuTimeInMinutes = min( CPUTimes )
      # These are (real, wall clock) minutes - damn BDII!
      CPUTime = int( cpuTimeInMinutes ) * 60
    else:
      queueInfo = getQueueInfo( '%s/%s' % ( gridCE, CEQueue ) )
      if not queueInfo['OK'] or not queueInfo['Value']:
        gLogger.warn( "Can't find a CE/queue, defaulting CPUTime to 10000" )
        CPUTime = 10000
      else:
        queueCSSection = queueInfo['Value']['QueueCSSection']
        # These are (real, wall clock) minutes - damn BDII!
        cpuTimeInMinutes = gConfig.getValue( '%s/maxCPUTime' % queueCSSection )
        CPUTime = int( cpuTimeInMinutes ) * 60

  return CPUTime

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
