""" Just a module with some utilities
"""

import os, tarfile, math

from DIRAC import S_OK, S_ERROR, gConfig

def tarFiles( outputFile, files = [], compression = '', deleteInput = False ):
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

def getEventsToProduce( CPUe, CPUTime = None, CPUNormalizationFactor = None ):
  """
    Return the number of events to produce considering the CPU time available.
  """

  if CPUe <= 0.0:
    return S_ERROR( 'CPUe must be strictly positive' );

  if CPUTime is None:
    CPUTime = gConfig.getValue( '/LocalSite/CPUTime', 0.0 )

  if CPUNormalizationFactor is None:
    CPUNormalizationFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )

  eventsToProduce = int( math.floor( ( CPUTime * CPUNormalizationFactor ) / CPUe ) )

  return S_OK( eventsToProduce )
