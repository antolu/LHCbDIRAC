""" Just a module with some utilities
"""

import os

def tarFiles( outputFile, files = [], compression = '', deleteInput = False ):
  """ just make a tar
  """
  import tarfile

  tar = tarfile.open( outputFile, 'w:' + compression )
  for file in files:
    tar.add( file )
  tar.close()

  if deleteInput:
    for file in files:
      os.remove( file )


#############################################################################

def lowerExtension():
  """
    Lowers the file extension of the produced files (on disk!). 
    E.g.: fileName.EXTens.ION -> fileName.extens.ion
  """

  filesInDir = [x for x in os.listdir( '.' ) if not os.path.isdir( x )]

  lowers = []

  for file in filesInDir:
    splitted = file.split( '.' )
    if len( splitted ) > 1:
      lowered = ''
      for toBeLowered in splitted[1:]:
        lowered = lowered + '.' + toBeLowered.lower()
        final = splitted[0] + lowered
    else:
      final = splitted[0]
    lowers.append( ( file, final ) )

  for file in lowers:
    os.rename( file[0], file[1] )

#############################################################################
