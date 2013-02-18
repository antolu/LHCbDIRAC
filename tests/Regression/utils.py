import os, shutil

def cleanTestDir():
  for fileIn in os.listdir( '.' ):
    if 'Local' in fileIn:
      shutil.rmtree( fileIn )
    for fileToRemove in ['std.out', 'std.err']:
      try:
        os.remove( fileToRemove )
      except OSError:
        continue

def getOutput( typeOut = 'MC' ):
  # Now checking for some outputs
  # prodConf files
  
  if typeOut == 'MC':
    filesCouples = [( 'prodConf_Boole_00012345_00006789_2.py', 'pConfBooleExpected.txt' ),
                    ( 'prodConf_Moore_00012345_00006789_3.py', 'pConfMooreExpected.txt' ),
                    ( 'prodConf_Brunel_00012345_00006789_4.py', 'pConfBrunelExpected.txt' ),
                    ( 'prodConf_DaVinci_00012345_00006789_5.py', 'pConfDaVinciExpected.txt' )]

  retList = []
  
  for fileIn in os.listdir( '.' ):
    if 'Local_' in fileIn:
      for found, expected in filesCouples:
        fd = open( './' + fileIn + '/' + found )
        pConfFound = fd.read()
        pConfExpected = ( open( expected ) ).read()
        retList.append( ( pConfFound, pConfExpected ) )

  return retList
