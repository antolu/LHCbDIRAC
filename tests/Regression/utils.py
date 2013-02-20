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
  elif typeOut == 'Reco':
    filesCouples = [( 'prodConf_Brunel_00012345_00006789_1.py', 'pConfBrunelRecoExpected.txt' ),
                    ( 'prodConf_DaVinci_00012345_00006789_2.py', 'pConfDaVinciRecoExpected.txt' )
                    ]
  elif typeOut == 'Reco_old':
    filesCouples = [( 'prodConf_Brunel_0020194_00106359_1.py', 'pConfBrunelRecoOldExpected.txt' ),
                    ( 'prodConf_DaVinci_0020194_00106359_2.py', 'pConfDaVinciRecoOldExpected.txt' )
                    ]
  elif typeOut == 'Stripp':
    filesCouples = [( 'prodConf_DaVinci_00012345_00006789_1.py', 'pConfDaVinciStrippExpected.txt' )
                    ]
  elif typeOut == 'Merge':
    filesCouples = [( 'prodConf_LHCb_00012345_00006789_1.py', 'pConfLHCbExpected.txt' )
                    ]
  elif typeOut == 'MergeM':
    filesCouples = [( 'prodConf_DaVinci_00012345_00006789_1.py', 'pConfDaVinciMergeExpected.txt' )
                    ]

  retList = []

  for fileIn in os.listdir( '.' ):
    if 'Local_' in fileIn:
      for found, expected in filesCouples:
        fd = open( './' + fileIn + '/' + found )
        pConfFound = fd.read()
        pConfExpected = ( open( expected ) ).read()
        retList.append( ( pConfFound, pConfExpected ) )

  return retList

