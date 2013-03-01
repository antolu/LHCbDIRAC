#!/usr/bin/env python
'''Script to run '''

from os import system, environ, pathsep, getcwd
import sys


# Main
if __name__ == '__main__':
  optGauss = "$APPCONFIGOPTS/Gauss/Beam4000GeV-md100-JulSep2012-nu2.5.py;"
  optDec = "$DECFILESROOT/options/15512012.py;"
  optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
  optOpts = " $APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py;"
  optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Gauss_00012345_00067890_1.py"
  options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf

  sys.exit( system( '''gaudirun.py -T %s''' % options ) / 256 )
