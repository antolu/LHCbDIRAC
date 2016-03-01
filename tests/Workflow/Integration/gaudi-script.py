#!/usr/bin/env python
'''Script to run '''

from os import system, environ, pathsep, getcwd
import sys
from Configurables import LHCbApp

# Main
if __name__ == '__main__':

  optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
  optDec = "$DECFILESROOT/options/11102400.py;"
  optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
  optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
  optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Gauss_00012345_00067890_1.py"

  options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
  LHCbApp().EvtMax = 2
  sys.exit( system( '''gaudirun.py -T %s''' % options ) / 256 )
