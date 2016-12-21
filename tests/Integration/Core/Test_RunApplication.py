#!/usr/bin/env python

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from LHCbDIRAC.Core.Utilities.RunApplication import *

# This is taken from PR 33857
ra = RunApplication()
ra.applicationName = 'Gauss'
ra.applicationVersion = 'v49r5'
ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.pyi',
                     '$APPCONFIGOPTS/Gauss/DataType-2012.py',
                     '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
                     '$APPCONFIGOPTS/Gauss/NoPacking.py',
                     '$DECFILESROOT/options/@{eventType}.py',
                     '$LBPYTHIA8ROOT/options/Pythia8.py',
                     '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
                     '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
ra.extraPackages = [('AppConfig', 'v3r277'), 
                    ('DecFiles', 'v29r10'),
#                    ('ProdConf', '')
                   ]
ra.step_Number = 1

#print ra.gaudirunCommand()

ra.run()
