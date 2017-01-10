#!/usr/bin/env python
""" Tests of invocation of lb-run via RunApplication module
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication

### Gauss
gLogger.always("\n ***************** Trying out GAUSS, using ProdConf (production style) \n")

# This is taken from PR 33857 (and would fall back to SetupProject)
gLogger.always("**** GAUSS v49r5")

ra = RunApplication()
ra.applicationName = 'Gauss'
ra.applicationVersion = 'v49r5'
ra.systemConfig = 'x86_64-slc6-gcc48-opt'
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
                    ('ProdConf', '')
                   ]
ra.step_Number = 1
ra.prodConfFileName = 'test_prodConf_gauss_v49r5.py'

#print ra.gaudirunCommand()

ra.run()


# # This is taken from step 133294
# gLogger.always("**** GAUSS v50r0")
#
# ra = RunApplication()
# ra.applicationName = 'Gauss'
# ra.applicationVersion = 'v50r0'
# ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Beam7000GeV-md100-nu7.6-HorExtAngle.py',
#                      '$DECFILESROOT/options/@{eventType}.py',
#                      '$LBPYTHIA8ROOT/options/Pythia8.py',
#                      '$APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py',
#                      '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
# ra.extraPackages = [('AppConfig', 'v3r304'),
#                     ('DecFiles', 'v29r9'),
#                     ('ProdConf', '')
#                    ]
# ra.systemConfig = 'x86_64-slc6-gcc48-opt'
#
# ra.step_Number = 1
#
# #print ra.gaudirunCommand()
#
# ra.run()
#
#
# gLogger.always("\n ***************** Trying out GAUSS, no ProdConf (user style) \n")
#
# gLogger.always("**** GAUSS v49r5")
#
# ra = RunApplication()
# ra.applicationName = 'Gauss'
# ra.applicationVersion = 'v49r5'
# ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.pyi',
#                      '$APPCONFIGOPTS/Gauss/DataType-2012.py',
#                      '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
#                      '$APPCONFIGOPTS/Gauss/NoPacking.py',
#                      '$DECFILESROOT/options/@{eventType}.py',
#                      '$LBPYTHIA8ROOT/options/Pythia8.py',
#                      '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
#                      '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
# ra.extraPackages = [('AppConfig', 'v3r277'),
#                     ('DecFiles', 'v29r10'),
#                    ]
# ra.step_Number = 1
#
# #print ra.gaudirunCommand()
#
# ra.run()
#
#
#
#
#
# ### Moore
# gLogger.always("\n ***************** Trying out Moore \n")


#try multicore
