from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

########################################################################################

gLogger.info( "Submitting hello world job banning T1s" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-T2s" )
helloJ.setInputSandbox( [cwd + '/exe-script.py'] )

helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setBannedSites( ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
                        'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'] )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "Submitting hello world job targeting LCG.CERN.ch" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-CERN" )
helloJ.setInputSandbox( [cwd + '/exe-script.py'] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setDestination( 'LCG.CERN.ch' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "Submitting gaudiRun job (Gauss only)" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Gauss-test" )
gaudirunJob.setInputSandbox( 'prodConf_Gauss_00012345_00067890_1.py' )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

optGauss = "$APPCONFIGOPTS/Gauss/Beam4000GeV-md100-JulSep2012-nu2.5.py;"
optDec = "$DECFILESROOT/options/15512012.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = " $APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
gaudirunJob.addPackage( 'AppConfig', 'v3r160' )
gaudirunJob.addPackage( 'DecFiles', 'v26r24' )
gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
gaudirunJob.setApplication( 'Gauss', 'v42r4', options, extraPackages = 'AppConfig.v3r160;DecFiles.v26r24;ProdConf.v1r9' )

gaudirunJob.setSystemConfig( 'ANY' )
gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################
