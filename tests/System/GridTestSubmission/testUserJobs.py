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

helloJ.setCPUTime( 172800 )
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

helloJ.setCPUTime( 172800 )
helloJ.setDestination( 'LCG.CERN.ch' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

# gLogger.info( "Submitting gaudiRun job (Gauss only)" )
#
# gaudiRunJ = LHCbJob()
#
# gaudiRunJ.setName( "gauss GaudiRun Job" )
# gaudiRunJ.setInputSandbox()
# gaudiRunJ.setOutputSandbox()
# gaudiRunJ.setApplicationScript()
# gaudiRunJ.setAncestorDepth( 0 )
# gaudiRunJ.setSystemConfig( 'x86_64-slc5-gcc43-opt' )
# gaudiRunJ.setCPUTime( 172800 )
#
# result = dirac.submit( gaudiRunJ )
# gLogger.info( 'Submission Result: ', result )
