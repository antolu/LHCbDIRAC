from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import random

from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Utilities.utils import find_all

gLogger.setLevel( 'DEBUG' )

gLogger.info( "Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )

gaudirunJob = LHCbJob()
dirac = DiracLHCb()

gaudirunJob.setName( "gaudirun-Gauss-test" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0],
                              find_all( 'wrongConfig.cfg', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
optDec = "$DECFILESROOT/options/11102400.py;"
optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
gaudirunJob.addPackage( 'AppConfig', 'v3r171' )
gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
gaudirunJob.setApplication( 'Gauss', 'v45r3', options, extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                             modulesNameList = ['CreateDataFile',
                                                'GaudiApplication',
                                                'FileUsage',
                                                'UploadOutputData',
                                                'UploadLogFile',
                                                'FailoverRequest',
                                                'UserJobFinalization'], )

gaudirunJob._addParameter( gaudirunJob.workflow, 'PRODUCTION_ID', 'string', '00000000', 'ProductionID' )
gaudirunJob._addParameter( gaudirunJob.workflow, 'JOB_ID', 'string', '00000000', 'JobID' )
gaudirunJob._addParameter( gaudirunJob.workflow, 'configName', 'string', 'testCfg', 'ConfigName' )
gaudirunJob._addParameter( gaudirunJob.workflow, 'configVersion', 'string', 'testVer', 'ConfigVersion' )
outputList = [{'stepName': 'GaussStep1', 'outputDataType': 'sim', 'outputBKType': 'SIM', 'outputDataSE': 'Tier1-BUFFER',
               'outputDataName': '00012345_00067890_1.sim'}]
gaudirunJob._addParameter( gaudirunJob.workflow, 'outputList', 'list', outputList, 'outputList' )
gaudirunJob._addParameter( gaudirunJob.workflow, 'outputDataFileMask', 'string', '', 'outputFM' )
gaudirunJob._addParameter( gaudirunJob.workflow, 'outputMode', 'string', 'Local', 'OM' )

gaudirunJob.setSystemConfig( 'ANY' )
gaudirunJob.setConfigArgs( 'wrongConfig.cfg' )

gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )
