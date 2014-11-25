'''Test class for the MCSimulationTestingAgent
'''

import unittest, datetime, importlib
from mock import MagicMock, patch

from LHCbDIRAC.TransformationSystem.Agent.MCSimulationTestingAgent import MCSimulationTestingAgent
from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production
from DIRAC.Core.Workflow.Workflow import fromXMLString
from DIRAC import gLogger

storedJobDescription = """<Workflow>
<origin></origin>
<description><![CDATA[prodDescription]]></description>
<descr_short>prodDescription</descr_short>
<version>0.0</version>
<type></type>
<name>Request_0_MCSimulation__EventType___1</name>
<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified type"><value><![CDATA[MCSimulation]]></value></Parameter>
<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User Job Priority"><value><![CDATA[1]]></value></Parameter>
<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>
<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>
<Parameter name="Site" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified destination site"><value><![CDATA[CLOUD.Test.ch]]></value></Parameter>
<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>
<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>
<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>
<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>
<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>
<Parameter name="ParametricParameters" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input parameters value"><value><![CDATA[]]></value></Parameter>
<Parameter name="OutputSandbox" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Output sandbox file list"><value><![CDATA[std.out;std.err;*.log]]></value></Parameter>
<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[100]]></value></Parameter>
<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>
<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00006789]]></value></Parameter>
<Parameter name="poolXMLCatName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="POOLXMLCatalogName"><value><![CDATA[pool_xml_catalog.xml]]></value></Parameter>
<Parameter name="outputMode" type="string" linked_module="" linked_parameter="" in="True" out="False" description="SEResolutionPolicy"><value><![CDATA[Any]]></value></Parameter>
<Parameter name="outputDataFileMask" type="string" linked_module="" linked_parameter="" in="True" out="False" description="outputDataFileMask"><value><![CDATA[allstreams.dst]]></value></Parameter>
<Parameter name="configName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ConfigName"><value><![CDATA[]]></value></Parameter>
<Parameter name="configVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ConfigVersion"><value><![CDATA[certification]]></value></Parameter>
<Parameter name="conditions" type="string" linked_module="" linked_parameter="" in="True" out="False" description="SimOrDataTakingCondsString"><value><![CDATA[]]></value></Parameter>
<Parameter name="groupDescription" type="string" linked_module="" linked_parameter="" in="True" out="False" description="GroupDescription"><value><![CDATA[]]></value></Parameter>
<Parameter name="simDescription" type="string" linked_module="" linked_parameter="" in="True" out="False" description="SimDescription"><value><![CDATA[]]></value></Parameter>
<Parameter name="eventType" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Event Type of the production"><value><![CDATA[]]></value></Parameter>
<Parameter name="numberOfEvents" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Number of events to test"><value><![CDATA[1000]]></value></Parameter>
<Parameter name="CPUe" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CPU time per event"><value><![CDATA[100]]></value></Parameter>
<Parameter name="maxNumberOfEvents" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Maximum number of events to produce (Gauss only)"><value><![CDATA[100]]></value></Parameter>
<Parameter name="multicore" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Flag for enabling gaudi parallel"><value><![CDATA[True]]></value></Parameter>
<Parameter name="BKProcessingPass" type="dict" linked_module="" linked_parameter="" in="True" out="False" description="BKProcessingPassInfo"><value><![CDATA[{'Step0': {'ApplicationName': 'Gauss', 'ExtraPackages': 'AppConfig.v3r171;ProdConf', 'DDDb': 'Sim08-20130503-1', 'BKStepID': 125080, 'ApplicationVersion': 'v45r3', 'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py', 'CondDb': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'StepName': 'Sim08a', 'StepVisible': 'Y'}}]]></value></Parameter>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
]]></body>
<origin></origin>
<description><![CDATA[ UploadLogFile module is used to upload the files present in the working
directory.
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>UploadLogFile</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication
]]></body>
<origin></origin>
<description><![CDATA[ Gaudi Application module - main module: creates the environment,
executes gaudirun with the right options
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>GaudiApplication</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.AnalyseLogFile import AnalyseLogFile
]]></body>
<origin></origin>
<description><![CDATA[ Analyse log file(s) module
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>AnalyseLogFile</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.ErrorLogging import ErrorLogging
]]></body>
<origin></origin>
<description><![CDATA[ The ErrorLogging module is used to perform error analysis using AppConfig
utilities. This occurs at the end of each workflow step such that the
step_commons dictionary can be utilized.

Since not all projects are instrumented to work with the AppConfig
error suite any failures will not be propagated to the workflow.
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>ErrorLogging</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
]]></body>
<origin></origin>
<description><![CDATA[ Module to upload specified job output files according to the parameters
defined in the production workflow.
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>UploadOutputData</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import AnalyseXMLSummary
]]></body>
<origin></origin>
<description><![CDATA[ Analyse XMLSummary module
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>AnalyseXMLSummary</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
]]></body>
<origin></origin>
<description><![CDATA[ Create and send a combined request for any pending operations at
the end of a job:
  fileReport (for the transformation)
  jobReport (for jobs)
  accounting
  request (for failover)
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>FailoverRequest</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport
]]></body>
<origin></origin>
<description><![CDATA[  Bookkeeping Reporting module (just prepare the files, do not send them
(which is done in the uploadOutput)
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>BookkeepingReport</type>
</ModuleDefinition>
<ModuleDefinition>
<body><![CDATA[
from LHCbDIRAC.Workflow.Modules.StepAccounting import StepAccounting
]]></body>
<origin></origin>
<description><![CDATA[ StepAccounting module performs several common operations at the end of
a workflow step, in particular prepares and sends the step accounting
data
]]></description>
<descr_short></descr_short>
<required></required>
<version>0.0</version>
<type>StepAccounting</type>
</ModuleDefinition>
<StepDefinition>
<origin></origin>
<version>0.0</version>
<type>Gaudi_App_Step</type>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<Parameter name="inputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="StepInputData"><value><![CDATA[]]></value></Parameter>
<Parameter name="inputDataType" type="string" linked_module="" linked_parameter="" in="True" out="False" description="InputDataType"><value><![CDATA[]]></value></Parameter>
<Parameter name="outputFilePrefix" type="string" linked_module="" linked_parameter="" in="True" out="False" description="OutputFilePrefix"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationName"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationVersion"><value><![CDATA[]]></value></Parameter>
<Parameter name="runTimeProjectName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="runTimeProjectName"><value><![CDATA[]]></value></Parameter>
<Parameter name="runTimeProjectVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="runTimeProjectVersion"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationType" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationType"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationLogFile"><value><![CDATA[]]></value></Parameter>
<Parameter name="XMLSummary" type="string" linked_module="" linked_parameter="" in="True" out="False" description="XMLSummaryFile"><value><![CDATA[]]></value></Parameter>
<Parameter name="optionsFile" type="string" linked_module="" linked_parameter="" in="True" out="False" description="OptionsFile"><value><![CDATA[]]></value></Parameter>
<Parameter name="extraOptionsLine" type="string" linked_module="" linked_parameter="" in="True" out="False" description="extraOptionsLines"><value><![CDATA[]]></value></Parameter>
<Parameter name="numberOfEventsInput" type="string" linked_module="" linked_parameter="" in="True" out="False" description="NumberOfEventsInput"><value><![CDATA[]]></value></Parameter>
<Parameter name="listoutput" type="list" linked_module="" linked_parameter="" in="True" out="False" description="StepOutputList"><value><![CDATA[[]]]></value></Parameter>
<Parameter name="extraPackages" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ExtraPackages"><value><![CDATA[]]></value></Parameter>
<Parameter name="BKStepID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="BKKStepID"><value><![CDATA[]]></value></Parameter>
<Parameter name="StepProcPass" type="string" linked_module="" linked_parameter="" in="True" out="False" description="StepProcessingPass"><value><![CDATA[]]></value></Parameter>
<Parameter name="HistogramName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="NameOfHistogram"><value><![CDATA[]]></value></Parameter>
<Parameter name="optionsFormat" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdConf configuration"><value><![CDATA[]]></value></Parameter>
<Parameter name="CondDBTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ConditionDatabaseTag"><value><![CDATA[]]></value></Parameter>
<Parameter name="DDDBTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="DetDescTag"><value><![CDATA[]]></value></Parameter>
<Parameter name="DQTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="DataQualityTag"><value><![CDATA[]]></value></Parameter>
<Parameter name="multiCore" type="string" linked_module="" linked_parameter="" in="True" out="False" description="MultiCore Flag"><value><![CDATA[]]></value></Parameter>
<Parameter name="SystemConfig" type="string" linked_module="" linked_parameter="" in="True" out="False" description="system config"><value><![CDATA[]]></value></Parameter>
<Parameter name="mcTCK" type="string" linked_module="" linked_parameter="" in="True" out="False" description="TCK to be simulated"><value><![CDATA[]]></value></Parameter>
<ModuleInstance>
<type>GaudiApplication</type>
<name>GaudiApplication</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>AnalyseLogFile</type>
<name>AnalyseLogFile</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>AnalyseXMLSummary</type>
<name>AnalyseXMLSummary</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>ErrorLogging</type>
<name>ErrorLogging</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>BookkeepingReport</type>
<name>BookkeepingReport</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>StepAccounting</type>
<name>StepAccounting</name>
<descr_short></descr_short>
</ModuleInstance>
</StepDefinition>
<StepDefinition>
<origin></origin>
<version>0.0</version>
<type>Job_Finalization</type>
<description><![CDATA[]]></description>
<descr_short></descr_short>
<ModuleInstance>
<type>UploadOutputData</type>
<name>UploadOutputData</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>UploadLogFile</type>
<name>UploadLogFile</name>
<descr_short></descr_short>
</ModuleInstance>
<ModuleInstance>
<type>FailoverRequest</type>
<name>FailoverRequest</name>
<descr_short></descr_short>
</ModuleInstance>
</StepDefinition>
<StepInstance>
<type>Gaudi_App_Step</type>
<name>Gauss_1</name>
<descr_short></descr_short>
<Parameter name="inputData" type="string" linked_module="self" linked_parameter="InputData" in="True" out="False" description="StepInputData"><value><![CDATA[]]></value></Parameter>
<Parameter name="inputDataType" type="string" linked_module="" linked_parameter="" in="True" out="False" description="InputDataType"><value><![CDATA[]]></value></Parameter>
<Parameter name="outputFilePrefix" type="string" linked_module="" linked_parameter="" in="True" out="False" description="OutputFilePrefix"><value><![CDATA[@{STEP_ID}]]></value></Parameter>
<Parameter name="applicationName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationName"><value><![CDATA[Gauss]]></value></Parameter>
<Parameter name="applicationVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationVersion"><value><![CDATA[v45r3]]></value></Parameter>
<Parameter name="runTimeProjectName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="runTimeProjectName"><value><![CDATA[]]></value></Parameter>
<Parameter name="runTimeProjectVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="runTimeProjectVersion"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationType" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationType"><value><![CDATA[]]></value></Parameter>
<Parameter name="applicationLog" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ApplicationLogFile"><value><![CDATA[@{applicationName}_@{STEP_ID}.log]]></value></Parameter>
<Parameter name="XMLSummary" type="string" linked_module="" linked_parameter="" in="True" out="False" description="XMLSummaryFile"><value><![CDATA[summary@{applicationName}_@{STEP_ID}.xml]]></value></Parameter>
<Parameter name="optionsFile" type="string" linked_module="" linked_parameter="" in="True" out="False" description="OptionsFile"><value><![CDATA[$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py]]></value></Parameter>
<Parameter name="extraOptionsLine" type="string" linked_module="" linked_parameter="" in="True" out="False" description="extraOptionsLines"><value><![CDATA[]]></value></Parameter>
<Parameter name="numberOfEventsInput" type="string" linked_module="" linked_parameter="" in="True" out="False" description="NumberOfEventsInput"><value><![CDATA[]]></value></Parameter>
<Parameter name="listoutput" type="list" linked_module="" linked_parameter="" in="True" out="False" description="StepOutputList"><value><![CDATA[[{'outputDataType': 'sim', 'outputDataName': '@{STEP_ID}.sim'}, {'outputDataType': 'gausshist', 'outputDataName': '@{applicationName}_@{STEP_ID}_Hist.root'}]]]></value></Parameter>
<Parameter name="extraPackages" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ExtraPackages"><value><![CDATA[AppConfig.v3r171;ProdConf]]></value></Parameter>
<Parameter name="BKStepID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="BKKStepID"><value><![CDATA[125080]]></value></Parameter>
<Parameter name="StepProcPass" type="string" linked_module="" linked_parameter="" in="True" out="False" description="StepProcessingPass"><value><![CDATA[Sim08a]]></value></Parameter>
<Parameter name="HistogramName" type="string" linked_module="" linked_parameter="" in="True" out="False" description="NameOfHistogram"><value><![CDATA[@{applicationName}_@{STEP_ID}_Hist.root]]></value></Parameter>
<Parameter name="optionsFormat" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdConf configuration"><value><![CDATA[]]></value></Parameter>
<Parameter name="CondDBTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ConditionDatabaseTag"><value><![CDATA[Sim08-20130503-1-vc-mu100]]></value></Parameter>
<Parameter name="DDDBTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="DetDescTag"><value><![CDATA[Sim08-20130503-1]]></value></Parameter>
<Parameter name="DQTag" type="string" linked_module="" linked_parameter="" in="True" out="False" description="DataQualityTag"><value><![CDATA[]]></value></Parameter>
<Parameter name="multiCore" type="string" linked_module="" linked_parameter="" in="True" out="False" description="MultiCore Flag"><value><![CDATA[N]]></value></Parameter>
<Parameter name="SystemConfig" type="string" linked_module="" linked_parameter="" in="True" out="False" description="system config"><value><![CDATA[x86_64-slc5-gcc43-opt]]></value></Parameter>
<Parameter name="mcTCK" type="string" linked_module="" linked_parameter="" in="True" out="False" description="TCK to be simulated"><value><![CDATA[]]></value></Parameter>
</StepInstance>
<StepInstance>
<type>Job_Finalization</type>
<name>finalization</name>
<descr_short></descr_short>
</StepInstance>
</Workflow>
"""

class MCSimulationTestingAgentTestCase( unittest.TestCase ):

  def setUp( self ):
    self.mockAM = MagicMock()
    self.agent = importlib.import_module( 'LHCbDIRAC.TransformationSystem.Agent.MCSimulationTestingAgent' )
    self.agent.AgentModule = self.mockAM
    self.agent = MCSimulationTestingAgent()
    self.agent.log = gLogger
    self.agent.log.setLevel( 'DEBUG' )
    self.transID = 1L
    self.tasks = [{'TargetSE': 'Unknown',
                   'TransformationID': 1L,
                   'LastUpdateTime': datetime.datetime( 2014, 7, 29, 12, 12, 13 ),
                   'RunNumber': 0L,
                   'CreationTime': datetime.datetime( 2014, 7, 29, 12, 12, 13 ),
                   'ExternalID': '0',
                   'ExternalStatus': 'Running',
                   'TaskID': 1L},
                  {'TargetSE': 'Unknown',
                   'TransformationID': 1L,
                   'LastUpdateTime': datetime.datetime( 2014, 7, 29, 12, 12, 13 ),
                   'RunNumber': 0L,
                   'CreationTime': datetime.datetime( 2014, 7, 29, 12, 12, 13 ),
                   'ExternalID': '0',
                   'ExternalStatus': 'Created',
                   'TaskID': 2L}]
    self.report = {'subject': 'MCSimulation Test Failure Report. TransformationID: ' + str( self.transID ),
                   'body': ['MCSimulation Test Failure Report. TransformationID: ' + str( self.transID ),
                            "",
                            "Transformation:",
                            "----------------------------------------------------------------------",
                            "TransformationID: " + str( self.transID ),
                            "TransformationName: transName",
                            "LastUpdate: 29/07/2014 13:06",
                            "Status: New",
                            "Description: description",
                            "TransformationFamily: 0",
                            "Plugin: Standard",
                            "Type: MCSimulation",
                            "AgentType: Manual",
                            "GroupSize: 1",
                            "MaxNumberOfTasks: 0",
                            "AuthorDN: /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sbidwell/CN=758039/CN=Simon Bidwell",
                            "TransformationGroup: General",
                            "InheritedFrom: 0",
                            "CreationDate: 29/07/2014 13:06",
                            "FileMask: ",
                            "EventsPerTask: 0",
                            "AuthorGroup: devGroup",
                            "",
                            "Number of Tasks: " + str( len( self.tasks ) ) ,
                            "Tasks:",
                            "----------------------------------------------------------------------",
                            "TaskID: 1",
                            "TargetSE: Unknown",
                            "LastUpdateTime: 29/07/2014 12:12",
                            "RunNumber: 0",
                            "CreationTime: 29/07/2014 12:12",
                            "ExternalID: 0",
                            "ExternalStatus: Running",
                            "",
                            "TaskID: 2",
                            "TargetSE: Unknown",
                            "LastUpdateTime: 29/07/2014 12:12",
                            "RunNumber: 0",
                            "CreationTime: 29/07/2014 12:12",
                            "ExternalID: 0",
                            "ExternalStatus: Created",
                            ""]}

    self.transClientMock = MagicMock()
    self.transClientMock.getTransformations.return_value = {'OK' : True,
                                                            'Value' : [{'Body': '',
                                                                       'LastUpdate': datetime.datetime( 2014, 7, 29, 13, 6, 8 ),
                                                                       'Status': 'New',
                                                                       'TransformationID': 1L,
                                                                       'Description': 'description',
                                                                       'TransformationFamily': 0,
                                                                       'Plugin': 'Standard',
                                                                       'Type': 'MCSimulation',
                                                                       'AgentType': 'Manual',
                                                                       'GroupSize': 1L,
                                                                       'LongDescription': 'longDescription',
                                                                       'MaxNumberOfTasks': 0L,
                                                                       'Hot': 0,
                                                                       'AuthorDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sbidwell/CN=758039/CN=Simon Bidwell',
                                                                       'TransformationName': 'transName',
                                                                       'TransformationGroup': 'General',
                                                                       'InheritedFrom': 0L,
                                                                       'CreationDate': datetime.datetime( 2014, 7, 29, 13, 6, 8 ),
                                                                       'FileMask': '',
                                                                       'EventsPerTask': 0L,
                                                                       'AuthorGroup': 'devGroup'}]}
    self.transClientMock.getStoredJobDescription.return_value = {'OK' : True, 'Value' : ( ( 1L, storedJobDescription ), )}
    self.transClientMock.setTransformationParameter.return_value = {'OK' : True}
    self.transClientMock.extendTransformation.return_value = {'OK' : True, 'Value' : [3L, 4L, 5L]}

    self.bkClientMock = MagicMock()
    self.bkClientMock.bulkJobInfo.return_value = {'OK': True,
                                                   'Value': {'Failed': [],
                                                    'Successful': {'/lhcb/production/DC06/phys-v2-lumi2/00001764/DST/0000/00001764_00000195_5.dst': {'ApplicationName': 'Brunel',
                                                      'ApplicationVersion': 'v30r14',
                                                      'CPUTIME': 1983.83,
                                                      'DIRACJobId': 1563495,
                                                      'DIRACVersion': 'v2r13 build 3',
                                                      'EventInputStat': None,
                                                      'ExecTime': 2079.81009412,
                                                      'FirstEventNumber': 1,
                                                      'JobId': 10987796,
                                                      'Location': 'LCG.Glasgow.uk',
                                                      'Name': '00001764_00000195_5',
                                                      'NumberOfEvents': 1,
                                                      'Production': 1764,
                                                      'StatisticsRequested': None,
                                                      'TotalLumonosity': 0,
                                                      'WNCACHE': '1024KB',
                                                      'WNCPUHS06': 0,
                                                      'WNCPUPOWER': None,
                                                      'WNMEMORY': '8195868kB',
                                                      'WNMODEL': 'DualCoreAMDOpteron(tm)Processor280',
                                                      'WORKERNODE': 'node046.beowulf.cluster'}}},
                                                   'rpcStub': (('Bookkeeping/BookkeepingManager',
                                                     {'delegatedDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sbidwell/CN=758039/CN=Simon Bidwell',
                                                      'delegatedGroup': 'lhcb_user',
                                                      'keepAliveLapse': 150,
                                                      'skipCACheck': False,
                                                      'timeout': 3600}),
                                                    'bulkJobInfo',
                                                    (['/lhcb/production/DC06/phys-v2-lumi2/00001764/DST/0000/00001764_00000195_5.dst'],))}

    self.notifyClientMock = MagicMock()
    self.notifyClientMock.sendMail.return_value = {'OK' : True, 'Value': "The mail was succesfully sent"}

    self.operationsMock = MagicMock()
    self.operationsMock.getValue.return_value = "sbidwell"

    self.agent.transClient = self.transClientMock
    self.agent.bkClient = self.bkClientMock
    self.agent.notifyClient = self.notifyClientMock
    self.agent.operations = self.operationsMock

  def tearDown(self):
    pass
  
  def test_create_report( self ):
    generated_report = self.agent._create_report( self.tasks )
    self.assertEqual(generated_report, self.report)
  
  def test_send_report( self ):
    res = self.agent._send_report( self.report )
    self.assertTrue( res['OK'] )

  def test_update_workflow( self ):
    CPUe = 1
    MCCpu = 25
    CPUe_xml = self.test_workflow = """<Parameter name="CPUe" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CPU time per event"><value><![CDATA[1]]></value></Parameter>\n"""
    max_e_xml = self.test_workflow = """<Parameter name="maxNumberOfEvents" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Maximum number of events to produce (Gauss)"><value><![CDATA[160000]]></value></Parameter>\n"""
    res = self.agent._update_workflow( self.transID, CPUe, MCCpu )
    self.assertTrue( res['OK'] )
    prod = Production()
    prod.LHCbJob.workflow = fromXMLString( res['Value'] )
    cpue_param = prod.LHCbJob.workflow.findParameter( 'CPUe' )
    max_e_param = prod.LHCbJob.workflow.findParameter( 'maxNumberOfEvents' )
    self.assertEqual( CPUe_xml, cpue_param.toXML() )
    self.assertEqual( max_e_xml, max_e_param.toXML() )

  @patch( "LHCbDIRAC.TransformationSystem.Agent.MCSimulationTestingAgent.getEventsToProduce" )
  def test_calculate_parameters( self, patch_mock ):
    expected_cpue = 2079.81009412
#     expected_max_e = 100
    # mock to make getEventsToProduce to return 100
#     patch_mock.return_value = expected_max_e
    res = self.agent._calculate_parameters( self.tasks )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['CPUe'], expected_cpue )
#     self.assertEqual( res['Value']['max_e'], expected_max_e )

  def test_extend_failed_tasks( self ):
    numberOfFailedTasks = 3
    res = self.agent._extend_failed_tasks( self.transID, numberOfFailedTasks )
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( MCSimulationTestingAgentTestCase)
  testResult = unittest.TextTestResult(verbosity = 2).run(suite)
