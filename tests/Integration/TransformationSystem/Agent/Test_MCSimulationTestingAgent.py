'''Integration Test class for the MCSimulationTestingAgent

   Test of the chain:
   MCSimulationTestingAgent -> TransformationClient -> TransformationManager -> TransformationDB
                            -> NotificationClient -> ...

   The method _calculate_parameters is unit tested in the Test_McSimulationTestingAgent.py file found in
   LHCbDIRAC/TransformationSystem/Agent/Test, but is mocked here because it needs entries in the bkClient
   to work.
'''

import unittest
from mock import Mock, patch

from LHCbDIRAC.TransformationSystem.Agent.MCSimulationTestingAgent import MCSimulationTestingAgent
from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry                    import getUserOption
from DIRAC.Core.Workflow.Workflow import fromXMLString

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

def _calculate_parameters_mock( self , input_params ):
  return {'OK' : True, 'Value': {'CPUe' : 100, 'max_e' : 100}}

@patch.object( MCSimulationTestingAgent, "_calculate_parameters", _calculate_parameters_mock )
class MCSimulationTestingAgentIntegrationTestCase( unittest.TestCase ):

  def setUp( self ):
    self.agent = MCSimulationTestingAgent( 'Transformation/MCSimulationTestingAgent', 'Transformation/MCSimulationTestingAgent' )
    self.agent.initialize()

    self.transClient = TransformationClient()

    self.transIDs = []

    res = self.transClient.addTransformation( 'transNameLHCb', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.transIDs.append( res['Value'] )
    res = self.transClient.addTransformation( 'transNameLHCb1', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.transIDs.append( res['Value'] )
    res = self.transClient.addTransformation( 'transNameLHCb2', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.transIDs.append( res['Value'] )
    res = self.transClient.addTransformation( 'transNameLHCb3', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.transIDs.append( res['Value'] )
    res = self.transClient.setTransformationParameter( self.transIDs[0], 'Status', 'Idle' )
    self.transClient.setTransformationParameter( self.transIDs[1], 'Status', 'Idle' )
    self.transClient.setTransformationParameter( self.transIDs[2], "Status", "Idle" )

    for transID in self.transIDs:
      self.transClient.addStoredJobDescription( transID, storedJobDescription )

  def tearDown( self ):
    for transID in self.transIDs:
      self.transClient.cleanTransformation( transID )
      self.transClient.deleteTransformation( transID )


  def test_update_workflow( self ):
    '''test to ensure the workflow is correctly updated with the given parameters
    '''
    CPUe = 1
    max_e = 1
    CPUe_xml = """<Parameter name="CPUe" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CPU time per event"><value><![CDATA[1]]></value></Parameter>\n"""
    max_e_xml = """<Parameter name="maxNumberOfEvents" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Maximum number of events to produce (Gauss only)"><value><![CDATA[1]]></value></Parameter>\n"""
    res = self.agent._update_workflow( self.transIDs[0], CPUe, max_e )
    self.assertTrue( res['OK'] )
    prod = Production()
    prod.LHCbJob.workflow = fromXMLString( res['Value'] )
    cpue_param = prod.LHCbJob.workflow.findParameter( 'CPUe' )
    max_e_param = prod.LHCbJob.workflow.findParameter( 'maxNumberOfEvents' )
    # assert the xml in the production parameter is the same as the xml strings above
    self.assertEqual( CPUe_xml, cpue_param.toXML() )
    self.assertEqual( max_e_xml, max_e_param.toXML() )


  def test_extend_failed_tasks( self ):
    '''test to ensure extend_failed_tasks extends a transformation by the correct number of tasks
    '''
    numberOfFailedTasks = 3
    res = self.agent._extend_failed_tasks( self.transIDs[0], numberOfFailedTasks )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationTasks( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( len( res['Value'] ), numberOfFailedTasks )
    self.assertTrue( res['OK'] )


  def test_update_transformations_table( self ):
    '''test to ensure that update_transformations_table updates the table with the correct body
       and sets the status to active as well as removing the storedJobDescription
    '''
    res = self.agent._update_transformations_table( self.transIDs[0], storedJobDescription )
    self.assertTrue( res['OK'] )
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( trans['Value'][0]['Body'], storedJobDescription )
    self.assertEqual( trans['Value'][0]['Status'], 'Active' )
    storedIDs = self.transClient.getStoredJobDescriptionIDs()['Value']
    self.assertNotIn( self.transIDs[0], storedIDs )

  def test_execute( self ):
    '''test for execute with multiple Idle transformations
    '''
    res = self.agent.execute()
    self.assertTrue( res['OK'] )
    res = self.transClient.getStoredJobDescriptionIDs()
    storedIDs = [transId[0] for transId in res['Value']]

    # are all idle (set in setUp) with no failed tasks before execute so after should no longer be in storedIDs
    self.assertNotIn( self.transIDs[0], storedIDs )
    self.assertNotIn( self.transIDs[1], storedIDs )
    self.assertNotIn( self.transIDs[2], storedIDs )

    # is not idle yet so should be in storedIDs
    self.assertIn( self.transIDs[3], storedIDs )

    # ensuring that the transformations all have the correct bodies after execute
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( fromXMLString( trans['Value'][0]['Body'] ), fromXMLString( storedJobDescription ) )
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[1]} )
    self.assertEqual( fromXMLString( trans['Value'][0]['Body'] ), fromXMLString( storedJobDescription ) )
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[2]} )
    self.assertEqual( fromXMLString( trans['Value'][0]['Body'] ), fromXMLString( storedJobDescription ) )
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[3]} )
    self.assertEqual( trans['Value'][0]['Body'], '' )


  def test_done_execute( self ):
    '''test for the scenario where every task for a transformation is Done
    '''
    res = self.transClient.extendTransformation( self.transIDs[0], 5 )
    for taskID in res['Value']:
      # setting task status's to done
      self.transClient.setTaskStatus( 'transNameLHCb', taskID, 'Done' )

    res = self.agent.execute()
    self.assertTrue( res['OK'] )
    res = self.transClient.getStoredJobDescriptionIDs()
    storedIDs = [transId[0] for transId in res['Value']]

    # ensuring transformation is removed from storedJobdescription
    self.assertNotIn( self.transIDs[0], storedIDs )

    # ensuring transformation has tyhe correct jobdescription and is set to active
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( fromXMLString( trans['Value'][0]['Body'] ), fromXMLString( storedJobDescription ) )
    self.assertEqual(trans['Value'][0]['Status'], 'Active')


  def test_failed_execute( self ):
    '''test the scenario where all tasks have failed
    '''
    res = self.transClient.extendTransformation( self.transIDs[0], 5 )
    for taskID in res['Value']:
      # set all 5 tasks to failed
      self.transClient.setTaskStatus( 'transNameLHCb', taskID, 'Failed' )

    # generate the report that should be created to check against the one actually created
    tasks = self.transClient.getTransformationTasks( condDict = {'TransformationID' : self.transIDs[0]} )
    report = self.agent._create_report(tasks['Value']) 
    username = self.agent.operations.getValue( "Shifter/ProductionManager/User" )
    email = getUserOption( username, "Email" )
    body = '\n'.join( report['body'] )
    notifyClientMock = Mock()
    notifyClientMock.sendMail.return_value = {'OK': True}
    self.agent.notifyClient = notifyClientMock
    
    res = self.agent.execute()
    self.assertTrue( res['OK'] )
    res = self.transClient.getStoredJobDescriptionIDs()
    storedIDs = [transId[0] for transId in res['Value']]
    self.assertIn( self.transIDs[0], storedIDs )
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( trans['Value'][0]['Status'], 'Idle' )

    # ensure the notifyClient is called with the correct email etc
    notifyClientMock.sendMail.assert_called_with( email, report['subject'], body, email )


  def test_partially_failed_execute( self ):
    '''test the scenario when only some tasks have failed
    '''
    res = self.transClient.extendTransformation( self.transIDs[0], 5 )
    for x in range( 0, 3 ):
      # make 3 of the 5 tasks be failed
      self.transClient.setTaskStatus( 'transNameLHCb', res['Value'][x], 'Failed' )

    # ensure execute returns True
    res = self.agent.execute()
    self.assertTrue( res['OK'] )

    # ensure the taskID is still in storedJobDescriptions as the task is still testing
    res = self.transClient.getStoredJobDescriptionIDs()
    storedIDs = [transId[0] for transId in res['Value']]
    self.assertIn( self.transIDs[0], storedIDs )

    # ensure the transformation is still Idle/this might need to be set back to active thinking about it
    trans = self.transClient.getTransformations( condDict = {"TransformationID" : self.transIDs[0]} )
    self.assertEqual( trans['Value'][0]['Status'], 'Idle' )

    # ensure that the amount of tasks for the transformation is the number of original tasks (5) + repeated tasks(3)
    tasks = self.transClient.getTransformationTasks( condDict = {'TransformationID' : self.transIDs[0]} )
    self.assertEqual( len( tasks['Value'] ), 8 )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( MCSimulationTestingAgentIntegrationTestCase )
  testResult = unittest.TextTestResult( verbosity = 2 ).run( suite )
