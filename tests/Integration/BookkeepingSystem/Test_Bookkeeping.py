"""

It tests the RAW data insert to the db.
It requires an Oracle database

"""
import unittest
import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


class DataInsertTestCase( unittest.TestCase ):
  """ Tests for the DB part of the RAWIntegrity system
  """

  def setUp( self ):
    super( DataInsertTestCase, self ).setUp()
    self.bk = BookkeepingClient()
    self.runnb = '1122'
    self.files = ['/lhcb/data/2016/RAW/Test/test/%s/000%s_test_%d.raw' % ( self.runnb, self.runnb, i ) for i in xrange( 5 )]
    self.xmlJob = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="Test" ConfigVersion="Test01" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="Production" Value="12" Type="Info"/>
  <TypedParameter Name="Name" Value="%runnb%" Type="Info"/>
  <TypedParameter Name="Location" Value="LHCb Online" Type="Info"/>
  <TypedParameter Name="ProgramName" Value="Moore" Type="Info"/>
  <TypedParameter Name="ProgramVersion" Value="v0r111" Type="Info"/>
  <TypedParameter Name="NumberOfEvents" Value="500321" Type="Info"/>
  <TypedParameter Name="ExecTime" Value="90000.0" Type="Info"/>
  <TypedParameter Name="JobStart"        Value="%jStart%" Type="Info"/>
  <TypedParameter Name="JobEnd"          Value="%jEnd%" Type="Info"/>
  <TypedParameter Name="FirstEventNumber"          Value="29" Type="Info"/>
  <TypedParameter Name="RunNumber"          Value="%runnb%" Type="Info"/>
  <TypedParameter Name="FillNumber"         Value="29" Type="Info"/>
  <TypedParameter Name="JobType"         Value="Merge" Type="Info"/>
  <TypedParameter Name="TotalLuminosity"         Value="121222.33" Type="Info"/>
  <TypedParameter Name="Tck"             Value="-2137784319" Type="Info"/>
  <TypedParameter Name="CondDB"             Value="xy" Type="Info"/>
 <TypedParameter Name="DDDB"             Value="xyz" Type="Info"/>
"""
    self.xmlFile = """
<Quality Group="Production Manager" Flag="Not Checked"/>
  <OutputFile Name="%filename%" TypeName="RAW" TypeVersion="1">
   <Parameter Name="MD5Sum" Value="24F71879BA006B91FB8ADC529ACB7CC6"/>
   <Parameter Name="EventTypeId" Value="30000000"/>
   <Parameter Name="EventStat" Value="9000"/>
   <Parameter Name="FileSize" Value="1640316586"/>
   <Parameter Name="Guid" Value="3cc1b6fe-63c8-11dd-852f-00188b8565aa"/>
   <Parameter Name="FullStat"          Value="429"/>
   <Parameter Name="CreationDate"        Value="%fileCreation%"/>
   <Parameter Name="Luminosity"          Value="1212.233"/>
 </OutputFile>
 """
  
    self.dqCond = """
  <DataTakingConditions>
  <Parameter Name="Description" Value="Real data"/>
  <Parameter Name="BeamCond"   Value="Collisions"/>
  <Parameter Name="BeamEnergy" Value="450.0"/>
  <Parameter Name="MagneticField" Value="Down"/>
  <Parameter Name="VELO"          Value="INCLUDED"/>
  <Parameter Name="IT"          Value="string"/>
  <Parameter Name="TT"          Value="string"/>
  <Parameter Name="OT"          Value=""/>
  <Parameter Name="RICH1"          Value="string"/>
  <Parameter Name="RICH2"          Value="string"/>
  <Parameter Name="SPD_PRS"          Value="string"/>
  <Parameter Name="ECAL"          Value="string"/>
  <Parameter Name="HCAL"          Value="string"/>
  <Parameter Name="MUON"          Value="string"/>
  <Parameter Name="L0"          Value="string"/>
  <Parameter Name="HLT"          Value="string"/>
  <Parameter Name="VeloPosition"          Value="Open"/>
</DataTakingConditions>
</Job>"""

class RAWDataInsert( DataInsertTestCase ):
  
  def test_echo( self ):
    
    """make sure we are able to use the bkk"
    """
    
    retVal = self.bk.echo( "Test" )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value'], "Test" )

  def test_sendXMLBookkeepingReport( self ):
    """
    insert a run to the db
    """
    currentTime = datetime.datetime.now()
    jobXML = self.xmlJob.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    jobXML = jobXML.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    jobXML = jobXML.replace( "%runnb%", self.runnb )
    jobXML = jobXML.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    jobXML = jobXML.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    xmlReport = jobXML
    for f in self.files:
      xmlReport += self.xmlFile.replace( "%filename%", f ).replace( '%fileCreation%', currentTime.strftime( '%Y-%m-%d %H:%M' ) ) 
    
    xmlReport += self.dqCond
    retVal = self.bk.sendXMLBookkeepingReport( xmlReport )
    self.assert_( retVal['OK'] )
  
  
class TestMethods( DataInsertTestCase ):
  
  
  def test_addFiles( self ):
    """
    add replica flag 
    """
    retVal = self.bk.addFiles( self.files )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Successful'] )
    self.assertEqual( retVal['Value']['Failed'], [] )
    self.assertEqual( retVal['Value']['Successful'], self.files )
    
    retVal = self.bk.addFiles( 'test.txt' )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['Successful'], [] )
    self.assertEqual( retVal['Value']['Failed'], ['test.txt'] )
    
  def test_fileMetadata( self ):
    """
    test the file metadata method
    """
    fileParams = ['GUID', 'ADLER32', 'FullStat', 'EventType', 'FileType',
                  'MD5SUM', 'VisibilityFlag', 'InsertTimeStamp', 'RunNumber',
                  'JobId', 'Luminosity', 'FileSize', 'EventStat', 'GotReplica',
                  'CreationDate', 'InstLuminosity', 'DataqualityFlag']
    retVal = self.bk.getFileMetadata( self.files )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Successful'] )
    self.assertEqual( retVal['Value']['Failed'], [] )
    self.assertEqual( len( retVal['Value']['Successful'].keys() ), len( self.files ) )
    self.assertEqual( sorted( retVal['Value']['Successful'].keys() ), sorted( self.files ) )
    # make sure the files has all parameters
    for fName in retVal['Value']['Successful']:
      self.assertEqual( sorted( retVal['Value']['Successful'][fName] ), sorted( fileParams ) )
    
    retVal = self.bk.getFileMetadata( 'test.txt' )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['Successful'], {} )
    self.assertEqual( retVal['Value']['Failed'], ['test.txt'] )
  
  def test_getRunFiles( self ):
    """
    retrieve all the files for a given run
    """
    fileParams = ['FullStat', 'Luminosity', 'FileSize', 'EventStat', 'GotReplica', 'GUID', 'InstLuminosity']
    retVal = self.bk.getRunFiles( int( self.runnb ) )
    self.assert_( retVal['OK'] )
    self.assertEqual( sorted( retVal['Value'].keys() ), sorted( self.files ) )
    for fName in retVal['Value']:
      self.assertEqual( sorted( retVal['Value'][fName].keys() ), sorted( fileParams ) )
  
  def test_getAvailableSteps( self ):
    """
    make sure the step is created
    """
    retVal = self.bk.getAvailableSteps( {"ApplicationName":"Moore",
                                         "ApplicationVersion":"v0r111",
                                         "ProcessingPadd":"/Real Data"} )
    
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                                                                            'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible',
                                                                            'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat', 'isMulticore',
                                                                            'SystemConfig', 'mcTCK', 'RuntimeProjects'] ) )
  
  def test_getAvailableFileTypes( self ):
    """
    retrieve the file types
    """
    
    retVal = self.bk.getAvailableFileTypes()
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    
  def test_Steps( self ):
    """
    insert a new step
    """
    paramNames = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
                  'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible',
                  'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat', 'isMulticore',
                  'SystemConfig', 'mcTCK', 'RuntimeProjects']
    
    step1 = {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '',
                      'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '',
                      'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)',
                      'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': '', 'DQTag': 'OK', 'OptionsFormat':'txt',
                      'isMulticore':'N', 'SystemConfig':'centos7', 'mcTCK':'ax1'},
             'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
             'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}]}
    
    retVal = self.bk.insertStep( step1 )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    stepid1 = retVal['Value']
    
    step2 = {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '',
                      'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '',
                      'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)',
                      'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': '', 'DQTag': 'OK', 'OptionsFormat':'txt',
                      'isMulticore':'N', 'SystemConfig':'centos7', 'mcTCK':'ax1'},
             'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
             'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],
             'RuntimeProjects':[{'StepId':stepid1}]}
    
    retVal = self.bk.insertStep( step2 )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    stepid2 = retVal['Value']
    
    retVal = self.bk.getAvailableSteps( {"StepId":stepid1} )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( paramNames ) )
    
    self.assertEqual( len( retVal['Value']['Records'] ), 1 )
    record = dict( zip( paramNames[1:-1], retVal['Value']['Records'][0][1:-1] ) )
    for stepParams in paramNames[1:-1]:
      if step1['Step'][stepParams]:
        self.assertEqual( record[stepParams], step1['Step'][stepParams] )
      else:
        self.assertEqual( record[stepParams], None )
    
    retVal = self.bk.getAvailableSteps( {"StepId":stepid2} )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( len( retVal['Value']['Records'] ), 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( paramNames ) )
    record = dict( zip( paramNames[1:-1], retVal['Value']['Records'][0][1:-1] ) )
    for stepParams in paramNames[1:-1]:
      if step2['Step'][stepParams]:
        self.assertEqual( record[stepParams], step2['Step'][stepParams] )
      else:
        self.assertEqual( record[stepParams], None )
    
    
    retVal = self.bk.getStepInputFiles( stepid1 )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( ['FileType', 'Visible'] ) )
        
    retVal = self.bk.getStepOutputFiles( stepid1 )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( ['FileType', 'Visible'] ) )
    
    retVal = self.bk.setStepInputFiles( stepid1, [{"FileType":"Test.DST", "Visible":"Y"}] )
    self.assert_( retVal['OK'] )  # make sure the change works
    retVal = self.bk.getStepInputFiles( stepid1 )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( ['FileType', 'Visible'] ) )
    self.assertEqual( retVal['Value']['Records'], [['Test.DST', 'Y']] )
    
    retVal = self.bk.setStepOutputFiles( stepid2, [{"FileType":"Test.DST", "Visible":"Y"}] )
    self.assert_( retVal['OK'] )  # make sure the change works
    retVal = self.bk.getStepOutputFiles( stepid2 )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( ['FileType', 'Visible'] ) )
    self.assertEqual( retVal['Value']['Records'], [['Test.DST', 'Y']] )
    
    step1['Step']['StepName'] = 'test1'
    step1['Step']['StepId'] = stepid1
    retVal = self.bk.updateStep( {'StepId':stepid1, 'StepName': 'test1'} )
    self.assert_( retVal['OK'] )
    # check after the modification
    retVal = self.bk.getAvailableSteps( {"StepId":stepid1} )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( sorted( retVal['Value']['ParameterNames'] ), sorted( paramNames ) )
    self.assertEqual( len( retVal['Value']['Records'] ), 1 )
    record = dict( zip( paramNames[1:-1], retVal['Value']['Records'][0][1:-1] ) )
    for stepParams in paramNames[1:-1]:
      if step1['Step'][stepParams]:
        self.assertEqual( record[stepParams], step1['Step'][stepParams] )
      else:
        self.assertEqual( record[stepParams], None )
        
    
    # at the end delete the steps
    retVal = self.bk.deleteStep( stepid2 )
    self.assert_( retVal['OK'] )
    
    retVal = self.bk.deleteStep( stepid1 )
    self.assert_( retVal['OK'] )
    
  def test_getAvailableConfigNames( self ):
    """
    Must have one configuration name
    """
    retVal = self.bk.getAvailableConfigNames()
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
  
  def test_getConfigVersions( self ):
    """
    The bookkeeping view is isued, we can not use the newly inserted configuration name: Test
    """
    retVal = self.bk.getConfigVersions( {"ConfigName":"MC"} )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 11 )
  
  def test_getConditions( self ):
    """
    
    Get the available configurations for a given bk dict
    
    """
    simParams = ['SimId', 'Description', 'BeamCondition', 'BeamEnergy', 'Generator', 'MagneticField', 'DetectorCondition', 'Luminosity', 'G4settings']
    dataParams = ['DaqperiodId', 'Description', 'BeamCondition', 'BeanEnergy', 'MagneticField', 'VELO', 'IT', 'TT', 'OT', 'RICH1', 'RICH2', 'SPD_PRS', 'ECAL', 'HCAL',
                  'MUON', 'L0', 'HLT', 'VeloPosition']
    
    retVal = self.bk.getConditions( {"ConfigName":"MC", "ConfigVersion":"2012"} )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( len( retVal['Value'] ), 2 )
    self.assertEqual( retVal['Value'][1]['TotalRecords'], 0 )
    self.assertEqual( retVal['Value'][1]['ParameterNames'], dataParams )
    self.assert_( retVal['Value'][0]['TotalRecords'] > 0 )
    self.assertEqual( retVal['Value'][0]['ParameterNames'], simParams )
    
    
  def test_getProcessingPass( self ):
    """
    
    Check the available processing passes for a given bk path. Again bkk view...
    
    """
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC", "ConfigVersion":"2012", } )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( len( retVal['Value'] ), 2 )
    self.assert_( retVal['Value'][0]['TotalRecords'] > 0 )
  
  def test_bookkeepingtree( self ):
    """
    Browse the bookkeeping database
    """
    bkQuery = {"ConfigName":"MC"}
    retVal = self.bk.getAvailableConfigNames()
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assert_( bkQuery['ConfigName'] in [cName[0] for cName in retVal['Value']['Records']] ) 
    
    retVal = self.bk.getConfigVersions( {"ConfigName":"MC"} )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 11 )
    
    retVal = self.bk.getConditions( {"ConfigName":"MC",
                                     "ConfigVersion":"2012"} )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['TotalRecords'], 2 )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"} )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['Records'][0][0], "Sim08a" )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
                                        "/Sim08a" )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['Records'][0][0], "Digi13" )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
                                        "/Sim08a/Digi13" )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['Records'][0][0], "Trig0x409f0045" )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
                                        "/Sim08a/Digi13/Trig0x409f0045" )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['Records'][0][0], "Reco14a" )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a" )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][0]['Records'][0][0], "Stripping20NoPrescalingFlagged" )
    
    retVal = self.bk.getProcessingPass( {"ConfigName":"MC",
                                         "ConfigVersion":"2012",
                                         "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged" )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value'][1]['Records'][0][0], 12442001 )
    
    retVal = self.bk.getFileTypes( {"ConfigName":"MC",
                                    "ConfigVersion":"2012",
                                    "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                                    "ProcessingPass":"/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged"} )
    
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( retVal['Value']['Records'][0][0], 'ALLSTREAMS.DST' )
    
    retVal = self.bk.getFiles( {"ConfigName":"MC",
                                "ConfigVersion":"2012",
                                "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                                "ProcessingPass":"/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
                                "FileType":"ALLSTREAMS.DST"} )
    
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 10 )
    
    retVal = self.bk.getFiles( {"ConfigName":"MC",
                                "ConfigVersion":"2012",
                                "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                                "ProcessingPass":"/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
                                "FileType":"ALLSTREAMS.DST",
                                "EventType":12442001} )
    
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 10 )
    
    retVal = self.bk.getFiles( {"ConfigName":"MC",
                                "ConfigVersion":"2012",
                                "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                                "ProcessingPass":"/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
                                "FileType":"ALLSTREAMS.DST",
                                "EventType":12442001,
                                "Visible": "N"} )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 0 )
    
    retVal = self.bk.getFiles( {"ConfigName":"MC",
                                "ConfigVersion":"2012",
                                "ConditionDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                                "ProcessingPass":"/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
                                "FileType":"ALLSTREAMS.DST",
                                "EventType":12442001,
                                "Visible": "All"} )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 10 )
  
  def test_getFiles1( self ):
    """
    This is used to test the getFiles method.
    """  
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
               'EventType': '30000000',
               'FileType': 'DST',
               'ProcessingPass': '/Sim01/Reco08',
               'Visible': 'Y',
               'ConfigVersion': 'MC10',
               'Quality': []}
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 301 )
    
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
               'EventType': '30000000',
               'FileType': 'DST',
               'ProcessingPass': '/Sim01/Reco08',
               'Visible': 'Y',
               'ConfigVersion': 'MC10',
               'Quality': "ALL"}
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 301 )
  
  def test_getFiles2( self ):
    """
    It is used to test the getFiles method
    """
    bkQuery = {'Production': 10917,
               'EventType': '30000000',
               'FileType': 'DST',
               'Visible': 'Y',
               'Quality': 'OK'}
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 301 )
    
    bkQuery.pop( 'Visible' )
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 301 )
    
    
  def test_getFiles3( self ):
    """
    It is used to test the getFiles method
    """
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
               'EventType': 28144012,
               'FileType': 'XGEN',
               'ProcessingPass': '/MC10Gen01',
               'Visible': 'Y',
               'ConfigVersion': 'MC10',
               'Quality': []}
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 200 )
    
  def test_getFiles4( self ):
    
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
               'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
               'Visible': 'N',
               'ConfigVersion': 'MC10'}
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 605 )
    
    bkQuery['Visible'] = 'Y'
    retVal = self.bk.getFileTypes( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( retVal['Value']['Records'][0][0], 'DST' )
  
               
  def test_getProductions2( self ):
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
               'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
               'Visible': 'N',
               'ConfigVersion': 'MC10'}
    
    retVal = self.bk.getProductions( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( retVal['Value']['Records'][0][0], 10713 )
    
    bkQuery['Visible'] = 'Y'
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( retVal['Value']['Records'][0][0], 10713 )
    
    
  def test_getFile5( self ):
    """
    Get list of file types
    """
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
               'EventType': 28144012,
               'ProcessingPass': '/MC10Gen01',
               'Visible': 'Y',
               'ConfigVersion': 'MC10',
               'Quality': []}
    
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 200 )
  
  def test_getProductions1( self ):
    """
    This is used to test the getFiles method.
    """  
    bkQuery = {'ConfigName': 'MC',
               'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
               'EventType': '30000000',
               'FileType': 'DST',
               'ProcessingPass': '/Sim01/Reco08',
               'Visible': 'Y',
               'ConfigVersion': 'MC10',
               'Quality': []}
    
    retVal = self.bk.getProductions( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 1 )
    self.assertEqual( retVal['Value']['Records'][0][0], 10917 )
  
  def test_getVisibleFilesWithMetadata1( self ):
    """
    This si used to test the ganga queries
    """
    
    bkQuery = {'ConfigName':'LHCb',
               'ConfigVesrion':'Collision12',
               'ProcessingPass':'/Real Data/Reco13a/Stripping19a',
               'FileType':'BHADRON.MDST',
               'Visible':'Y',
               'EventType':90000000,
               'DataTakingConditions':'Beam4000GeV-VeloClosed-MagDown',
               'DataQuality':'OK'}
    
    summary = {'EventInputStat': 56223169,
               'FileSize': 783.777593157,
               'InstLuminosity': 0,
               'Luminosity': 156438151.738,
               'Number Of Files': 439,
               'Number of Events': 68170375,
               'TotalLuminosity': 0}
    
    paramNames = ['TotalLuminosity', 'Luminosity', 'Fillnumber', 'EventInputStat', 'FileSize', 'EventStat', 'Runnumber', 'InstLuminosity', 'TCK']
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 439 )
    self.assertEqual( retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames )
                     
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 439 )
    
    bkQuery['RunNumber'] = [115055],
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 439 )
    
    bkQuery['TCK'] = ['0x95003d']
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 439 )
    
    
  def test_getVisibleFilesWithMetadata2( self ):
    """
    This si used to test the ganga queries
    Now test the run numbers
    """
    
    bkQuery = {'ConfigName': 'LHCb',
               'DataTakingConditions': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': 90000000,
               'FileType': 'EW.DST',
               'ProcessingPass': '/Real Data/Reco10/Stripping13b',
               'Visible': 'Y',
               'ConfigVersion': 'Collision11',
               'Quality': ['OK']}
    
    summary = {'TotalLuminosity': 0,
               'Number Of Files': 2316,
               'Luminosity': 175005777.674,
               'Number of Events': 69545408,
               'EventInputStat': 2177589640,
               'FileSize': 6315.58588745,
               'InstLuminosity': 0}
    
    paramNames = ['TotalLuminosity', 'Luminosity', 'Fillnumber', 'EventInputStat', 'FileSize', 'EventStat', 'Runnumber', 'InstLuminosity', 'TCK']
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 2316 )
    self.assertEqual( retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames )
    
    bkQuery['RunNumber'] = [90104, 92048, 87851]
    summary = {'TotalLuminosity': 0,
               'Number Of Files': 2,
               'Luminosity': 57308.7467796,
               'Number of Events': 17852,
               'EventInputStat': 889893,
               'FileSize': 2.148836844,
               'InstLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 2 )
    
    bkQuery.pop( 'RunNumber' )
    bkQuery['StartRun'] = 93102
    bkQuery['EndRun'] = 93407
    summary = {'TotalLuminosity': 0,
                'Number Of Files': 178,
                'Luminosity': 19492211.2845,
                'Number of Events': 8688923,
                'EventInputStat': 222920612,
                'FileSize': 740.377729246,
                'InstLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 178 )
    
    bkQuery.pop( 'StartRun' )
    bkQuery.pop( 'EndRun' )
    bkQuery['StartDate'] = "2011-06-15 19:15:25"
    summary = {'TotalLuminosity': 0,
                'Number Of Files': 127,
                'Luminosity': 11841384.7998,
                'Number of Events': 5229684,
                'EventInputStat': 135822190,
                'FileSize': 449.842646079,
                'InstLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 127 )
    
    bkQuery['EndDate'] = "2011-06-16 19:15:25"
    summary = {'TotalLuminosity': 0,
               'Number Of Files': 125,
               'Luminosity': 11832088.6133,
               'Number of Events': 5226863,
               'EventInputStat': 135629659,
               'FileSize': 449.439082551,
               'InstLuminosity': 0}
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 125 )
  
  def test_getVisibleFilesWithMetadata3( self ):
    """
    This si used to test the ganga queries
    Now test the MC datasets
    """
    bkQuery = {'ConfigName': 'MC',
                'SimulationConditions': 'Beam3500GeV-May2010-MagOff-Fix1',
                'EventType': '30000000',
                'FileType': 'DST',
                'ProcessingPass': '/Sim01/Reco08',
                'Visible': 'Y',
                'ConfigVersion': 'MC10',
                'Quality': ['OK']}
    
    summary = {'EventInputStat': 6020000,
               'FileSize': 468.227136723,
               'InstLuminosity': 0,
               'Luminosity': 0,
               'Number Of Files': 301,
               'Number of Events': 6020000,
               'TotalLuminosity': 0}
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 301 )
  
  def test_getFiles6( self ):
    
    """
    This is used to test the getFiles
    """
    bkQuery = {'ConfigName':'LHCb',
               'ConfigVesrion':'Collision12',
               'ProcessingPass':'/Real Data/Reco13a/Stripping19a',
               'FileType':'BHADRON.MDST',
               'Visible':'Y',
               'EventType':90000000,
               'ConditionDescription':'Beam4000GeV-VeloClosed-MagDown',
               'DataQuality':'OK'}
    
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 439 )
                         
    # now test the data datakingcondition
    bkQuery.pop( 'ConditionDescription' )
    bkQuery['DataTakingConditions'] = 'Beam4000GeV-VeloClosed-MagDown'
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 439 )
    
    bkQuery['RunNumber'] = [115055]
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 439 )
    
    bkQuery['TCK'] = ['0x95003d']
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 439 )
   
  def test_getFiles7( self ):
    """
    This si used to test the ganga queries
    Now test the run numbers
    """
    
    bkQuery = {'ConfigName': 'LHCb',
               'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': 90000000,
               'FileType': 'EW.DST',
               'ProcessingPass': '/Real Data/Reco10/Stripping13b',
               'Visible': 'Y',
               'ConfigVersion': 'Collision11',
               'Quality': ['OK']}
    
    retVal = self.bk.getFiles( bkQuery )
    self.assertEqual( len( retVal['Value'] ), 3223 )
    
    bkQuery['RunNumber'] = [90104, 92048, 87851]
    retVal = self.bk.getFiles( bkQuery )
    self.assertEqual( len( retVal['Value'] ), 3 )
    
    bkQuery.pop( 'RunNumber' )
    bkQuery['StartRun'] = 93102
    bkQuery['EndRun'] = 93407
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 187 )
    
    bkQuery.pop( 'StartRun' )
    bkQuery.pop( 'EndRun' )
    bkQuery['StartDate'] = "2011-06-15 19:15:25"
    
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 135 )
    
    bkQuery['EndDate'] = "2011-06-16 19:15:25"
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 127 )
  
  def test_getFiles8( self ):
    """
    This is used to test the ganga queries
    Now test the MC datasets
    """
    bkQuery = {'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
                'EventType': '30000000',
                'FileType': 'DST',
                'ProcessingPass': '/Sim01/Reco08',
                'Visible': 'Y',
                'ConfigVersion': 'MC10',
                'Quality': ['OK']}
    
   
    retVal = self.bk.getFiles( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( len( retVal['Value'] ), 301 )
  
  def getFilesWithMetadata1( self ):
    
    """
    This is used to test the getFiles
    """
    bkQuery = {'ConfigName':'LHCb',
               'ConfigVesrion':'Collision12',
               'ProcessingPass':'/Real Data/Reco13a/Stripping19a',
               'FileType':'BHADRON.MDST',
               'Visible':'Y',
               'EventType':90000000,
               'ConditionDescription':'Beam4000GeV-VeloClosed-MagDown',
               'DataQuality':'OK'}
    
    paramNames = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'FileType', 'RunNumber', 'FillNumber',
                  'FullStat', 'DataqualityFlag', 'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK', 'GUID',
                  'ADLER32', 'EventType', 'MD5SUM', 'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 439 )
    self.assertEqual( len( retVal['Value']['Records'] ), 439 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames )                         
    
    # now test the data datakingcondition
    bkQuery.pop( 'ConditionDescription' )
    bkQuery['DataTakingConditions'] = 'Beam4000GeV-VeloClosed-MagDown'
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 439 )
    self.assertEqual( len( retVal['Value']['Records'] ), 439 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames )  
        
    bkQuery['RunNumber'] = [115055]
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 439 )
    self.assertEqual( len( retVal['Value']['Records'] ), 439 )
    self.assertEqual( len( retVal['Value']['ParameterNames'] ), paramNames )  
        
    bkQuery['TCK'] = ['0x95003d']
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 439 )
    self.assertEqual( len( retVal['Value']['Records'] ), 439 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames )  
       
  def test_getFilesWithMetadata2( self ):
    """
    This si used to test the ganga queries
    Now test the run numbers
    """
    
    bkQuery = {'ConfigName': 'LHCb',
               'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': 90000000,
               'FileType': 'EW.DST',
               'ProcessingPass': '/Real Data/Reco10/Stripping13b',
               'Visible': 'Y',
               'ConfigVersion': 'Collision11',
               'Quality': ['OK']}
    
    paramNames = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'FileType', 'RunNumber', 'FillNumber',
                  'FullStat', 'DataqualityFlag', 'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK', 'GUID',
                  'ADLER32', 'EventType', 'MD5SUM', 'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
    
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 0 )
    self.assertEqual( retVal['Value']['TotalRecords'], 2314 )
    self.assertEqual( len( retVal['Value']['Records'] ), 2314 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames ) 
        
    bkQuery['RunNumber'] = [90104, 92048, 87851]
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 2 )
    self.assertEqual( len( retVal['Value']['Records'] ), 2 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames ) 
    
        
    bkQuery.pop( 'RunNumber' )
    bkQuery['StartRun'] = 93102
    bkQuery['EndRun'] = 93407
    
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 178 )
    self.assertEqual( len( retVal['Value']['Records'] ), 178 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames ) 
        
    bkQuery.pop( 'StartRun' )
    bkQuery.pop( 'EndRun' )
    bkQuery['StartDate'] = "2011-06-15 19:15:25"
    
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 125 )
    self.assertEqual( len( retVal['Value']['Records'] ), 125 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames ) 
    
    
    bkQuery['EndDate'] = "2011-06-16 19:15:25"
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 125 )
    self.assertEqual( len( retVal['Value']['Records'] ), 125 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames ) 
    
  
  def test_getFilesWithMetadata3( self ):
    """
    This is used to test the ganga queries
    Now test the MC datasets
    """
    bkQuery = {'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
                'EventType': '30000000',
                'FileType': 'DST',
                'ProcessingPass': '/Sim01/Reco08',
                'Visible': 'Y',
                'ConfigVersion': 'MC10',
                'Quality': ['OK']}
    
    paramNames = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'FileType', 'RunNumber', 'FillNumber',
                  'FullStat', 'DataqualityFlag', 'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK', 'GUID',
                  'ADLER32', 'EventType', 'MD5SUM', 'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
   
    retVal = self.bk.getFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['TotalRecords'], 301 )
    self.assertEqual( len( retVal['Value']['Records'] ), 301 )
    self.assertEqual( retVal['Value']['ParameterNames'], paramNames )
    
    
class TestRemoveFiles( DataInsertTestCase ):
  
  def test_removeFiles( self ):
    """
    Set the replica flag to no
    """
    
    retVal = self.bk.removeFiles( self.files )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Successful'] )
    self.assertEqual( retVal['Value']['Failed'], [] )
    self.assertEqual( retVal['Value']['Successful'], self.files )
    
    retVal = self.bk.removeFiles( 'test.txt' )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value']['Successful'], [] )
    self.assertEqual( retVal['Value']['Failed'], ['test.txt'] )

class MCInsertTestCase( unittest.TestCase ):
  """ Tests for the DB part of the RAWIntegrity system
  """

  def setUp( self ):
    super( MCInsertTestCase, self ).setUp()
    self.bk = BookkeepingClient()
    self.simCondDict = {"SimDescription":"Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                        "BeamCond": "beta*~3m, zpv=25.7mm, xAngle=0.236mrad and yAngle=0.100mrad",
                        "BeamEnergy": "4000 GeV",
                        "Generator":"Pythia8",
                        "MagneticField":"1",
                        "DetectorCond":"2012, Velo Closed around offset beam",
                        "Luminosity":"pp collisions nu = 2.5, no spillover",
                        "G4settings":"specified in sim step",
                        "Visible": 'Y'}
    self.productionSteps = {"SimulationConditions" : "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                            "ConfigName" : "MC",
                            "ConfigVersion" :"2012",
                            "Production" : 56438,
                            "Steps":[]}
    
    self.xmlStep1 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                      <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                      <TypedParameter Name="CPUTIME" Type="Info" Value="36196.1"/>
                      <TypedParameter Name="ExecTime" Type="Info" Value="36571.0480781"/>
                      <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                      <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                      <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                      <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                      <TypedParameter Name="WNMEMORY" Type="Info" Value="1667656.0"/>
                      <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                      <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                      <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                      <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_1"/>
                      <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                      <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                      <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                      <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                      <TypedParameter Name="ProgramName" Type="Info" Value="Gauss"/>
                      <TypedParameter Name="ProgramVersion" Type="Info" Value="v49r5"/>
                      <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                      <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                      <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                      <TypedParameter Name="StepID" Type="Info" Value="130277"/>
                      <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                      <OutputFile Name="/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_1.sim" TypeName="SIM" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="862802861"/>
                              <Parameter Name="MD5Sum" Value="ae647981ea419cc9f8e8fa0a2d6bfd3d"/>
                              <Parameter Name="Guid" Value="546014C4-55C6-E611-8E94-02163E00F6B2"/>
                      </OutputFile>
                      <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_1.log" TypeName="LOG" TypeVersion="1">
                              <Parameter Name="FileSize" Value="319867"/>
                              <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_1.log"/>
                              <Parameter Name="MD5Sum" Value="e4574c9083d1163d43ba6ac033cbd769"/>
                              <Parameter Name="Guid" Value="E4574C90-83D1-163D-43BA-6AC033CBD769"/>
                      </OutputFile>
                      <SimulationCondition>
                              <Parameter Name="SimDescription" Value="Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"/>
                      </SimulationCondition>
                </Job>
             """
    
    self.xmlStep2 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="234.52"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="342.997269869"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="1297688.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_2"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Boole"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v30r1"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="129462"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_1.sim"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_2.digi" TypeName="DIGI" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="241904920"/>
                              <Parameter Name="MD5Sum" Value="a76f78f3c86cc36c663d18b4e16861b9"/>
                              <Parameter Name="Guid" Value="7EF857D2-AAC6-E611-BBBC-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_2.log" TypeName="LOG" TypeVersion="1">
                              <Parameter Name="FileSize" Value="131897"/>
                              <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_2.log"/>
                              <Parameter Name="MD5Sum" Value="2d9cdd2116535cd484cf06cdb1620d75"/>
                              <Parameter Name="Guid" Value="2D9CDD21-1653-5CD4-84CF-06CDB1620D75"/>
                    </OutputFile>
                    </Job>
                    """
    self.xmlStep3 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="521.94"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="576.953828096"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="899072.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_3"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="125877"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_2.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_3.digi" TypeName="DIGI" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="164753549"/>
                              <Parameter Name="MD5Sum" Value="a47bd5214a02b77f2507e0f4dd0b1fb5"/>
                              <Parameter Name="Guid" Value="6A6A5873-ABC6-E611-A680-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_3.log" TypeName="LOG" TypeVersion="1">
                              <Parameter Name="FileSize" Value="57133"/>
                              <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_3.log"/>
                              <Parameter Name="MD5Sum" Value="c62640e23c464305ff1c3b7b58b3027c"/>
                              <Parameter Name="Guid" Value="C62640E2-3C46-4305-FF1C-3B7B58B3027C"/>
                    </OutputFile>
                    </Job>
                    """
     
    self.xmlStep4 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="677.39"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="836.60585618"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="1918032.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_4"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r2p1"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="129905"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_3.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_4.digi" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="159740940"/>
                                <Parameter Name="MD5Sum" Value="b062307166b1a8e4fb905d3fb38394c7"/>
                                <Parameter Name="Guid" Value="48911F46-ADC6-E611-BD04-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_4.log" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="1621948"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_4.log"/>
                                <Parameter Name="MD5Sum" Value="ea8bc998c1905a1c6ff192393a931766"/>
                                <Parameter Name="Guid" Value="EA8BC998-C190-5A1C-6FF1-92393A931766"/>
                    </OutputFile>
                    </Job>                                   
                    """
    self.xmlStep5 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">               
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="494.27"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="617.996832132"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="692064.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_5"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="125881"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_4.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_5.digi" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="166543538"/>
                                <Parameter Name="MD5Sum" Value="3f15ea07bc80df0e8fd7a00bf21bf426"/>
                                <Parameter Name="Guid" Value="E88994D2-AEC6-E611-9D2C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_5.log" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="30967"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_5.log"/>
                                <Parameter Name="MD5Sum" Value="bbe1d585f4961281968c48ed6f115f98"/>
                                <Parameter Name="Guid" Value="BBE1D585-F496-1281-968C-48ED6F115F98"/>
                    </OutputFile>
                    </Job>
                    """               
    self.xmlStep6 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">               
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="518.02"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="585.730292082"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="899044.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_6"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="125878"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_5.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_6.digi" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="166994688"/>
                                <Parameter Name="MD5Sum" Value="e1d47cd7962d2dc4181508ab26b19fba"/>
                                <Parameter Name="Guid" Value="58DB8F37-B0C6-E611-9B3C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_6.log" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="56250"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_6.log"/>
                                <Parameter Name="MD5Sum" Value="6521d54c12608adc7b06c92e43d7d824"/>
                                <Parameter Name="Guid" Value="6521D54C-1260-8ADC-7B06-C92E43D7D824"/>
                    </OutputFile>
                    </Job>
                    """               
    self.xmlStep7 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="641.7"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="709.81375289"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="2001584.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_7"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r6"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="130192"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_6.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_7.digi" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="161641611"/>
                                <Parameter Name="MD5Sum" Value="f7f2d353164382712bec0ddfc46943ec"/>
                                <Parameter Name="Guid" Value="EAB2A0D4-B1C6-E611-A70C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_7.log" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="1709809"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_7.log"/>
                                <Parameter Name="MD5Sum" Value="a2209db13ee25ba252c6c52839232999"/>
                                <Parameter Name="Guid" Value="A2209DB1-3EE2-5BA2-52C6-C52839232999"/>
                    </OutputFile>
                    </Job>
                    """
    self.xmlStep8 = """<?xml version="1.0" encoding="ISO-8859-1"?>
                    <!DOCTYPE Job SYSTEM "book.dtd">
                    <Job ConfigName="MC" ConfigVersion="2012" Date="%jDate%" Time="%jTime%">
                    <TypedParameter Name="CPUTIME" Type="Info" Value="472.93"/>
                    <TypedParameter Name="ExecTime" Type="Info" Value="493.59373498"/>
                    <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
                    <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
                    <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
                    <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
                    <TypedParameter Name="WNMEMORY" Type="Info" Value="700256.0"/>
                    <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
                    <TypedParameter Name="Production" Type="Info" Value="00056438"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_8"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="126908"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_7.digi"/>
                    <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_8.digi" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="168778046"/>
                                <Parameter Name="MD5Sum" Value="391359b6a37856985b831c09e0653427"/>
                                <Parameter Name="Guid" Value="342F831B-B3C6-E611-94AA-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_8.log" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="31116"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_8.log"/>
                                <Parameter Name="MD5Sum" Value="0a622440c036b46811912e48ceee076f"/>
                                <Parameter Name="Guid" Value="0A622440-C036-B468-1191-2E48CEEE076F"/>
                    </OutputFile>
                    </Job>
                    """         

class MCProductionRegistration ( MCInsertTestCase ):
  """
  Test all the methods, which are used to register a production to the bkk. To register a 
  production to db requites:
  -existance of the simulation conditions
  -steps
  -production
  """
  def test_echo( self ):
    
    """make sure we are able to use the bkk"
    """
    
    retVal = self.bk.echo( "Test" )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value'], "Test" )
  
  def test_insertSimConditions( self ):
    """
    register a simulatiuon condition to the db
    """
    retVal = self.bk.insertSimConditions( self.simCondDict )
    if retVal["OK"]:
      self.assert_( retVal['OK'] )
    else:
      self.assert_( 'unique constraint' in retVal["Message"] )
  
  def test_registerProduction( self ):
    """
    insert all steps which will be used by the production and register the production
    """
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Gauss',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v49r5',
                                           'ExtraPackages': 'AppConfig.v3r277;DecFiles.v29r10',
                                           'StepName': 'Sim09b - 2012 - MU - Pythia8',
                                           'ProcessingPass': 'Sim09b',
                                           'isMulticore' : 'N',
                                           'Visible': 'Y',
                                           'DDDB': 'dddb-20150928',
                                           'SystemConfig':'x86_64-slc6-gcc48-opt',
                                           'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$APPCONFIGOPTS/Gauss/DataType-2012.py;$APPCONFIGOPTS/Gauss/RICHRandomHits.py;$APPCONFIGOPTS/Gauss/NoPacking.py;$DECFILESROOT/options/@{eventType}.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                           'CONDDB': 'sim-20160321-2-vc-mu100'},
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'SIM'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'Y'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Boole',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v30r1',
                                           'ExtraPackages': 'AppConfig.v3r266',
                                           'StepName': 'Digi14a for 2012 (to use w Sim09)',
                                           'ProcessingPass': 'Digi14a',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'SystemConfig':'x86_64-slc6-gcc48-opt',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/NoPacking.py;$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'SIM'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Moore',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v20r4',
                                           'ExtraPackages': 'AppConfig.v3r200',
                                           'StepName': 'L0 emulation - TCK 003d',
                                           'ProcessingPass': 'L0Trig0x003d',
                                           'OptionsFormat':'l0app',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'SystemConfig':'x86_64-slc6-gcc48-opt',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;$APPCONFIGOPTS/L0App/L0AppTCK-0x003d.py;$APPCONFIGOPTS/L0App/DataType-2012.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Moore',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v14r2p1',
                                           'ExtraPackages': 'AppConfig.v3r288',
                                           'StepName': 'TCK-0x4097003d Flagged MC - 2012 - to be used in multipleTCKs',
                                           'ProcessingPass': 'Trig0x4097003d',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep.py;$APPCONFIGOPTS/Conditions/TCK-0x4097003d.py;$APPCONFIGOPTS/Moore/DataType-2012.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Noether',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v1r4',
                                           'ExtraPackages': 'AppConfig.v3r200',
                                           'StepName': 'Move TCK-0x4097003d from default location',
                                           'ProcessingPass': 'MoveTCK0x4097003d',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/Moore/MoveTCK.py;$APPCONFIGOPTS/Moore/MoveTCK-0x4097003d.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Moore',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v20r4',
                                           'ExtraPackages': 'AppConfig.v3r200',
                                           'StepName': 'L0 emulation - TCK 0042',
                                           'ProcessingPass': 'L0Trig0x0042',
                                           'OptionsFormat':'l0a',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;$APPCONFIGOPTS/L0App/L0AppTCK-0x0042.py;$APPCONFIGOPTS/L0App/DataType-2012.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Moore',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v14r6',
                                           'ExtraPackages': 'AppConfig.v3r300',
                                           'StepName': 'TCK-0x40990042 Flagged MC - 2012 - to be used in multipleTCKs',
                                           'ProcessingPass': 'Trig0x40990042',
                                           'OptionsFormat':'l0a',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep.py;$APPCONFIGOPTS/Conditions/TCK-0x40990042.py;$APPCONFIGOPTS/Moore/DataType-2012.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.insertStep( {'Step': {'ApplicationName': 'Noether',
                                           'Usable': 'Yes',
                                           'StepId': '',
                                           'ApplicationVersion': 'v1r4',
                                           'ExtraPackages': 'AppConfig.v3r200',
                                           'StepName': 'Move TCK-0x40990042 from default location',
                                           'ProcessingPass': 'MoveTCK0x40990042',
                                           'OptionsFormat':'l0a',
                                           'isMulticore' : 'N',
                                           'Visible': 'N',
                                           'DDDB': 'fromPreviousStep',
                                           'OptionFiles': '$APPCONFIGOPTS/Moore/MoveTCK.py;$APPCONFIGOPTS/Moore/MoveTCK-0x40990042.py',
                                           'CONDDB': 'fromPreviousStep'},
                                  'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
                                  'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]} )
    
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value'] > 0 )
    self.productionSteps['Steps'].append( {'StepId':retVal['Value'], 'Visible':'N'} )
    
    retVal = self.bk.addProduction( self.productionSteps )
    self.assert_( retVal['OK'] )
    
    
class MCXMLReportInsert( MCInsertTestCase ):
  
  def test_echo( self ):
    
    """make sure we are able to use the bkk"
    """
    
    retVal = self.bk.echo( "Test" )
    self.assert_( retVal['OK'] )
    self.assertEqual( retVal['Value'], "Test" )

  def test_sendXMLBookkeepingReport( self ):
    """
    insert a run to the db
    """
    currentTime = datetime.datetime.now()
    step1 = self.xmlStep1.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step1 = step1.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step1 = step1.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step1 = step1.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step1 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step2 = self.xmlStep2.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step2 = step2.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step2 = step2.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step2 = step2.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step2 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step3 = self.xmlStep3.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step3 = step3.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step3 = step3.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step3 = step3.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step3 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step4 = self.xmlStep4.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step4 = step4.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step4 = step4.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step4 = step4.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step4 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step5 = self.xmlStep5.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step5 = step5.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step5 = step5.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step5 = step5.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step5 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step6 = self.xmlStep6.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step6 = step6.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step6 = step6.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step6 = step6.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step6 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step7 = self.xmlStep7.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step7 = step7.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step7 = step7.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step7 = step7.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step7 )
    self.assert_( retVal['OK'] )
    
    currentTime = datetime.datetime.now()
    step8 = self.xmlStep8.replace( "%jDate%", currentTime.strftime( '%Y-%m-%d' ) )
    step8 = step8.replace( "%jTime%", currentTime.strftime( '%H:%M' ) )
    step8 = step8.replace( "%jStart%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    step8 = step8.replace( "%jEnd%", currentTime.strftime( '%Y-%m-%d %H:%M' ) )
    retVal = self.bk.sendXMLBookkeepingReport( step8 )
    self.assert_( retVal['OK'] )
    
class MCProductionTest ( MCInsertTestCase ):
  
  """
  Test the existence of the inserted data. 
  """
  
  def test_getSimConditions( self ):
    """
    check the existence of the sim cond
    """
    retVal = self.bk.getSimConditions()
    self.assert_( retVal['OK'] )
    self.assert_( len( retVal['Value'] ) > 1 )
    self.assert_( self.simCondDict['SimDescription'] in ( i[1] for i in retVal['Value'] ) )
  
    
if __name__ == '__main__':
  
  mcTestSuite = unittest.defaultTestLoader.loadTestsFromTestCase( MCProductionRegistration )
  mcTestSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCProductionTest ) )
  # mcTestSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCXMLReportInsert ) )
  unittest.TextTestRunner( verbosity = 2, failfast = True ).run( mcTestSuite )
  # suite = unittest.defaultTestLoader.loadTestsFromTestCase( RAWDataInsert )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMethods ) )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestRemoveFiles ) )
  # unittest.TextTestRunner( verbosity = 2, failfast = True ).run( suite )
  
