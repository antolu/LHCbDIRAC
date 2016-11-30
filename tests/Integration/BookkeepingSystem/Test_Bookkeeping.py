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
               'ConditionDescription':'Beam4000GeV-VeloClosed-MagDown',
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
                     
    # now test the data takingcondition
    bkQuery.pop( 'ConditionDescription' )
    bkQuery['DataTakingConditions'] = 'Beam4000GeV-VeloClosed-MagDown'
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
               'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
               'EventType': 90000000,
               'FileType': 'EW.DST',
               'ProcessingPass': '/Real Data/Reco10/Stripping13b',
               'Visible': 'Y',
               'ConfigVersion': 'Collision11',
               'Quality': ['OK']}
    
    summary = {'EventInputStat': 2673532842,
               'FileSize': 7551.21445591,
               'InstLuminosity': 0,
               'Luminosity': 212911143.681,
               'Number Of Files': 3223,
               'Number of Events': 78548304,
               'TotalLuminosity': 0}
    
    paramNames = ['TotalLuminosity', 'Luminosity', 'Fillnumber', 'EventInputStat', 'FileSize', 'EventStat', 'Runnumber', 'InstLuminosity', 'TCK']
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 3223 )
    self.assertEqual( retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames )
    
    bkQuery['RunNumber'] = [90104, 92048, 87851]
    summary = {'EventInputStat': 1214023,
              'FileSize': 3.028363003,
              'InstLuminosity': 0,
              'Luminosity': 78038.8082125,
              'Number Of Files': 3,
              'Number of Events': 24213,
              'TotalLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 3 )
    
    bkQuery.pop( 'RunNumber' )
    bkQuery['StartRun'] = 93102
    bkQuery['EndRun'] = 93407
    summary = {'TotalLuminosity': 0, 
               'Number Of Files': 187, 
               'Luminosity': 20677680.1721, 
               'Number of Events': 9210890, 
               'EventInputStat': 235554627, 
               'FileSize': 783.770696193, 
               'InstLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 187 )
    
    bkQuery.pop( 'StartRun' )
    bkQuery.pop( 'EndRun' )
    bkQuery['StartDate'] = "2011-06-15 19:15:25"
    summary = {'EventInputStat': 138749890,
               'FileSize': 459.354464625,
               'InstLuminosity': 0,
               'Luminosity': 12092594.3009,
               'Number Of Files': 135,
               'Number of Events': 5340551,
               'TotalLuminosity': 0}
    
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 135 )
    
    bkQuery['EndDate'] = "2011-06-16 19:15:25"
    summary = {'EventInputStat': 138232621,
               'FileSize': 458.469704932,
               'InstLuminosity': 0,
               'Luminosity': 12076709.4593,
               'Number Of Files': 127,
               'Number of Events': 5334368,
               'TotalLuminosity': 0}
    retVal = self.bk.getVisibleFilesWithMetadata( bkQuery )
    self.assert_( retVal['OK'] )
    self.assert_( retVal['Value']['Summary'] )
    self.assert_( retVal['Value']['LFNs'] )
    self.assertEqual( retVal['Value']['Summary'], summary )
    self.assertEqual( len( retVal['Value']['LFNs'] ), 127 )
  
  def test_getVisibleFilesWithMetadata3( self ):
    """
    This si used to test the ganga queries
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

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RAWDataInsert )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMethods ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestRemoveFiles ) )
  unittest.TextTestRunner( verbosity = 2, failfast = True ).run( suite )
  
