"""

This test is used to test the insert

"""
import random
import time
import datetime

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}
    self.production = 2
    self.file1 = "/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_1_%rndfile%.sim"
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
                      <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                      <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                      <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                      <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                      <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                      <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                      <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                      <TypedParameter Name="ProgramName" Type="Info" Value="Gauss"/>
                      <TypedParameter Name="ProgramVersion" Type="Info" Value="v49r5"/>
                      <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                      <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                      <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                      <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                      <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                      <OutputFile Name="%outputfile1%" TypeName="SIM" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="862802861"/>
                              <Parameter Name="MD5Sum" Value="ae647981ea419cc9f8e8fa0a2d6bfd3d"/>
                              <Parameter Name="Guid" Value="546014C4-55C6-E611-8E94-02163E00F6B2"/>
                      </OutputFile>
                      <OutputFile Name="%logfile1%" TypeName="LOG" TypeVersion="1">
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
    self.file2 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_2_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Boole"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v30r1"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile1%"/>
                    <OutputFile Name="%outputfile2%" TypeName="DIGI" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="241904920"/>
                              <Parameter Name="MD5Sum" Value="a76f78f3c86cc36c663d18b4e16861b9"/>
                              <Parameter Name="Guid" Value="7EF857D2-AAC6-E611-BBBC-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile2%" TypeName="LOG" TypeVersion="1">
                              <Parameter Name="FileSize" Value="131897"/>
                              <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_2.log"/>
                              <Parameter Name="MD5Sum" Value="2d9cdd2116535cd484cf06cdb1620d75"/>
                              <Parameter Name="Guid" Value="2D9CDD21-1653-5CD4-84CF-06CDB1620D75"/>
                    </OutputFile>
                    </Job>
                    """
    self.file3 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_3_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile2%"/>
                    <OutputFile Name="%outputfile3%" TypeName="DIGI" TypeVersion="ROOT">
                              <Parameter Name="EventTypeId" Value="11104131"/>
                              <Parameter Name="EventStat" Value="411"/>
                              <Parameter Name="FileSize" Value="164753549"/>
                              <Parameter Name="MD5Sum" Value="a47bd5214a02b77f2507e0f4dd0b1fb5"/>
                              <Parameter Name="Guid" Value="6A6A5873-ABC6-E611-A680-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile3%" TypeName="LOG" TypeVersion="1">
                              <Parameter Name="FileSize" Value="57133"/>
                              <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_3.log"/>
                              <Parameter Name="MD5Sum" Value="c62640e23c464305ff1c3b7b58b3027c"/>
                              <Parameter Name="Guid" Value="C62640E2-3C46-4305-FF1C-3B7B58B3027C"/>
                    </OutputFile>
                    </Job>
                    """
    self.file4 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_4_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r2p1"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile3%"/>
                    <OutputFile Name="%outputfile4%" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="159740940"/>
                                <Parameter Name="MD5Sum" Value="b062307166b1a8e4fb905d3fb38394c7"/>
                                <Parameter Name="Guid" Value="48911F46-ADC6-E611-BD04-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile4%" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="1621948"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_4.log"/>
                                <Parameter Name="MD5Sum" Value="ea8bc998c1905a1c6ff192393a931766"/>
                                <Parameter Name="Guid" Value="EA8BC998-C190-5A1C-6FF1-92393A931766"/>
                    </OutputFile>
                    </Job>
                    """
    self.file5 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_5_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile4%"/>
                    <OutputFile Name="%outputfile5%" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="166543538"/>
                                <Parameter Name="MD5Sum" Value="3f15ea07bc80df0e8fd7a00bf21bf426"/>
                                <Parameter Name="Guid" Value="E88994D2-AEC6-E611-9D2C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile5%" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="30967"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_5.log"/>
                                <Parameter Name="MD5Sum" Value="bbe1d585f4961281968c48ed6f115f98"/>
                                <Parameter Name="Guid" Value="BBE1D585-F496-1281-968C-48ED6F115F98"/>
                    </OutputFile>
                    </Job>
                    """
    self.file6 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_6_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile5%"/>
                    <OutputFile Name="%outputfile6%" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="166994688"/>
                                <Parameter Name="MD5Sum" Value="e1d47cd7962d2dc4181508ab26b19fba"/>
                                <Parameter Name="Guid" Value="58DB8F37-B0C6-E611-9B3C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile6%" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="56250"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_6.log"/>
                                <Parameter Name="MD5Sum" Value="6521d54c12608adc7b06c92e43d7d824"/>
                                <Parameter Name="Guid" Value="6521D54C-1260-8ADC-7B06-C92E43D7D824"/>
                    </OutputFile>
                    </Job>
                    """
    self.file7 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_7_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r6"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile6%"/>
                    <OutputFile Name="%outputfile7%" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="161641611"/>
                                <Parameter Name="MD5Sum" Value="f7f2d353164382712bec0ddfc46943ec"/>
                                <Parameter Name="Guid" Value="EAB2A0D4-B1C6-E611-A70C-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile7%" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="1709809"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_7.log"/>
                                <Parameter Name="MD5Sum" Value="a2209db13ee25ba252c6c52839232999"/>
                                <Parameter Name="Guid" Value="A2209DB1-3EE2-5BA2-52C6-C52839232999"/>
                    </OutputFile>
                    </Job>
                    """
    self.file8 = "/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_8_%rndfile%.digi"
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
                    <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
                    <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
                    <TypedParameter Name="Name" Type="Info" Value="%jobname%"/>
                    <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
                    <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
                    <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
                    <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
                    <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
                    <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
                    <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
                    <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
                    <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
                    <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
                    <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
                    <InputFile Name="%inputfile7%"/>
                    <OutputFile Name="%outputfile8%" TypeName="DIGI" TypeVersion="ROOT">
                                <Parameter Name="EventTypeId" Value="11104131"/>
                                <Parameter Name="EventStat" Value="411"/>
                                <Parameter Name="FileSize" Value="168778046"/>
                                <Parameter Name="MD5Sum" Value="391359b6a37856985b831c09e0653427"/>
                                <Parameter Name="Guid" Value="342F831B-B3C6-E611-94AA-02163E00F6B2"/>
                    </OutputFile>
                    <OutputFile Name="%logfile8%" TypeName="LOG" TypeVersion="1">
                                <Parameter Name="FileSize" Value="31116"/>
                                <Replica Location="Web" Name="http://lhcb-logs.cern.ch/storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_8.log"/>
                                <Parameter Name="MD5Sum" Value="0a622440c036b46811912e48ceee076f"/>
                                <Parameter Name="Guid" Value="0A622440-C036-B468-1191-2E48CEEE076F"/>
                    </OutputFile>
                    </Job>
                    """

  def __prepareXML(self):
    steps = []
    jobStart = datetime.datetime.now()
    jobStart = jobEnd = jobStart.replace(second=0, microsecond=0)
    currentTime = datetime.datetime.now()

    seq = "%d" % random.randint(0, 9000000)
    outputfile1 = self.file1.replace('%rndfile%', seq)
    logfile1 = outputfile1.replace(".sim", ".log")
    step1 = self.xmlStep1.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step1 = step1.replace("%jTime%", currentTime.strftime('%H:%M'))
    step1 = step1.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step1 = step1.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step1 = step1.replace("%jobname%", "00056438_00001025_1_%s" % seq)
    step1 = step1.replace("%jProduction%", str(self.production))
    step1 = step1.replace("%outputfile1%", outputfile1)
    step1 = step1.replace("%logfile1%", logfile1)
    step1 = step1.replace("%jStepid%", "130404")
    steps.append(step1)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile2 = self.file2.replace('%rndfile%', seq)
    logfile2 = outputfile2.replace(".digi", ".log")
    step2 = self.xmlStep2.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step2 = step2.replace("%jTime%", currentTime.strftime('%H:%M'))
    step2 = step2.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step2 = step2.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step2 = step2.replace("%jobname%", "00056438_00001025_2_%s" % seq)
    step2 = step2.replace("%jProduction%", str(self.production))
    step2 = step2.replace("%inputfile1%", outputfile1)
    step2 = step2.replace("%outputfile2%", outputfile2)
    step2 = step2.replace("%logfile2%", logfile2)
    step2 = step2.replace("%jStepid%", "130405")
    steps.append(step2)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile3 = self.file3.replace('%rndfile%', seq)
    logfile3 = outputfile3.replace(".digi", ".log")
    step3 = self.xmlStep3.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step3 = step3.replace("%jTime%", currentTime.strftime('%H:%M'))
    step3 = step3.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step3 = step3.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step3 = step3.replace("%jobname%", "00056438_00001025_3_%s" % seq)
    step3 = step3.replace("%jProduction%", str(self.production))
    step3 = step3.replace("%inputfile2%", outputfile2)
    step3 = step3.replace("%outputfile3%", outputfile3)
    step3 = step3.replace("%logfile3%", logfile3)
    step3 = step3.replace("%jStepid%", "130406")
    steps.append(step3)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile4 = self.file4.replace('%rndfile%', seq)
    logfile4 = outputfile4.replace(".digi", ".log")
    step4 = self.xmlStep4.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step4 = step4.replace("%jTime%", currentTime.strftime('%H:%M'))
    step4 = step4.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step4 = step4.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step4 = step4.replace("%jobname%", "00056438_00001025_4_%s" % seq)
    step4 = step4.replace("%jProduction%", str(self.production))
    step4 = step4.replace("%inputfile3%", outputfile3)
    step4 = step4.replace("%outputfile4%", outputfile4)
    step4 = step4.replace("%logfile4%", logfile4)
    step4 = step4.replace("%jStepid%", "130407")
    steps.append(step4)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile5 = self.file5.replace('%rndfile%', seq)
    logfile5 = outputfile5.replace(".digi", ".log")
    step5 = self.xmlStep5.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step5 = step5.replace("%jTime%", currentTime.strftime('%H:%M'))
    step5 = step5.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step5 = step5.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step5 = step5.replace("%inputfile4%", outputfile4)
    step5 = step5.replace("%outputfile5%", outputfile5)
    step5 = step5.replace("%logfile5%", logfile5)
    step5 = step5.replace("%jobname%", "00056438_00001025_5_%s" % seq)
    step5 = step5.replace("%jProduction%", str(self.production))
    step5 = step5.replace("%jStepid%", "130408")
    steps.append(step5)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile6 = self.file6.replace('%rndfile%', seq)
    logfile6 = outputfile6.replace(".digi", ".log")
    step6 = self.xmlStep6.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step6 = step6.replace("%jTime%", currentTime.strftime('%H:%M'))
    step6 = step6.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step6 = step6.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step6 = step6.replace("%jobname%", "00056438_00001025_6_%s" % seq)
    step6 = step6.replace("%jProduction%", str(self.production))
    step6 = step6.replace("%inputfile5%", outputfile5)
    step6 = step6.replace("%outputfile6%", outputfile6)
    step6 = step6.replace("%logfile6%", logfile6)
    step6 = step6.replace("%jStepid%", "130409")
    steps.append(step6)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile7 = self.file7.replace('%rndfile%', seq)
    logfile7 = outputfile7.replace(".digi", ".log")
    step7 = self.xmlStep7.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step7 = step7.replace("%jTime%", currentTime.strftime('%H:%M'))
    step7 = step7.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step7 = step7.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step7 = step7.replace("%jobname%", "00056438_00001025_7_%s" % seq)
    step7 = step7.replace("%jProduction%", str(self.production))
    step7 = step7.replace("%inputfile6%", outputfile6)
    step7 = step7.replace("%outputfile7%", outputfile7)
    step7 = step7.replace("%logfile7%", logfile7)
    step7 = step7.replace("%jStepid%", "130410")
    steps.append(step7)

    seq = "%d" % random.randint(0, 9000000)
    currentTime = datetime.datetime.now()
    outputfile8 = self.file8.replace('%rndfile%', seq)
    logfile8 = outputfile8.replace(".digi", ".log")
    step8 = self.xmlStep8.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
    step8 = step8.replace("%jTime%", currentTime.strftime('%H:%M'))
    step8 = step8.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
    step8 = step8.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
    step8 = step8.replace("%jobname%", "00056438_00001025_8_%s" % seq)
    step8 = step8.replace("%jProduction%", str(self.production))
    step8 = step8.replace("%inputfile7%", outputfile7)
    step8 = step8.replace("%outputfile8%", outputfile8)
    step8 = step8.replace("%logfile8%", logfile8)
    step8 = step8.replace("%jStepid%", "130411")
    steps.append(step8)

    return steps

  def run(self):
    steps = self.__prepareXML()
    ostart_time = time.time()
    i = 0
    self.custom_timers['Bkk_ERROR'] = 0
    for step in steps:
      i += 1
      start_time = time.time()
      retVal = cl.sendXMLBookkeepingReport(step)
      if not retVal['OK']:
        self.custom_timers['Bkk_ERROR'] = self.custom_timers['Bkk_ERROR'] + 1
        print retVal['Message']
        return retVal
      end_time = time.time()
      self.custom_timers['Bkk_Step%d' % i] = end_time - start_time

    oend_time = time.time()

    self.custom_timers['Bkk_ResponseTime'] = oend_time - ostart_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
