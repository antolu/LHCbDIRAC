########################################################################
# $Id: NewBookkeepingReport.py,v 1.3 2008/07/02 12:47:34 joel Exp $
########################################################################
""" Bookkeeping Report Class """

__RCSID__ = "$Id: NewBookkeepingReport.py,v 1.3 2008/07/02 12:47:34 joel Exp $"

from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from WorkflowLib.Utilities.Tools import *
from WorkflowLib.Module.ModuleBase import ModuleBase
from DIRAC import  *

import os, time, re

class BookkeepingReport(ModuleBase):

  def __init__(self):
    self.version = __RCSID__
    self.configName = ''
    self.configVersion = ''
    self.run_number = 0
    self.firstEventNumber = 1
    self.numberOfEvents = 0
    self.numberOfEventsInput = 0
    self.numberOfEventsOutput = 0
    self.eventType = ''
    self.poolXMLCatName = ''
    self.inputData = ''
    self.JOB_ID = None # to check
    self.sourceData=''
    self.applicationName = ''
    self.applicationLog = ''
    self.log = gLogger.getSubLogger("BookkeepingReport")
    pass

  def resolveInputVariables(self):
    if self.workflow_commons.has_key('sourceData'):
        self.sourceData = self.workflow_commons['sourceData']

    if self.step_commons.has_key('eventType'):
        self.eventType = self.step_commons['eventType']

    if self.step_commons.has_key('numberOfEvents'):
       self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key('numberOfEventsOutput'):
       self.numberOfEventsOutput = self.step_commons['numberOfEventsOutput']

    if self.step_commons.has_key('numberOfEventsInput'):
       self.numberOfEventsInput = self.step_commons['numberOfEventsInput']

    if self.step_commons.has_key('inputDataType'):
       self.inputDataType = self.step_commons['inputDataType']

    if self.workflow_commons.has_key('poolXMLCatName'):
       self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    if self.step_commons.has_key('inputData'):
       self.inputData = self.step_commons['inputData']

    if self.step_commons.has_key('listoutput'):
       self.listoutput = self.step_commons['listoutput']

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']


  def execute(self):
    self.log.info('Initializing '+self.version)
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
       self.log.info('Skip this module, failure detected in a previous step :')
       self.log.info('Workflow status : %s' %(self.workflowStatus))
       self.log.info('Step Status %s' %(self.stepStatus))
       return S_OK()

    self.resolveInputVariables()
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    bfilename = 'bookkeeping_'+self.STEP_ID+'.xml'
    bfile = open(bfilename,'w')
    print >> bfile,self.makeBookkeepingXMLString()
    bfile.close()

    bfilename = 'newbookkeeping_'+self.STEP_ID+'.xml'
    bfile = open(bfilename,'w')
    print >> bfile,self.NewmakeBookkeepingXMLString()
    bfile.close()

    if self.workflow_commons.has_key('outputList'):
        self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
        self.workflow_commons['outputList'] = self.listoutput

    return S_OK()

  def __parameter_string(self,name,value,ptype):

    return '  <TypedParameter Name="' + str(name) + \
                     '" Value="'+str(value)+'" Type="'+str(ptype)+'"/>\n'

  def makeBookkeepingXMLString(self):

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF']
    site = gConfig.getValue('/LocalSite/Site','Site')
    if self.workflow_commons.has_key('dataType'):
      job_mode = self.workflow_commons['dataType'].lower()
    else:
      job_mode = 'test'
    ldate = time.strftime("%Y-%m-%d",time.localtime(time.time()))
    ltime = time.strftime("%H:%M",time.localtime(time.time()))

    s = ''
    s = s+'<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    s = s+'<!DOCTYPE Job SYSTEM "book.dtd">\n'

    # Get the Config name from the environment if any
    if self.workflow_commons.has_key('configName'):
      configName = self.workflow_commons['configName']
      configVersion = self.workflow_commons['configVersion']
    else:
      configName = self.applicationName
      configVersion = self.applicationVersion

    s = s+'<Job ConfigName="'+configName+ \
          '" ConfigVersion="'+configVersion+ \
          '" Date="'+ldate+ \
          '" Time="'+ltime+'">\n'

    s = s+self.__parameter_string("Production",self.PRODUCTION_ID,'Info')
    s = s+self.__parameter_string("Job",self.JOB_ID,'Info')
    s = s+self.__parameter_string("Name",self.STEP_ID,'Info')
    s = s+self.__parameter_string("Location",site,'Info')

    host = None
    if os.environ.has_key("HOSTNAME"):
      host = os.environ["HOSTNAME"]
    elif os.environ.has_key("HOST"):
      host = os.environ["HOST"]
    if host is not None:
      s = s+self.__parameter_string('Host',host,'Info')

    if  os.environ.has_key('DBASE_VERSION'):
      s = s+self.__parameter_string("DbaseVersion",os.environ["DBASE_VERSION"],'Info')
    if  os.environ.has_key('XMLDDDB_VERSION'):
      s = s+self.__parameter_string("XmlDDDBVersion",os.environ["XMLDDDB_VERSION"],'Info')

    s = s+self.__parameter_string("ProgramName",self.applicationName,'Info')
    s = s+self.__parameter_string("ProgramVersion",self.applicationVersion,'Info')

    # DIRAC version
    s = s+self.__parameter_string('DIRAC_Version',str(majorVersion)+' '+str(minorVersion)+' '+str(patchLevel),'Info')

    # Run number, first event number if any, stats
    if self.run_number != None:
      s = s+self.__parameter_string('RunNumber',self.run_number,"Info")

    if self.firstEventNumber != None:
      s = s+self.__parameter_string('FirstEventNumber',self.firstEventNumber,"Info")
    else:
      s = s+self.__parameter_string('FirstEventNumber',"1","Info")

    if self.numberOfEvents != None:
      s = s+self.__parameter_string('StatisticsRequested',self.numberOfEvents,"Info")

    if self.numberOfEventsInput != None:
      s = s+self.__parameter_string('NumberOfEvents',self.numberOfEventsInput,"Info")
    else:
      s = s+self.__parameter_string('NumberOfEvents',self.numberOfEvents,"Info")

    if self.sourceData:
      self.LFN_ROOT= getLFNRoot(self.sourceData)
    else:
      self.LFN_ROOT=getLFNRoot(self.sourceData,configVersion)

    if self.inputData:
      for inputname in self.inputData.split(';'):
        lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(inputname,self.inputDataType,''),job_mode,self.PRODUCTION_ID)
        s = s+'  <InputFile    Name="'+lfn+'"/>\n'


    ####################################################################
    # Output files
    # Define DATA TYPES - ugly! should find another way to do that


    if self.eventType != None:
      eventtype = self.eventType
    else:
      self.log.warn( 'BookkeepingReport: no eventType specified' )
      eventtype = 'Unknown'
    self.log.info( 'Event type = %s' % (str(self.eventType)))
    self.log.info( 'stats = %s' %(self.numberOfEventsOutput))

    if self.numberOfEventsOutput != None:
      statistics = self.numberOfEventsOutput
    elif self.numberOfEvents != None:
      statistics = self.numberOfEvents
    else:
      self.log.warn( 'BookkeepingReport: no numberOfEvents specified' )
      statistics = "0"


    outputs = []
    count = 0
    while (count < len(self.listoutput)):
      if self.listoutput[count].has_key('outputDataName'):
        outputs.append(((self.listoutput[count]['outputDataName']),(self.listoutput[count]['outputDataSE']),(self.listoutput[count]['outputDataType'])))
      count=count+1
    outputs_done = []
    outputs.append(((self.applicationLog),('LogSE'),('LOG')))
    self.log.info(outputs)
    for output,outputse,outputtype in outputs:
      typeName = outputtype.upper()
      typeVersion = '1'

      # Output file size
      try:
        outputsize = str(os.path.getsize(output))
      except:
        outputsize = '0'

      comm = 'md5sum '+str(output)
      resultTuple = shellCall(0,comm)
      status = resultTuple['Value'][0]
      out = resultTuple['Value'][1]

      if status:
        self.log.info( "Failed to get md5sum of %s" % str( output ) )
        self.log.info( str( out ) )
        md5sum = '000000000000000000000000000000000000'
      else:
        md5sum = out.split()[0]

      guid = getGuidFromPoolXMLCatalog(self.poolXMLCatName,output)
      if guid == '':
        if md5sum != '000000000000000000000000000000000000':
          guid = makeGuid(output)
        else:
          guid = makeGuid()

      # build the lfn
      lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,typeName,typeVersion),job_mode, self.PRODUCTION_ID)

      s = s+'  <OutputFile   Name="'+lfn+'" TypeName="'+typeName+'" TypeVersion="'+typeVersion+'">\n'
      if typeName in dataTypes:
        s = s+'    <Parameter  Name="EventType"     Value="'+eventtype+'"/>\n'
        s = s+'    <Parameter  Name="EventStat"       Value="'+statistics+'"/>\n'
        s = s+'    <Parameter  Name="Size"        Value="'+outputsize+'"/>\n'
        s = s+'    <Quality Group="Production Manager" Flag="Not Checked"/>\n'


      ############################################################
      # Log file replica information
#      if typeName == "LOG":
      if self.applicationLog != None:
          logfile = self.applicationLog
          if logfile == output:
#            logpath = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,typeName,typeVersion),job_mode,self.PRODUCTION_ID)
            logpath = makeProductionPath(self.JOB_ID,self.LFN_ROOT,typeName,job_mode,self.PRODUCTION_ID,log=True)
            logurl = 'http://lhcb-logs.cern.ch/storage'

            url = logurl+logpath+'/'+self.JOB_ID+'/'
            s = s+'    <Replica Name="'+url+'" Location="Web"/>\n'

      s = s+'    <Parameter  Name="MD5SUM"        Value="'+md5sum+'"/>\n'
      s = s+'    <Parameter  Name="GUID"        Value="'+guid+'"/>\n'
      s = s+'  </OutputFile>\n'

    s = s+'</Job>'
    return s

  def NewmakeBookkeepingXMLString(self):

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF']
    site = gConfig.getValue('/LocalSite/Site','Site')
    if self.workflow_commons.has_key('dataType'):
      job_mode = self.workflow_commons['dataType'].lower()
    else:
      job_mode = 'test'
    ldate = time.strftime("%Y-%m-%d",time.localtime(time.time()))
    ltime = time.strftime("%H:%M",time.localtime(time.time()))

    s = ''
    s = s+'<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    s = s+'<!DOCTYPE Job SYSTEM "book.dtd">\n'

    # Get the Config name from the environment if any
    if self.workflow_commons.has_key('configName'):
      configName = self.workflow_commons['configName']
      configVersion = self.workflow_commons['configVersion']
    else:
      configName = self.applicationName
      configVersion = self.applicationVersion

    s = s+'<Job ConfigName="'+configName+ \
          '" ConfigVersion="'+configVersion+ \
          '" Date="'+ldate+ \
          '" Time="'+ltime+'">\n'

    s = s+self.__parameter_string("Production",self.PRODUCTION_ID,'Info')
    s = s+self.__parameter_string("DiracJobId",self.JOB_ID,'Info')
    s = s+self.__parameter_string("Name",self.STEP_ID,'Info')
    s = s+self.__parameter_string("Location",site,'Info')

    host = None
    if os.environ.has_key("HOSTNAME"):
      host = os.environ["HOSTNAME"]
    elif os.environ.has_key("HOST"):
      host = os.environ["HOST"]
    if host is not None:
      s = s+self.__parameter_string('Host',host,'Info')

    if  os.environ.has_key('XMLDDDB_VERSION'):
      s = s+self.__parameter_string("GeometryVersion",os.environ["XMLDDDB_VERSION"],'Info')

    s = s+self.__parameter_string("ProgramName",self.applicationName,'Info')
    s = s+self.__parameter_string("ProgramVersion",self.applicationVersion,'Info')

    # DIRAC version
    s = s+self.__parameter_string('DIRACVersion',str(majorVersion)+' '+str(minorVersion)+' '+str(patchLevel),'Info')

    if self.firstEventNumber != None:
      s = s+self.__parameter_string('FirstEventNumber',self.firstEventNumber,"Info")
    else:
      s = s+self.__parameter_string('FirstEventNumber',"1","Info")

    if self.numberOfEvents != None:
      s = s+self.__parameter_string('StatisticsRequested',self.numberOfEvents,"Info")

    if self.numberOfEventsInput != None:
      s = s+self.__parameter_string('NumberOfEvents',self.numberOfEventsInput,"Info")
    else:
      s = s+self.__parameter_string('NumberOfEvents',self.numberOfEvents,"Info")

    if self.sourceData:
      self.LFN_ROOT= getLFNRoot(self.sourceData)
    else:
      self.LFN_ROOT=getLFNRoot(self.sourceData,configVersion)

    if self.inputData:
      for inputname in self.inputData.split(';'):
        lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(inputname,self.inputDataType,''),job_mode,self.PRODUCTION_ID)
        s = s+'  <InputFile    Name="'+lfn+'"/>\n'


    ####################################################################
    # Output files
    # Define DATA TYPES - ugly! should find another way to do that


    if self.eventType != None:
      eventtype = self.eventType
    else:
      self.log.warn( 'BookkeepingReport: no eventType specified' )
      eventtype = 'Unknown'
    self.log.info( 'Event type = %s' % (str(self.eventType)))
    self.log.info( 'stats = %s' %(self.numberOfEventsOutput))

    if self.numberOfEventsOutput != None:
      statistics = self.numberOfEventsOutput
    elif self.numberOfEvents != None:
      statistics = self.numberOfEvents
    else:
      self.log.warn( 'BookkeepingReport: no numberOfEvents specified' )
      statistics = "0"


    outputs = []
    count = 0
    while (count < len(self.listoutput)):
      if self.listoutput[count].has_key('outputDataName'):
        outputs.append(((self.listoutput[count]['outputDataName']),(self.listoutput[count]['outputDataSE']),(self.listoutput[count]['outputDataType'])))
      count=count+1
    outputs_done = []
    outputs.append(((self.applicationLog),('LogSE'),('LOG')))
    self.log.info(outputs)
    for output,outputse,outputtype in outputs:
      typeName = outputtype.upper()
      typeVersion = '1'

      # Output file size
      try:
        outputsize = str(os.path.getsize(output))
      except:
        outputsize = '0'

      comm = 'md5sum '+str(output)
      resultTuple = shellCall(0,comm)
      status = resultTuple['Value'][0]
      out = resultTuple['Value'][1]

      if status:
        self.log.info( "Failed to get md5sum of %s" % str( output ) )
        self.log.info( str( out ) )
        md5sum = '000000000000000000000000000000000000'
      else:
        md5sum = out.split()[0]

      guid = getGuidFromPoolXMLCatalog(self.poolXMLCatName,output)
      if guid == '':
        if md5sum != '000000000000000000000000000000000000':
          guid = makeGuid(output)
        else:
          guid = makeGuid()

      # build the lfn
      lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,typeName,typeVersion),job_mode, self.PRODUCTION_ID)

      s = s+'  <OutputFile   Name="'+lfn+'" TypeName="'+typeName+'" TypeVersion="'+typeVersion+'">\n'
      if typeName in dataTypes:
        s = s+'    <Parameter  Name="EventTypeId"     Value="'+eventtype+'"/>\n'
        s = s+'    <Parameter  Name="EventStat"       Value="'+statistics+'"/>\n'
        s = s+'    <Parameter  Name="Size"        Value="'+outputsize+'"/>\n'


      ############################################################
      # Log file replica information
#      if typeName == "LOG":
      if self.applicationLog != None:
          logfile = self.applicationLog
          if logfile == output:
#            logpath = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,typeName,typeVersion),job_mode,self.PRODUCTION_ID)
            logpath = makeProductionPath(self.JOB_ID,self.LFN_ROOT,typeName,job_mode,self.PRODUCTION_ID,log=True)
            logurl = 'http://lhcb-logs.cern.ch/storage'

            url = logurl+logpath+'/'+self.JOB_ID+'/'
            s = s+'    <Replica Name="'+url+'" Location="Web"/>\n'

      s = s+'    <Parameter  Name="MD5SUM"        Value="'+md5sum+'"/>\n'
      s = s+'    <Parameter  Name="GUID"        Value="'+guid+'"/>\n'
      s = s+'  </OutputFile>\n'
    if self.applicationName == "Gauss":
        makeBeamConditions()

    s = s+'</Job>'
    return s

  def makeBeamConditions(self):
      s = s+'<SimulationCondition>\n'
      s = s+'    <Parameter Name="SimDescription"   Value="DC06 simulation 2 10**32"/>\n'
      s = s+'    <Parameter Name="BeamCond"         Value="Collisions"/>\n'
      s = s+'    <Parameter Name="BeamEnergy"       Value="7 TeV"/>\n'
      s = s+'    <Parameter Name="Generator"        Value="Pythia 6.325.2"/>\n'
      s = s+'    <Parameter Name="MagneticField"    Value="-100%"/>\n'
      s = s+'    <Parameter Name="DetectorCond"     Value="Normal"/>\n'
      s = s+'    <Parameter Name="Luminosity"       Value="Fixed 2 10**32"/>\n'
      s = s+'</SimulationCondition>\n'


