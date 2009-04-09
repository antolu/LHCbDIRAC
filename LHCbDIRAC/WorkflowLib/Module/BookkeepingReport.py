########################################################################
# $Id: BookkeepingReport.py,v 1.35 2009/04/09 13:17:58 paterson Exp $
########################################################################
""" Bookkeeping Report Class """

__RCSID__ = "$Id: BookkeepingReport.py,v 1.35 2009/04/09 13:17:58 paterson Exp $"

from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from WorkflowLib.Utilities.Tools import *
from WorkflowLib.Module.ModuleBase import ModuleBase
from DIRAC import  *

try:
  from LHCbSystem.Utilities.ProductionData  import constructProductionLFNs
except Exception,x:
  from DIRAC.LHCbSystem.Utilities.ProductionData  import constructProductionLFNs

import os, time, re, string

class BookkeepingReport(ModuleBase):

  def __init__(self):
    self.version = __RCSID__
    self.configName = ''
    self.configVersion = ''
    self.run_number = 0
    self.firstEventNumber = 1
    self.numberOfEvents = ''
    self.numberOfEventsInput = ''
    self.numberOfEventsOutput = ''
    self.simDescription = 'NoSimConditions'
    self.eventType = ''
    self.poolXMLCatName = ''
    self.inputData = ''
    self.InputData = ''
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

    if self.workflow_commons.has_key('simDescription'):
        self.simDescription = self.workflow_commons['simDescription']

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

    if self.workflow_commons.has_key('InputData'):
       self.InputData = self.workflow_commons['InputData']

    if self.step_commons.has_key('inputData'):
       self.inputData = self.step_commons['inputData']

    if self.step_commons.has_key('listoutput'):
       self.listoutput = self.step_commons['listoutput']

    if self.workflow_commons.has_key('outputList'):
        self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
        self.workflow_commons['outputList'] = self.listoutput

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']

    if self.workflow_commons.has_key('BookkeepingLFNs') and self.workflow_commons.has_key('LogFilePath') and self.workflow_commons.has_key('ProductionOutputData'):
      self.logFilePath = self.workflow_commons['LogFilePath']
      self.bkLFNs = self.workflow_commons['BookkeepingLFNs']
      if not type(self.bkLFNs)==type([]):
        self.bkLFNs = [i.strip() for i in self.bkLFNs.split(';')]
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']
      if not type(self.prodOutputLFNs)==type([]):
        self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split(';')]
    else:
      self.log.info('LogFilePath / BookkeepingLFNs parameters not found, creating on the fly')
      result = constructProductionLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      self.bkLFNs=result['Value']['BookkeepingLFNs']
      self.logFilePath=result['Value']['LogFilePath'][0]
      self.prodOutputLFNs=result['Value']['ProductionOutputData']

    return S_OK()


  def execute(self):
    self.log.info('Initializing '+self.version)
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
       self.log.info('Skip this module, failure detected in a previous step :')
       self.log.info('Workflow status : %s' %(self.workflowStatus))
       self.log.info('Step Status %s' %(self.stepStatus))
       return S_OK()

    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    bfilename = 'bookkeeping_'+self.STEP_ID+'.xml'
    bfile = open(bfilename,'w')
    print >> bfile,self.makeBookkeepingXMLString()
    bfile.close()

    return S_OK()

  def __parameter_string(self,name,value,ptype):

    return '  <TypedParameter Name="' + str(name) + \
                     '" Value="'+str(value)+'" Type="'+str(ptype)+'"/>\n'


  def makeBookkeepingXMLString(self):

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF','HIST']
    site = gConfig.getValue('/LocalSite/Site','Site')
    if self.workflow_commons.has_key('dataType'):
      job_mode = self.workflow_commons['dataType'].lower()
    else:
      job_mode = 'test'
    ldate = time.strftime("%Y-%m-%d",time.localtime(time.time()))
    ltime = time.strftime("%H:%M",time.localtime(time.time()))
    if self.step_commons.has_key('StartTime'):
      ldatestart = time.strftime("%Y-%m-%d",time.localtime(self.step_commons['StartTime']))
      ltimestart = time.strftime("%H:%M",time.localtime(self.step_commons['StartTime']))

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
    s = s+self.__parameter_string("JobStart",ldatestart+' '+ltimestart,'Info')
    s = s+self.__parameter_string("JobEnd",ldate+' '+ltime,'Info')
    s = s+self.__parameter_string("Location",site,'Info')

    host = None
    if os.environ.has_key("HOSTNAME"):
      host = os.environ["HOSTNAME"]
    elif os.environ.has_key("HOST"):
      host = os.environ["HOST"]
    if host is not None:
      s = s+self.__parameter_string('WorkerNode',host,'Info')

    if  os.environ.has_key('XMLDDDB_VERSION'):
      s = s+self.__parameter_string("GeometryVersion",os.environ["XMLDDDB_VERSION"],'Info')

    s = s+self.__parameter_string("ProgramName",self.applicationName,'Info')
    s = s+self.__parameter_string("ProgramVersion",self.applicationVersion,'Info')

    # DIRAC version
    s = s+self.__parameter_string('DiracVersion','v'+str(majorVersion)+'r'+str(minorVersion)+'p'+str(patchLevel),'Info')

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

    if self.inputData:
      for inputname in self.inputData.split(';'):
        lfn = ''
        for bkLFN in self.bkLFNs:
          if os.path.basename(bkLFN)==os.path.basename(inputname):
            lfn = bkLFN
        if not lfn:
          return S_ERROR('Could not construct LFN for %s' %inputname)
        s = s+'  <InputFile    Name="'+lfn+'"/>\n'


    ####################################################################
    # Output files
    # Define DATA TYPES - ugly! should find another way to do that

    statistics = "0"

    if self.eventType != None:
      eventtype = self.eventType
    else:
      self.log.warn( 'BookkeepingReport: no eventType specified' )
      eventtype = 'Unknown'
    self.log.info( 'Event type = %s' % (str(self.eventType)))
    self.log.info( 'stats = %s' %(str(self.numberOfEventsOutput)))

    if self.numberOfEventsOutput != '':
      statistics = self.numberOfEventsOutput
    elif self.numberOfEventsInput != '':
      statistics = self.numberOfEventsInput
    elif self.numberOfEvents != '':
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
      self.log.info('Looking at output %s %s %s' %(output,outputse,outputtype))
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

      # find the constructed lfn
      lfn = ''
      if not re.search('.log$',output):
        for outputLFN in self.bkLFNs:
          if os.path.basename(outputLFN)==output:
            lfn=outputLFN
        if not lfn:
          return S_ERROR('Could not find LFN for %s' %output)
      else:
        lfn = '%s/%s' %(self.logFilePath,self.applicationLog)

      #Fix for histograms
      oldTypeName=None
      if typeName.upper()=='HIST':
        typeVersion='0'
        oldTypeName=typeName
        typeName='%sHIST' %(self.applicationName.upper())

      s = s+'  <OutputFile   Name="'+lfn+'" TypeName="'+typeName+'" TypeVersion="'+typeVersion+'">\n'

      #HIST is in the dataTypes e.g. we may have new names in the future ;)
      if oldTypeName:
        typeName=oldTypeName

      if typeName in dataTypes:
        s = s+'    <Parameter  Name="EventTypeId"     Value="'+eventtype+'"/>\n'
        s = s+'    <Parameter  Name="EventStat"       Value="'+statistics+'"/>\n'
        s = s+'    <Parameter  Name="FileSize"        Value="'+outputsize+'"/>\n'


      ############################################################
      # Log file replica information
#      if typeName == "LOG":
      if self.applicationLog != None:
          logfile = self.applicationLog
          if logfile == output:
            logurl = 'http://lhcb-logs.cern.ch/storage'
            url = logurl+self.logFilePath+'/'+self.applicationLog
            s = s+'    <Replica Name="'+url+'" Location="Web"/>\n'

      s = s+'    <Parameter  Name="MD5Sum"        Value="'+md5sum+'"/>\n'
      s = s+'    <Parameter  Name="Guid"        Value="'+guid+'"/>\n'
      s = s+'  </OutputFile>\n'
    if self.applicationName == "Gauss":
        s = self.makeBeamConditions(s)

    s = s+'</Job>'
    return s

  def makeBeamConditions(self,sbeam):
      sbeam = sbeam+'  <SimulationCondition>\n'
      sbeam = sbeam+'    <Parameter Name="SimDescription"   Value="'+self.simDescription+'"/>\n'
      sbeam = sbeam+'  </SimulationCondition>\n'
      return sbeam


