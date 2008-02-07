########################################################################
# $Id: BookkeepingReport.py,v 1.2 2008/02/07 08:41:56 joel Exp $
########################################################################
""" Book Keeping Report Class """

__RCSID__ = "$Id: BookkeepingReport.py,v 1.2 2008/02/07 08:41:56 joel Exp $"

from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC import                                        *

import os, time

class BookkeepingReport(object):

  def __init__(self):
    self.STEP_ID = None
    self.CONFIG_NAME = None
    self.RUN_NUMBER = None
    self.FIRST_EVENT_NUMBER = None
    self.NUMBER_OF_EVENTS = None
    self.NUMBER_OF_EVENTS_OUTPUT = None
    self.nb_events_input = None
    pass

  def execute(self):

    bfilename = 'bookkeeping_'+self.STEP_ID+'.xml'
    bfile = open(bfilename,'w')
    print self.nb_events_input
    print >> bfile,self.makeBookkeepingXMLString()
    bfile.close()

    return S_OK()

  def __parameter_string(self,name,value,ptype):

    return '  <TypedParameter Name="' + str(name) + \
                     '" Value="'+str(value)+'" Type="'+str(ptype)+'"/>\n'

  def makeBookkeepingXMLString(self):

    site = gConfig.getValue('/LocalSite/Site','Site')
    job_mode = gConfig.getValue('/LocalSite/Site','Setup')
    ldate = time.strftime("%Y-%m-%d",time.localtime(time.time()))
    ltime = time.strftime("%H:%M",time.localtime(time.time()))

    s = ''
    s = s+'<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    s = s+'<!DOCTYPE Job SYSTEM "book.dtd">\n'

    # Get the Config name from the environment if any
    if self.CONFIG_NAME != None:
      configName = self.CONFIG_NAME
      configVersion = self.CONFIG_VERSION
    else:
      configName = self.appName
      configVersion = self.appVersion

    s = s+'<Job ConfigName="'+configName+ \
          '" ConfigVersion="'+configVersion+ \
          '" Date="'+ldate+ \
          '" Time="'+ltime+'">\n'

    s = s+self.__parameter_string("Production",self.PRODUCTION_ID,'Info')
    s = s+self.__parameter_string("Job",self.JOB_ID,'Info')
    s = s+self.__parameter_string("Name",self.STEP_ID,'Info')
    s = s+self.__parameter_string("Location",site,'Info')


    # Store all parameters of JOB_PARAM type
##    params = self.module.getAllParameters()
##    for name,par in params.items():
##      if par.type == 'JOB_PARAM':
##        s = s+self.__parameter_string(name,par.value,'Info')


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

    s = s+self.__parameter_string("ProgramName",self.appName,'Info')
    s = s+self.__parameter_string("ProgramVersion",self.appVersion,'Info')


    # Versions of all the explicitly specified packages
##    packages = self.module.step.getPackages()
##    for pname,pversion in packages.items():
##      s = s+self.__parameter_string(pname+'_Version',pversion,'Info')

    # DIRAC version
    s = s+self.__parameter_string('DIRAC_Version',str(majorVersion)+' '+str(minorVersion)+' '+str(patchLevel),'Info')


    # Data tags defined in the production run
##    for pname,par in stepparameters.items():
##      if par.type.lower() == "jobtag":
##        s = s+self.__parameter_string(pname,par.value,"Info")

    # Run number, first event number if any, stats
    if self.RUN_NUMBER != None:
      s = s+self.__parameter_string('RunNumber',self.RUN_NUMBER,"Info")

    if self.FIRST_EVENT_NUMBER != None:
      s = s+self.__parameter_string('FirstEventNumber',self.FIRST_EVENT_NUMBER,"Info")
    else:
      s = s+self.__parameter_string('FirstEventNumber',"1","Info")

    if int(self.NUMBER_OF_EVENTS) > 0:
      s = s+self.__parameter_string('NumberOfEvents',self.NUMBER_OF_EVENTS,"Info")
      if self.NUMBER_OF_EVENTS != None:
        s = s+self.__parameter_string('StatisticsRequested',self.NUMBER_OF_EVENTS,"Info")

    elif self.NUMBER_OF_EVENTS != None:
      if self.NUMBER_OF_EVENTS_OUTPUT != None:
        s = s+self.__parameter_string('NumberOfEvents',self.NUMBER_OF_EVENTS_OUTPUT,"Info")
      else:
        s = s+self.__parameter_string('NumberOfEvents',self.NUMBER_OF_EVENTS,"Info")
      if self.nb_events_input != None:
        s = s+self.__parameter_string('StatisticsRequested',self.nb_events_input,"Info")
      else:
        s = s+self.__parameter_string('StatisticsRequested',self.NUMBER_OF_EVENTS,"Info")

    return s

    # Extra job parameters
    JobId = os.environ['JOBID']
    s = s+self.__parameter_string('DIRAC_JobID',JobId,"Info")
    parfilename = str(JobId)+'_parameters.txt'
    if os.path.exists(parfilename):
      parfile = file(str(JobId)+'_parameters.txt')
      lines = parfile.readlines()
      for l in lines:
        par_name,par_value = l.strip().split('=')
        if par_name.find('Step ') == -1:
          s = s+self.__parameter_string(par_name,par_value,"Info")

    # CPU time consumed in the step
    endstats = os.times()
    parentcputime = endstats[0] - self.module.step.statistics[0]
    childcputime = endstats[2] - self.module.step.statistics[2]
    cputime = parentcputime + childcputime
    s = s+self.__parameter_string("CPUTime",str(cputime),"Info")

    # Wall clock time for the step
    end_time = time.time()
    exectime = end_time - self.module.step.start_time
    s = s+self.__parameter_string("ExecTime",str(exectime),"Info")

    # Input files
    for inputname,typeName,typeVersion in self.module.step.inputFiles():
      lfn = makeProductionLfn((inputname,typeName,typeVersion),job_mode,jobparameters, \
                     self.module.step.job.prod_id)
      s = s+'  <InputFile    Name="'+lfn+'"/>\n'


    ####################################################################
    # Output files
    # Define DATA TYPES - ugly! should find another way to do that

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST']

    if stepparameters.has_key('EVENTTYPE'):
      eventtype = stepparameters['EVENTTYPE'].value
    elif allparameters.has_key('EVENTTYPE'):
      eventtype = allparameters['EVENTTYPE'].value
    else:
      gLog.warn( 'BookkeepingReport: no EVENTTYPE specified' )
      eventtype = 'Unknown'

    if stepparameters.has_key('NUMBER_OF_EVENTS_OUTPUT'):
      statistics = stepparameters['NUMBER_OF_EVENTS_OUTPUT'].value
    elif allparameters.has_key('NUMBER_OF_EVENTS'):
      statistics = allparameters['NUMBER_OF_EVENTS'].value
    else:
      gLog.warn( 'BookkeepingReport: no NUMBER_OF_EVENTS specified' )
      statistics = "0"

    for output,typeName,typeVersion,se in self.module.step.outputFiles():

      # Output file size
      try:
        outputsize = str(os.path.getsize(output))
      except:
        outputsize = '0'

      comm = 'md5sum '+output
      status,out = commands.getstatusoutput(comm)
      if status:
        gLog.info( "Failed to get md5sum of %s" % str( output ) )
        gLog.info( str( out ) )
        md5sum = '000000000000000000000000000000000000'
      else:
        md5sum = out.split()[0]

      guid = self.getGuidFromPoolXMLCatalog(output)
      if guid == '':
        if md5sum != '000000000000000000000000000000000000':
          guid = makeGuid(md5sum = md5sum)
        else:
          guid = makeGuid()

      # build the lfn
      lfn = makeProductionLfn((output,typeName,typeVersion),job_mode,jobparameters, \
                               self.module.step.job.prod_id)

      s = s+'  <OutputFile   Name="'+lfn+'" TypeName="'+typeName+'" TypeVersion="'+typeVersion+'">\n'
      if typeName in dataTypes:
        s = s+'    <Parameter  Name="EventType"     Value="'+eventtype+'"/>\n'
        s = s+'    <Parameter  Name="EventStat"       Value="'+statistics+'"/>\n'
        s = s+'    <Parameter  Name="Size"        Value="'+outputsize+'"/>\n'
        s = s+'    <Quality Group="Production Manager" Flag="Not Checked"/>\n'

      ############################################################
      # Log file replica information
      if typeName == "LOG":
        if stepparameters.has_key('LOGFILE[0]'):
          logfile = stepparameters['LOGFILE[0]'].value
          if logfile == output:
            logname = 'file:'+os.environ['LHCBPRODROOT']+'/job/log/'+ \
                      self.module.step.job.prod_id+'/'+ \
                      self.module.step.job.job_id+'/'+ logfile + '.gz'
            s = s+'    <Replica Name="'+logname+'" Location="'+ site+'"/>\n'

            # Get the url for log files
            logpath = makeProductionPath(job_mode,jobparameters,self.module.step.job.prod_id,log=True)
            logse = StorageElement('LogSE')
            ses = logse.getStorage(['http'])
            if ses:
              # HTTP protocol is defined for LogSE
              logurl = ses[0].getPFNBase()+logpath
            else:
              # HTTP protocol is not defined for LogSE
              try:
                logurl = cfgSvc.get(job_mode,'LogPathHTTP')+logpath
              except:
                # Default url
                logurl = 'http://lxb2003.cern.ch/storage'+logpath

            url = logurl+'/'+ \
                  self.module.step.job.job_id+'/'+ logfile + '.gz'
            s = s+'    <Replica Name="'+url+'" Location="Web"/>\n'

      s = s+'    <Parameter  Name="MD5SUM"        Value="'+md5sum+'"/>\n'
      s = s+'    <Parameter  Name="GUID"        Value="'+guid+'"/>\n'
      s = s+'  </OutputFile>\n'

    s = s+'</Job>'
    return s

  def getGuidFromPoolXMLCatalog(self,output):

    self.prod_id = self.module.step.job.prod_id
    self.job_id = self.module.step.job.job_id

    ####################################
    # Get the Pool XML catalog if any
    poolcat = None
    fcname = []
    if os.environ.has_key('PoolXMLCatalog'):
      fcn = os.environ['PoolXMLCatalog']
      if os.path.isfile(fcn+'.gz'):
        gunzip(fcn+'.gz')
      fcname.append(fcn)
    else:
      if self.module.step.job.parameters.has_key('PoolXMLCatalog'):
        fcn = self.module.step.job.parameters['PoolXMLCatalog'].value
        if os.path.isfile(fcn+'.gz'):
          gunzip(fcn+'.gz')
        fcname.append(fcn)

    flist = os.listdir('.')
    for fcn in flist:
      # Account for file names like xxx_catalog.xml, NewCatalog.xml
      if re.search('atalog.xml',fcn) and not re.search('BAK',fcn) and not re.search('.temp$',fcn):
        if re.search('.gz',fcn):
          gunzip(fcn)
          fcn = fcn.replace('.gz','')
        fcname.append(fcn)

    print "BookkeepingReport: Pool XML catalog files:"
    for f in fcname:
      print f

    if fcname:
      poolcat = PoolXMLCatalog(fcname)

    print "BookkeepingReport: Pool XML catalog constructed"

    try:
      guid = poolcat.getGuidByPfn(output)
      return guid
    except Exception,x :
      gLog.error( "Failed to get GUID from PoolXMLCatalog ! %s" % str( x ) )
      return ''

