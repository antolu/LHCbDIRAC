########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/JobInfoFromXML.py,v 1.1 2008/09/05 11:30:10 roma Exp $
# File :   JobOutputLFN.py
# Author : Vladimir Romanovsky
########################################################################

from DIRAC.Interfaces.API.Dirac                              import Dirac
from DIRAC.Interfaces.API.Job                                import Job

import shutil,os
    
def makeProductionLFN(jobid,prodid,config,fname,ftype):
  """ Constructs the logical file name according to LHCb conventions.
  Returns the lfn without 'lfn:' prepended
  """

  if fname.count('lfn:'):
    return fname.replace('lfn:','')
  
  if fname.count('LFN:'):
    return fname.replace('LFN:','')

  if config.count('DC06'):
    lfnroot = '/lhcb/MC/DC06'
  else:
    lfnroot = '/lhcb/data/'
    
  try:
    jobindex = "%04d"%(int(jobid)/10000)
  except:
    jobindex = '0000'

  return os.path.join(lfnroot,str(ftype).upper(),prodid,jobindex,fname)


class JobInfoFromXML:
  '''
  '''

  def __init__(self,jobid):
  
    self.message = None
    try:
      job = int(jobid)
    except:
      self.message='Input parameter is not integer'
      return
  
    dirac=Dirac()

    result = dirac.getInputSandbox(job)
    if not result['OK']:
      self.message=resul['Message']
      return

    try:
      xml = open('InputSandbox%s/jobDescription.xml' %job).read()
    except Exception,x:
      self.message= 'Can not read XML file: %s'%x
      return
    shutil.rmtree( 'InputSandbox%s'%job )      
    
    self.j = Job(xml)
    self.jobid =  None
    self.prodid = None
    self.jobname = None
    self.output = None
    self.inputdata = None
    self.configversion = None
  
    for p in self.j.workflow.parameters:
      if p.getName() == "JOB_ID":
        self.jobid = p.getValue()
      if p.getName() == "PRODUCTION_ID":
        self.prodid = p.getValue()
      if p.getName() == "JobName":
        self.jobname = p.getValue()
      if p.getName() == "outputDataFileMask":
        self.output = p.getValue()
      if p.getName() == "InputData":
        self.inputdata = p.getValue()
      if p.getName() == "configVersion":
        self.configversion = p.getValue()

    if not self.jobid or not self.prodid or not self.jobname or not self.configversion:
      self.message = 'Wrong job parameters: %s'%str({'JOB_ID':jobid, 'PRODUCTION_ID':prodid, 'JobName':jobname,'configVersion':configversion})
      return
      
  def valid(self):
    if self.message:
      return {'OK':False,'Message':self.message}  
    return {'OK':True}

  def getInputLFN(self):
  
    if self.message:
      return {'OK':False,'Message':self.message}

    if not self.inputdata:
      return {'OK':True,'Value':[]}
      
    jobid = self.jobid
    prodid = self.prodid
    configversion = self.configversion
    filename = self.inputdata
    filetype = None
    inputlfns= [makeProductionLFN(jobid,prodid,configversion,filename,filetype)]
    return {'OK':True,'Value':inputlfns}
  
  def getOutputLFN(self):
  
    if self.message:
      return {'OK':False,'Message':self.message}

    code = self.j.createCode()
    listoutput = []
    for line in code.split("\n"):
      if line.count("listoutput"):
        listoutput += eval(line.split("#")[0].split("=")[-1]) 

    outputlfns = []
    for item in listoutput:
      if (not self.output) or item['outputDataType'] in self.output:
        jobid = self.jobid
        prodid = self.prodid
        configversion = self.configversion
        filename = item['outputDataName']
        filetype = item['outputDataType']
        lfn = makeProductionLFN(jobid,prodid,configversion,filename,filetype)
        outputlfns.append(lfn)

    return {'OK':True, 'Value':outputlfns}
  
             
