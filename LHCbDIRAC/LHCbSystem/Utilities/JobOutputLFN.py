########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/Attic/JobOutputLFN.py,v 1.2 2008/08/31 17:43:44 roma Exp $
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


def getJobOutputLFN(job):
  '''
For JobID return list of output LFN'''

  try:
    job = int(job)
  except:
    return {'OK':False, 'Message':'Input parameter is not integer'}
  
  dirac=Dirac()

  result = dirac.getInputSandbox(int(job))
  if not result['OK']:
    return result
  
  try:
    xml = open('InputSandbox%s/jobDescription.xml' %job).read()
  except Exception:
    return {'OK':False, 'Message': 'Can not read XML file: %s'}
  shutil.rmtree( 'InputSandbox%s'%job )      

  j = Job(xml)
  jobid =  None
  prodid = None
  jobname = None
  output = None
  inputdata = None
  configversion = None
  
  for p in j.workflow.parameters:
    if p.getName() == "JOB_ID":
      jobid = p.getValue()
    if p.getName() == "PRODUCTION_ID":
      prodid = p.getValue()
    if p.getName() == "JobName":
      jobname = p.getValue()
    if p.getName() == "outputDataFileMask":
      output = p.getValue()
    if p.getName() == "InputData":
      inputdata = p.getValue()
    if p.getName() == "configVersion":
      configversion = p.getValue()

  if not jobid or not prodid or not jobname or not configversion:
    message = 'Wrong job parameters: %s'%str({'JOB_ID':jobid, 'PRODUCTION_ID':prodid, 'JobName':jobname,'configVersion':configversion})
    return {'OK':False, 'Message':message}
         
  code = j.createCode()
  listoutput = []
  for line in code.split("\n"):
    if line.count("listoutput"):
      listoutput += eval(line.split("#")[0].split("=")[-1]) 

  lfns = []
  for item in listoutput:
    if (not output) or item['outputDataType'] in output:
      filename = item['outputDataName']
      filetype = item['outputDataType']
      lfn = makeProductionLFN(jobid,prodid,configversion,filename,filetype)
      lfns.append(lfn)
        
  return {'OK':True, 'Value':lfns}
