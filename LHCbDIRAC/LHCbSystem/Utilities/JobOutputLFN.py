########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/Attic/JobOutputLFN.py,v 1.1 2008/08/20 18:09:26 roma Exp $
# File :   JobOutputLFN.py
# Author : Vladimir Romanovsky
########################################################################

from DIRAC.Interfaces.API.Dirac                              import Dirac
from DIRAC.Interfaces.API.Job                                import Job

import shutil


def getLFNRoot(lfn,mcYear=0):

  
  if lfn:
    return '/lhcb/data/'
          
  elif mcYear=='DC06': #This should be reviewed.
    return '/lhcb/production/DC06'
  else:
    return '/lhcb/MC/'+str(mcYear)
    
def makeProductionLfn(JOB_ID,LFN_ROOT,filetuple,mode,prodstring,prodConfig='phys-v4-lumi2'):
  """ Constructs the logical file name according to LHCb conventions.
  Returns the lfn without 'lfn:' prepended
  """
  try:
    jobindex = "%04d"%(int(JOB_ID)/10000)
  except:
    jobindex = '0000'

  fname = filetuple[0]

  if fname.count('lfn:'):
    return fname.replace('lfn:','')
  
  if fname.count('LFN:'):
    return fname.replace('LFN:','')

  if LFN_ROOT.count('DC06',): #This should be reviewed, is a nasty fix.
    return LFN_ROOT+'/'+prodConfig+'/'+prodstring+'/'+filetuple[1].upper()+'/'+jobindex+'/'+filetuple[0]

  return LFN_ROOT+'/'+filetuple[1].upper()+'/'+prodstring+'/'+jobindex+'/'+filetuple[0]


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
      lfnroot = getLFNRoot(inputdata,configversion)
      lfn = makeProductionLfn(jobid,lfnroot,(filename,item['outputDataType']),'MODE',prodid)
      lfns.append(lfn)
          
  return {'OK':True, 'Value':lfns}
