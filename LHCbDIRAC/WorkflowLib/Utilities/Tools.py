# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Utilities/Tools.py,v 1.1 2008/02/13 14:17:52 joel Exp $
__RCSID__ = "$Id: Tools.py,v 1.1 2008/02/13 14:17:52 joel Exp $"

import re, string

def makeProductionLfn(self,filetuple,mode,prodstring):
    """ Constructs the logical file name according to LHCb conventions.
    Returns the lfn without 'lfn:' prepended
    """

    try:
      jobid = int(self.JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'

    fname = filetuple[0]
    if re.search('lfn:',fname):
      return fname.replace('lfn:','')
    else:
      if re.search('LFN:',fname):
        return fname.replace('LFN:','')
      else:
#        path = makeProductionPath(self,mode,prodstring)
        return self.LFN_ROOT+'/'+filetuple[1]+'/'+prodstring+'/'+jobindex+'/'+filetuple[0]

def makeProductionPath(self,typeName,mode,prodstring,log=False):
  """ Constructs the path in the logical name space where the output
  data for the given production will go.
  """
#  result = '/lhcb/'+mode+'/'+self.CONFIG_NAME+'/'+self.CONFIG_VERSION+'/'+prodstring+'/'
#  result = '/lhcb/'+self.DataType+'/'+self.YEAR+'/'+self.appType.upper()+'/'+self.CONFIG_NAME+'/'+prodstring+'/'
  result = self.LFN_ROOT+'/'+typeName+'/'+self.CONFIG_NAME+'/'+prodstring+'/'

  if log:
    try:
      jobid = int(self.JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'
    result += 'LOG/'+jobindex

  return result
