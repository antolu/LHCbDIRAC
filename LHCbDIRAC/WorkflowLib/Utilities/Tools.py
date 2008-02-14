# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Utilities/Tools.py,v 1.2 2008/02/14 07:26:34 joel Exp $
__RCSID__ = "$Id: Tools.py,v 1.2 2008/02/14 07:26:34 joel Exp $"

import os, re, string
from DIRAC.Core.Utilities.Subprocess                     import shellCall


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

def gzip(fname):
  "interface to the system gzip command"
  if not os.path.exists(fname):
    print "gzip failed for file ",fname,": no such file"
    return 1

  result = shellCall(0,'gzip -f '+fname)
  status = result['Value'][0]
  output = result['Value'][1]
  if status > 0 :
    print "gzip failed for file ",fname
    print output
    return status

  return 0

def gunzip(fname):
  "interface to the system gunzip command"
  if not os.path.exists(fname):
    fname_gz = fname+".gz"
    if not os.path.exists(fname_gz):
      print "gunzip failed for file ",fname,": no such file"
      return 1
    else:
      fname = fname_gz

  result = shellCall(0,'gunzip '+fname)
  status = result['Value'][0]
  output = result['Value'][1]
  if status > 0 :
    print "gunzip failed for file ",fname
    print output
    return status

  return 0

def makeIndex():
  """ Function to make an index of log files in the current run directory"""

  jobname = None
  files = os.listdir('.')
  for f in files:
    if f == 'job.info':
      jobfile = open(f)
      jobname = jobfile.readline()
      jobname = string.strip(jobname)

  if jobname is None:
    cwd = os.path.basename(os.getcwd())
    try:
      run = int(cwd)
      jobname = str(run)
    except:
      print "Wrong current directory",cwd,", index failed"
      return

  if os.path.exists("index.html"):
    os.remove("index.html")

  flist = os.listdir(".")
  index = open("index.html","w")
  index.write( """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>\n""")
  index.write("<title>Logs for Run %s</title>\n" % jobname)
  index.write( """</head>
<body text="#000000" bgcolor="#33ffff" link="#000099" vlink="#990099"
 alink="#000099"> \n
""")
  index.write("<h3>Log files for Run %s</h3> \n" % jobname)

  for f in flist:
    if f != "index.html":
      index.write( '<a href="%s">%s</a><br> \n' % (f,f))

  index.write( '<a href="job.output">job.output</a><br> \n')

  index.write("""</body>
</html>""" )

  index.close()

  return 0
