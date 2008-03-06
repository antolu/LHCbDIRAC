# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Utilities/Tools.py,v 1.16 2008/03/06 15:18:58 gkuznets Exp $
__RCSID__ = "$Id: Tools.py,v 1.16 2008/03/06 15:18:58 gkuznets Exp $"

import os, re, string
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog


def makeProductionLfn(JOB_ID,LFN_ROOT,filetuple,mode,prodstring):
    """ Constructs the logical file name according to LHCb conventions.
    Returns the lfn without 'lfn:' prepended
    """

    try:
      jobid = int(JOB_ID)
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
        return LFN_ROOT+'/'+filetuple[1].upper()+'/'+prodstring+'/'+jobindex+'/'+filetuple[0]

def makeProductionPath(JOB_ID,LFN_ROOT,typeName,mode,prodstring,log=False):
  """ Constructs the path in the logical name space where the output
  data for the given production will go.
  """
#  result = '/lhcb/'+mode+'/'+self.CONFIG_NAME+'/'+self.CONFIG_VERSION+'/'+prodstring+'/'
#  result = '/lhcb/'+self.DataType+'/'+self.YEAR+'/'+self.appType.upper()+'/'+self.CONFIG_NAME+'/'+prodstring+'/'
  result = LFN_ROOT+'/'+typeName.upper()+'/'+prodstring+'/'
  if log:
    try:
      jobid = int(JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'
    result += jobindex

  return result

def tar(fname,file):
  "interface to the system tar command"
  if os.path.exists(fname):
    print "tar failed for file ",fname,": file already exist"
    return 1

  result = shellCall(0,'tar cf '+fname+' '+file)
  status = result['Value'][0]
  output = result['Value'][1]
  if status > 0 :
    print "tar failed for file ",fname
    print output
    return status

  return 0

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

#  index.write( '<a href="std.out">std.out</a><br> \n')

  index.write("""</body>
</html>""" )

  index.close()

  return 0

def hpsssize(fname):

  status,out = commands.getstatusoutput("rfdir "+fname)
  if status:
    # rfdir failed, return 0
    return 0
  else:
    fields = string.split(out)
    size = int(fields[4])
    return size

def getfilesize(fname):

  if not os.path.exists(fname):
    if os.path.islink(fname):
      fname = os.readlink(fname)
    result = re.search("hpss|HPSS|castor", fname)
    if result is not None:
      return hpsssize(fname)
    else:
      return 0
  else:
    if os.path.isdir(fname):
      return 0
    else:
      return os.path.getsize(fname)


def uniq(list):
  """ Creates a copy of the list removing all the duplicate entries
      but retaining the order of the first occurences of the values
  """

  new_list = []
  for i in list:
    if not i in new_list:
      new_list.append(i)

  return new_list


def getPFNFromPoolXMLCatalog(poolXMLCatName,output):

#    self.prod_id = self.PRODUCTION_ID
#    self.job_id = self.JOB_ID

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
      if poolXMLCatName != None:
        fcn = poolXMLCatName
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

    fcnames = uniq(fcname)
    print "BookkeepingReport: Pool XML catalog files: ",fcnames

    if fcnames:
      poolcat = PoolXMLCatalog(fcnames)

    print "BookkeepingReport: Pool XML catalog constructed"

    try:
      pfnName = poolcat.getPfnsByLfn(output)
      return pfnName
    except Exception,x :
      self.log.error( "Failed to get PFN from PoolXMLCatalog ! %s" % str( x ) )
      return ''


def getGuidFromPoolXMLCatalog(poolXMLCatName,output):

#    self.prod_id = self.PRODUCTION_ID
#    self.job_id = self.JOB_ID

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
      if poolXMLCatName != None:
        fcn = poolXMLCatName
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

    fcnames = uniq(fcname)
    print "BookkeepingReport: Pool XML catalog files: ",fcnames

    if fcnames:
      poolcat = PoolXMLCatalog(fcnames)

    print "BookkeepingReport: Pool XML catalog constructed"

    try:
      guid = poolcat.getGuidByPfn(output)
      return guid
    except Exception,x :
      self.log.error( "Failed to get GUID from PoolXMLCatalog ! %s" % str( x ) )
      return ''

def copyClassAttributes(from_, to_, except_=[]):
  """copy class attributes from one class to another"""
  for attr in from_.__dict__.keys():
    if attr not in except_:
      to_.__dict__[attr]=from_.__dict__[attr]

def combineMutipleAttributes(obj):
  """ find all atributes with the names xxxx_1=valx1 xxxx_2=valx2 xxxx_3=valx3
  yyyy_1=valy1 yyyy_2=valy2 yyyy_3=valy3
  and group them into dictionary of dictionaries
  [1]['xxxx']=valx1
  [1]['yyyy']=valy1
  [2]['xxxx']=valx2
  [2]['yyyy']=valy2
  [3]['xxxx']=valx3
  [3]['yyyy']=valy3
  """
  ret = {}
  for attr in obj.__dict__.keys():
    # lets find index
    index = attr.rfind('_')
    if index > 0:
      attr_name=attr[0:index]
      attr_index=attr[index+1:]
      if attr_index.isdigit() :
        attr_number = int(attr_index)
        if not ret.has_key(attr_number) :
          ret[attr_number]={}
        ret[attr_number][attr_name]=obj.__dict__[attr]

  return ret
