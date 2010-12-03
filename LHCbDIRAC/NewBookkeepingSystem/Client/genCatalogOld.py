#!/usr/bin/env python2.2
"""
  Generates the POOL XML file catalog according to Gaudi job options\n
  Usage:
  genCatalog -o options1.opts [-o options1.opts] -c <input component 1> <input component 2>

  Options:
    -o <file>, --options=<file>
       Job option file to be processed for DATAFILE declarations
    -C <component property>, --component=<component property>
       Name of a component property to be checked in order to access
       input file declarations

    -p <file>, --pool=<file>
       Generate POOL file catalog and specify the name. Must be used with -s, -P options
    -c <file>, --Card=<file>
       Generate the Gaudi card and specify the name. Must be used with -s, -P options

    -g <file>, --gaudi=<file>
       - If an options file was specified, generate GAUDI file catalog
         using the information from the specified options files and the
         information in the LFC database
       - If no options are present assume, that a gaudi catalog exists
         and use this file as an input to produce a POOL file catalog
         slice for a given computing site

    -s, --site=<storage-name>
       Name of the current Site to generate the POOL catalog slice.
       Must be used with -p, -P options

    -d, --depth=<number>
       Depth of input sources contained in file catalog

    -P, --Protocol=<file access protocol>
       To generate the PFN with the specified protocol
       Must be used with -s, -p options
    -N, --nostrict Disable strict processing

    -h, --help     Print this help\n

    -u, --url=<Server URL>

    -i, --info   Print all the informations about sites, storage elemts
                 and protocols.

    Examples:

    $> genCatalog -o Brunel.opts -g myCatalog.xml -C EventSelector.Input -C SpilloverSelector.Input
       Will produce a complete catalog slice according to the LFC database

    $> genCatalog -g myCatalog.xml -p poolCatalog.xml -s CERN -P rfio
       Will produce a POOL file catalog slice according using the LFC
       database slice in myCatalog.xml and will project all files, which
       are availible at CERN.

    $> genCatalog -o Brunel.opts -p poolCatalog.xml -s CERN -P rfio
       Will produce a POOL file catalog slice according to the LFC database
       for CERN only;

    $> genCatalog -o Brunel.opts -c gaudicard.opts -s CERN -P rfio
       Will produce a Gaudi card according to the LFC database
       for CERN only;

"""

import sys, os, re, imp, string, xmllib
import threading,xmlrpclib,traceback


class StringFile:
  def __init__(self):
      self.str=''

  def write(self, *msg):
    for fild in msg:
      self.str=self.str+fild

  def read(self):
    return self.str


class Retriever(threading.Thread):
  def __init__(self, srv, fids,strict, depth):
    self.srv = srv
    self.fids = fids
    self.strict = strict
    self.result = ''
    self.depth = depth
    threading.Thread.__init__(self)

  def run(self):
    print 'Contacting catalog server at ', self.srv.url,'...'
    if ( self.depth == 1 ):
      reps = self.srv.server.replicas(self.fids.keys())
    else:
      reps = self.srv.server.replicasWithHistory(self.fids.keys(), self.depth)
    if reps['Status']=='OK':
      reps=reps['Result']
    else:
      print "ERROR: The following error was found \n"+str(reps['Message'])
      return
    #print 'Reps=', reps
    deferred = []
    for r in reps:
      if ( len(r[0]) > 0 ):
        if ( not self.fids.has_key(r[2]) ):
          deferred.append(r[2])
          self.fids[r[2]] = {}
          self.fids[r[2]]['replicas'] = {}
          if ( r[1][:4] == 'DIGI' ):
            self.fids[r[2]]['type'] = 'POOL_ROOT'
          elif ( r[1][:3] == 'SIM' ):
            self.fids[r[2]]['type'] = 'POOL_ROOT'
          elif ( r[1][:3] == 'DST' ):
            self.fids[r[2]]['type'] = 'POOL_ROOT'
          else:
            self.fids[r[2]]['type'] = r[1]
        self.fids[r[2]]['guid'] = r[0]
        self.fids[r[2]]['replicas'][r[4]] = r[3]
      else:
        self.srv.incomplete['Global'] = 1
        self.srv.incomplete['MissingLFNs'] = self.fids.keys()
    if(self.depth>1):
      print '.... added deferred sources:', deferred

    self.result = ''
    for f,ff in self.fids.items():
      try:
        if self.strict:
          self.result = self.result + self.srv.catalogEntry(ff['replicas'], f, ff['type'], ff['guid'], 'FromCatalog')
          if not len(ff['guid']) or not len(ff['replicas'].keys()):
              print 'Processing error: file ', f, ' was not found in the LFC database.'
              self.srv.incomplete['MissingLFNs'].append(f)
              self.srv.incomplete['Global'] = 1
        else:
          if len(ff['guid']) and len(ff['replicas'].keys()):
            self.result = self.result + self.srv.catalogEntry(ff['replicas'], f, ff['type'], ff['guid'], 'FromCatalog')
          else:
            print 'WARNING: file ', f, ' was not found in the LFC database.'
            self.srv.incomplete['MissingLFNs'].append(f)
      except KeyError, X:
        if self.strict:
            print 'Processing error: file ', f, ' was not found in the LFC database.'
            self.srv.incomplete['MissingLFNs'].append(f)
            self.srv.incomplete['Global'] = 1
        else:
          print 'WARNING: file ', f, ' was not found in the LFC database.'
          self.srv.incomplete['MissingLFNs'].append(f)
    self.fids = None
    self.done = 1

class Element:
  """
     A parsed XML element

     @author M.Frank

  """
  def __init__(self,name,attributes):
    """
        Element constructor
        @author M.Frank
    """
    # The element's tag name
    self.name = name
    # The element's attribute dictionary
    self.attributes = attributes
    # The element's cdata
    self.cdata = ''
    # The element's child element list (sequence)
    self.children = []

  def AddChild(self,element):
    """
        Add a reference to a child element
        @author M.Frank
    """
    self.children.append(element)

  def getAttribute(self,key):
    """
        Get an attribute value
        @author M.Frank
    """
    return self.attributes.get(key)

  def getData(self):
    """
        Get the cdata
        @author M.Frank
    """
    return self.cdata

  def getElements(self,name=''):
    """
        Get a list of child elements
        @author M.Frank
    """
    #If no tag name is specified, return the all children
    if not name:
        return self.children
    else:
        # else return only those children with a matching tag name
        elements = []
        for element in self.children:
            if element.name == name:
                elements.append(element)
        return elements

class Xml2Obj(xmllib.XMLParser):
  """
      XML to Object converter
      @author M.Frank
  """
  def __init__(self):
    xmllib.XMLParser.__init__(self)
    self.root = None
    self.nodeStack = []

  def encode(self, data):
    return data

  def Parse(self, data):
    """
        Parse the XML File
        @author M.Frank
    """
    self.feed(data)
    return self.root

  def handle_data(self, data):
    self.handle_cdata(data)

  def handle_cdata(self, data):
    'SAX character data event handler'
    import string
    if string.strip(data):
        data = self.encode(data)
        element = self.nodeStack[-1]
        element.cdata = element.cdata + data
        return

  def unknown_starttag(self, name, attributes):
    """
        SAX start element even handler
        Internal -- handle starttag, return length or -1 if not terminated
        @author M.Frank
    """
    # Instantiate an Element object
    element = Element(self.encode(name),attributes)
    # Push element onto the stack and make it a child of parent
    if len(self.nodeStack) > 0:
        parent = self.nodeStack[-1]
        parent.AddChild(element)
    else:
        self.root = element
    self.nodeStack.append(element)

  def unknown_endtag(self, data):
    """
        SAX end element event handler
        Internal -- handle starttag, return length or -1 if not terminated
        @author M.Frank
    """
    self.nodeStack = self.nodeStack[:-1]


class JobOptionParser:
  def __init__(self):
    self.rawIncludeFiles=[]
    self.rawInputFiles=[]

  def ParseFile(self, file,params):
      """
      Parse the file looking for 'include' statement and input file
      as specified by params.
      Essentially it extract only the lines containing include statement (put in rawIncludeFiles)
      and the line containing input files (put in rawInputFiles)

      @author C.Cioffi
      """
      print 'Parsing file',file+' ....'
      ex='\s*('
      for i in params:
          ex=ex+i+'|'
      ex=ex[:-1]+')'
      try:
          f=open(file,'r')
          while(1):
              l=f.readline()
              if not l:
                  break
              mo=re.match('\s*//',l)
              if mo!=None:
                  continue #jump the commented line
              mo=re.match('\s*#include\s',l)
              if mo!=None:
                  self.rawIncludeFiles.append(l)
                  continue

              mo=re.match(ex+'\s*(=|(\+=))',l)
              if mo!=None:
                  input=''
                  mo2=re.search('}',l)
                  while(mo2==None):#read up to }
                      mo3=l.replace(' ','')
                      if len(mo3) > 3 and not (mo3[:2]=='//' or mo3[:3]==',//'):
                        input=input+l
                      l=f.readline()
                      if not l:
                          print 'ERROR: Unexpected end of file'
                          sys.exit(1)
                      mo2=re.search('}',l)
                  else:
                    input=input+l
                    self.rawInputFiles.append(input)

          f.close()
      except IOError, e:
          print 'ERROR: ', e

  def ParseInput(self, input):
      """
      parse the string enclosed between the " to find the file name and the type of file.

      @author C.Cioffi
      """
      result=['','']
      mo=re.search('[dD][aA][tT].*?=\s*\'(.*?)\'',input)
      if mo!=None:
          result[0]=mo.group(1)
      else:
          return None
      mo=re.search('[Tt][Yy][Pp].*?=\s*\'(.*?)\'',input)
      if mo!=None:
          result[1]=mo.group(1)
      else:
          return None
      return result


  def Parse(self, fileName, goodInputs):
      """
        @author C.Cioffi
      """
      self.rawIncludeFiles=[]
      self.rawInputFiles=[]

      inputFiles=[]
      output=[]

      self.ParseFile(fileName,goodInputs)
      #loop on all the include statement to find the input files
      while(1):

          if self.rawIncludeFiles:
              include=self.rawIncludeFiles.pop()
              mo=re.search('".*"',include)

              if mo!=None:
                  file=mo.group(0)#exstract the file path
                  dirs=string.split(file[1:-1],os.sep)# the [1:-1] remove the " from
                  dirs=string.split(file[1:-1],'/')# the [1:-1] remove the " from
                                                      # the string
                  for i in range(len(dirs)):#parsing the file path to replace the
                                            #the enviroment variable if any
                      if dirs[i]!='':
                          if dirs[i][0]=='$':
                              replacement=''
                              try:
                                  replacement=os.environ[dirs[i][1:]]
                              except KeyError:
                                  print 'ERROR: in the included file',file,' the environment variable ',dirs[i],'is not defined!!!'
                                  sys.exit(1)
                              dirs[i]=replacement
                              newpath=''
                              for i in dirs:
                                  newpath=newpath+i+os.sep

              else:
                  print "ERROR include statement:",include,'not valide'
                  sys.exit(1)
              self.ParseFile(newpath[:-1],goodInputs)
          else:
              break

      #at this point rawInputFiles contain all the line containing the right input statement as
      #specified by goodInputs
      for i in self.rawInputFiles:
          rawInputs=string.split(i,'\n')
          for j in rawInputs:
              mo=re.search('".*"',j)#extract the string enclosed by "
              if(mo!=None):
                  inputFiles.append(mo.group(0))

      for i in inputFiles:
          result=self.ParseInput(i)
          if(result!=None):
              output.append(result)
      return output


class Bookkeeping:
  """
  """
  def __init__(self, url):
    import xmlrpclib
    xmlrpclib.ExpatParser = None
    self.url = url
    self.server = xmlrpclib.Server(url)
    self.incomplete = {'Global': 0,'MissingLFNs':[]}
    self.catalog = ''

  def __getPFNs(self,cat):
    pfns=[]
    parser = Xml2Obj()
    element = parser.Parse(cat)
    tree = {}
    self.analyse(element, tree)
    for guid in tree.keys():
      f = tree[guid]
      for p in f['physical']:
          pfns.append(p['name'])
    return pfns

  def __getLFNs(self,cat):
    lfns=[]
    parser = Xml2Obj()
    element = parser.Parse(cat)
    tree = {}
    self.analyse(element, tree)
    for guid in tree.keys():
      f = tree[guid]
      for p in f['logical']:
          lfns.append(p)
    return lfns


  def supportedSites(self):
    return self.server.supportedSites()

  def supportedProtocols(self):
    return self.server.supportedProtocols()

  def SEsProtocols(self):
    return self.server.SEsProtocols()

  def getAncestors(self,LFNs,depth=1):
    return self.server.inputs(LFNs,depth)

  def printCatalog(self, msg):
    self.catalog = self.catalog + msg

  def startCatalog(self, msg):
    self.catalog = msg

  def endCatalog(self, msg):
    self.catalog = self.catalog + msg
    return self.catalog


  def analyse(self, e, dic):
    if e.name=='File':
      id = e.attributes['ID']
      dic[id]={}
      for i in e.children:
        if i.name == 'logical':
          dic[id][i.name] = []
          self.analyse(i, dic[id][i.name])
        if i.name == 'physical':
          dic[id][i.name] = []
          self.analyse(i, dic[id][i.name])
      return
    elif e.name=='physical':
      for i in e.children:
        self.analyse(i, dic)
      return
    elif e.name=='logical':
      for i in e.children:
        self.analyse(i, dic)
      return
    elif e.name=='pfn':
      dic.append(e.attributes)
      return
    elif e.name=='lfn':
      dic.append(e.attributes['name'])
      return
    for i in e.children:
      self.analyse(i,dic)
    return

  def catalogEntry(self, replicas, fname, filetype, guid, guid_typ):
    res = ''
    res = res + '  <File ID="'+guid+'" guid="'+guid_typ+'">\n'
    res = res + '    <physical>\n'
    for r in replicas.keys():
      res = res + '      <pfn name="'+replicas[r]+'" site="'+r+'" filetype="'+filetype+'"/>\n'
    res = res + '    </physical>\n'
    res = res + '    <logical>\n'
    res = res + '      <lfn name="'+fname+'"/>\n'
    res = res + '    </logical>\n'
    res = res + '  </File>\n'
    return res

  def siteCatalog(self, cat, site, strict,protocol,card):
    SEs=self.server.sitesTOses([site])
    SEsProtocols=self.server.SEsProtocols()
    parser = Xml2Obj()
    element = parser.Parse(cat)
    tree = {}
    self.analyse(element, tree)
    job_opts='EventSelector.Input   = {\n'
    datafile=''
    datafiles=''
    ent = '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n'
    ent = ent + '<!-- POOL XML file catalog: Edited By Gaudi Bookkeeping tools -->\n\n'
    ent = ent + '<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n\n'
    ent = ent + '<POOLFILECATALOG>\n'
    self.startCatalog(ent)
    self.incomplete[site] = self.incomplete['Global']
    use = 0
    for guid in tree.keys():
      f = tree[guid]
      res = '<File ID="'+guid+'">\n <physical>\n'
      use = 0
      datafile=''
      for p in f['physical']:
        if p['site'] in SEs and protocol in SEsProtocols[p['site']]:
          prot_pfn=self.server.getPfnForProtocol(p['site'],p['name'],protocol)
          if(prot_pfn['Status']=='OK')and(len(prot_pfn['Result'])>0):
            if protocol=='srm':
              pfn='gfal:'+prot_pfn['Result'][p['name']]
              datafile='"DATAFILE=\''+pfn+'\'  TYP=\''+p['filetype']+'\'  OPT=\'READ\'",\n'
            elif protocol=='gsidcap':
              pfn='dcap:'+prot_pfn['Result'][p['name']]
              datafile='"DATAFILE=\''+pfn+'\'  TYP=\''+p['filetype']+'\'  OPT=\'READ\'",\n'
            else:
              pfn=prot_pfn['Result'][p['name']]
              datafile='"DATAFILE=\'PFN:'+pfn+'\'  TYP=\''+p['filetype']+'\'  OPT=\'READ\'",\n'
            #datafile=datafile+pfn+'\'  '
            use=1

          else:
            pfn=""
          filetype = p['filetype']
          if ( filetype[:9] == 'POOL_ROOT' ):
            filetype = 'ROOT_All'
          res = res + '  <pfn name="'+pfn+'" filetype="'+filetype+'"/>\n'

        elif (p['site'] in SEs and protocol not in SEsProtocols[p['site']] and strict):
          print "Protocol \""+protocol+"\" not valid for file "+f['logical'][0]
          print "valid protocols are "+str(SEsProtocols[p['site']])
          self.incomplete['MissingLFNs'].append(f['logical'][0])
          #self.incomplete[site] = 1


      res = res + ' </physical>\n'
      res = res + ' <logical>\n'
      for p in f['logical']:
        res = res + '  <lfn name="'+p+'"/>\n'
      res = res + ' </logical>\n</File>\n'
      datafiles=datafiles+datafile
      if use:
        self.printCatalog(res)
      else:
        if strict:
          self.incomplete[site] = 1
          print 'Processing error: file ',f['logical'][0] ,' is missing'
          self.incomplete['MissingLFNs'].append(f)
        else:
          print 'WARNING: file ',f['logical'][0] ,' is missing'
          self.incomplete['MissingLFNs'].append(f)

    if (not self.incomplete[site]) and card and (use or not strict):
      try:
        card_file=open(card,'w')
        card_file.write(job_opts+datafiles[:-2]+'};')
        card_file.close()
      except Exception, x:
        print "ERROR while writing the file "+card
        print str(x)

    self.endCatalog('</POOLFILECATALOG>\n')
    return self.catalog

  """def gaudiCatalogBkk(self, files, strict, depth=1):
    catalog = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    catalog = catalog + '<!-- GaudiPoolCatalog: Edited By Gaudi Bookkeeping tools -->\n'
    catalog = catalog + '<GaudiPoolCatalog>\n'
    self.startCatalog(catalog)
    for ff in files:
      fname = ff[0]
      filetype = ff[1]
      if fname[0:4] == 'LFN:':
        fname = fname[4:]
      elif fname[0:4] == 'PFN:':
        # error
        pass
      else:
        print 'Unknown file specification:', fname
        pass
      file     = self.server.file(fname)
      guid     = self.server.fileParameter(file, 'GUID')
      guid_typ = 'FromCatalog'
      print ' --- processing ', fname
      if guid.__class__ == 'hello'.__class__:
        guid_typ = 'Generated'
        try:
          import md5
          s = md5.new(fname).hexdigest().upper()
          uuid = '%8s-%4s-%4s-%4s-%12s' % (s[0:8], s[8:12], s[12:16], s[16:20], s[20:32])
          guid  = {'ID': -1, 'value': uuid, 'name': 'GUID'}
        except:
          err = 'No GUID: '+fname+' '+str(guid)
          print err
          raise err
      replicas = self.server.replicas(file)
      if replicas.__class__ == 'hello'.__class__:
        # Error
        self.incomplete['Global'] = 1
        pass
      res = self.catalogEntry(replicas, fname, filetype, guid['value'], guid_typ)
      self.printCatalog(res)
    self.endCatalog('</GaudiPoolCatalog>\n')
    return self.catalog
  """

  def gaudiCatalogFromLFNs(self, files, strict, depth=1):
    import time
    srv = self.server
    fids = {}
    catalog = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    catalog = catalog + '<!-- GaudiPoolCatalog: Edited By Gaudi Bookkeeping tools -->\n'
    catalog = catalog + '<GaudiPoolCatalog>\n'
    self.startCatalog(catalog)
    length = len(files)
    count = 0
    prev = None
    threaded = 0
    start = time.time()
    for ff in files:
      """extention=ff[ff.rfind('.')+1:]
      if string.upper(extention) in ('DST','DIGI','SIM','RDST'):
        type='POOL_ROOTTREE'
      elif string.find(ff,'ETC')>-1:
        type='ROOT_ALL'
      else: # raw files
        type='ROOT_ALL'
      """
      fids[ff] = {'type':'ROOT_ALL', 'replicas':{}, 'guid':''}
      count = count + 1
      if ( (count % 500) == 0 or count == length ):
        start2 = time.time()
        if threaded:
          t = Retriever(self, fids,strict, depth)
          fids = {}
          t.start()
          if ( prev is not None ):
            tt = prev
            prev = t
            tt.join()
            self.printCatalog(tt.result)
          if ( count == length ):
            t.join()
            self.printCatalog(t.result)
          else:
            prev = t
        else:
          t = Retriever(self, fids, strict, depth)
          t.run()
          self.printCatalog(t.result)
          fids = {}
        #print '... processed %6d files in %7.2f seconds' % (count , time.time()-start2)
    self.endCatalog('</GaudiPoolCatalog>\n')
    #print ' processing took %7.2f seconds.' % (time.time()-start)
    return self.catalog


  def gaudiCatalog(self, files, strict, depth=1):
    import time
    srv = self.server
    fids = {}
    catalog = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    catalog = catalog + '<!-- GaudiPoolCatalog: Edited By Gaudi Bookkeeping tools -->\n'
    catalog = catalog + '<GaudiPoolCatalog>\n'
    self.startCatalog(catalog)
    length = len(files)
    count = 0
    prev = None
    threaded = 0
    start = time.time()
    for ff in files:
      fname = ff[0]
      if fname[0:4] == 'LFN:':
        fname = fname[4:]
      elif fname[0:4] == 'FID:':
        print 'Unknown file specification:', fname, 'need LFN:'
        fname = fname[4:]
        continue
      elif fname[0:4] == 'PFN:':
        print 'Unknown file specification:', fname, 'need LFN:'
        fname = fname[4:]
        continue
      else:
        print 'Unknown file specification:', fname, 'need LFN:'
        continue
      fids[fname] = {'type':ff[1], 'replicas':{}, 'guid':''}
      count = count + 1
      if ( (count % 500) == 0 or count == length ):
        start2 = time.time()
        if threaded:
          t = Retriever(self, fids,strict, depth)
          fids = {}
          t.start()
          if ( prev is not None ):
            tt = prev
            prev = t
            tt.join()
            self.printCatalog(tt.result)
          if ( count == length ):
            t.join()
            self.printCatalog(t.result)
          else:
            prev = t
        else:
          t = Retriever(self, fids, strict, depth)
          t.run()
          self.printCatalog(t.result)
          fids = {}
        #print '... processed %6d files in %7.2f seconds' % (count , time.time()-start2)
    self.endCatalog('</GaudiPoolCatalog>\n')
    #print ' processing took %7.2f seconds.' % (time.time()-start)
    return self.catalog

#----------------------------------------------------------------------------------
def usage() :
  print 'Usage:'
  print '  genCatalog -o <option file> -g <gaudi name> [-d -N] | -g <gaudi file>  -p <pool name> -s <site> -P <protocol> [-N -d] | ...'
  print 'Try "genCatalog --help" for more information.'
  return 0

#----------------------------------------------------------------------------------
def help() :
  print __doc__
  sys.exit()
#----------------------------------------------------------------------------------

## module specific methods and variables
_url_='http://lhcbbk.cern.ch:8110/RPC/FileCatalog'


def S_ERROR( sMessage = '' ):
  return { 'Status':'Error', 'OK' : 0, 'Message' : sMessage  }

def S_OK( sValue = None, sPname = 'Value' ):
  dResult = { 'Status': 'OK', 'OK' : 1 }
  if sValue is not None:
    dResult[ sPname ] = sValue
  return dResult

def S_WARNING( sMessage = '' ):
  return { 'Status':'Warning', 'OK' : 1, 'Message' : sMessage  }

def setURL(url):
  global _url_
  _url_=url

def getAncestors(LFNs,depth=1):
  print 'contacting '+_url_
  try:
    obj = Bookkeeping(_url_)
    result=S_OK()
    result['PFNs']=obj.getAncestors(LFNs,depth)
    return result
  except Exception,x:
    sf=StringFile()
    traceback.print_exc(file=sf)
    err_msg=S_ERROR(sf.read())
    err_msg['call']='\n command:  getAncestors('+str(LFNs)+','+str(depth)+')'
    return err_msg


def genCatalog(LFNs,outputFilename,depth=1,site=None,protocol=None):
  print 'contacting '+_url_
  try:
    obj = Bookkeeping(_url_)
    ancestorsLFNs=LFNs
    if depth>1:
      res=obj.getAncestors(LFNs,depth)
      if res['Status']=='OK':
        ancestorsLFNs=res['PFNs']
      else:
        return res
    if (site==None and protocol!=None) or (site!=None and protocol==None):
      return S_ERROR("Both site and protocol must be != None. Got site="+str(site)+"; protocol="+str(protocol))
    if site==None:
      cat = obj.gaudiCatalogFromLFNs(LFNs, 0, 1)
      cat_file = open(outputFilename, "w")
      print >>cat_file, cat
      cat_file.close()
      #print obj.incomplete['MissingLFNs'],len(obj.incomplete['MissingLFNs']),'\n',ancestorsLFNs,len(ancestorsLFNs)
      if len(obj.incomplete['MissingLFNs'])>0:
        if len(obj.incomplete['MissingLFNs'])==len(ancestorsLFNs):
          return S_ERROR("PFNs not found")
        else:
          result=S_WARNING("Some PFNs have not been found")
          result['PFNs']=obj.incomplete['MissingLFNs']
          return result
      else:
        return S_OK()
    else:
      result=None
      cat = obj.gaudiCatalogFromLFNs(LFNs, 0, 1)
      #print obj.incomplete['MissingLFNs'],len(obj.incomplete['MissingLFNs']),'\n',ancestorsLFNs,len(ancestorsLFNs)
      if len(obj.incomplete['MissingLFNs'])>0:
        if len(obj.incomplete['MissingLFNs'])==len(ancestorsLFNs):
          return S_ERROR("PFNs not found")
        else:
          result=S_WARNING("Some PFNs have not been found")
          result['PFNs']=obj.incomplete['MissingLFNs']

      catPool = obj.siteCatalog(cat, site, 0,protocol,0)
      cat_file = open(outputFilename, "w")
      print >>cat_file, catPool
      cat_file.close()
      #print obj.incomplete['MissingLFNs'],len(obj.incomplete['MissingLFNs']),'\n',ancestorsLFNs,len(ancestorsLFNs)
      if len(obj.incomplete['MissingLFNs'])>0:
        if len(obj.incomplete['MissingLFNs'])==len(ancestorsLFNs):
          return S_ERROR("PFNs not found")
        else:
          if result is None:
             result=S_WARNING("Some PFNs have not been found")
             result['PFNs']=obj.incomplete['MissingLFNs']
          else:
            result['PFNs'].append(obj.incomplete['MissingLFNs'])
          return result
      else:
        return S_OK()
  except Exception,x:
    print '==========',str(x)
    sf=StringFile()
    traceback.print_exc(file=sf)
    err_msg=S_ERROR(sf.read())
    err_msg['call']='genCatalog('+str(LFNs)+','+str(outputFilename)+','+str(depth)+','+str(site)+','+str(protocol)+')'
    return err_msg
#########################################



def execute(args) :
  import getopt
  url = 'http://lhcbbk.cern.ch:8111/RPC/FileCatalog'
  #url = 'http://lxplus060.cern.ch:8000'
  options = []
  components = []
  files = []
  pool = None
  gaudi = None
  site = None
  protocol = None
  card=None
  #quick = 1
  strict = 1
  depth = 1
  info=0
  sup_site=None
  sup_prot=None
  seTOprot=None
  #----Ontain the list of files to process------------
  for a in args:
    if a[0] != '-' :
      files.append(a)
    else :
      options = args[args.index(a):]
      break

  #----Process options--------------------------------
  try:
    opts, args = getopt.getopt(options, 'ibqhSNZ:o:c:d:p:s:g:u:pr:P:C:I:U:D:PC', \
    ['info','help','options=','component=','gaudi=','pool=','quick','bkk','site=','strict','nostrict','depth=','url=','Protocol=','card='])
  except getopt.GetoptError:
    return usage()

  #print opts, args
  for o, a in opts:
    if o in ('-h', '--help'):
      help()
      return 0
    if o in ('-i','--info'):
      info=1
    if o in ('-o', '--options'):
      files.append(a)
    if o in ('-C', '--component'):
      components.append(a)
    if o in ('-c','--card'):
      card=a
    if o in ('-d', '--depth'):
      depth = int(a)
    if o in ('-p', '--pool'):
      pool = a
    if o in ('-g', '--gaudi'):
      gaudi = a
    if o in ('-s', '--site'):
      site = a
    if o in ('-u', '--url'):
      url = a
    #if o in ('-q', '--quick'):
    #  quick = 1
    #if o in ('-b', '--bkk', '--bookkeeping'):
    #  quick = 0
    #if o in ('-S', '--strict'):
    #  strict = 1
    if o in ('-N', '--nostrict'):
      strict = 0
    if o in ('-P','--Protocol'):
      protocol=a

  #---Check existance of input files--------------------
  if files:
    for f in files :
      if not os.path.exists(f) :
        print 'options file "' + f + '" not found'
        return usage()
  if gaudi and not files:
       if not os.path.exists(gaudi) :
        print 'Gaudi catalog file "' + gaudi + '" not found'
        return usage()

  #else :
  #  print 'No input file specified'
  #  return usage()
  if ( len(components) == 0 ):
    components.append('EventSelector.Input')

  obj = Bookkeeping(url)
  #---Check the correctness of the POOL raled options
  if pool or info or card:
    sup_site=obj.supportedSites()
    sup_prot=obj.supportedProtocols()
    if info:
        seTOprot=obj.SEsProtocols()
  if pool or card:
    if site and protocol:
      if sup_site.has_key(site):
        if sup_prot.has_key(site):
          if not (protocol in sup_prot[site]):
            print "The protocol \""+protocol+"\" is not valid for the site "+site
            print "Valid protocols could be: "+str(sup_prot[site])
            return
        else:
          print "No protocol is defined for the site "+site
          print "This are the valid site, protocol"
          for site, protocol in sup_prot.items():
            print site, protocol
          return

      else:
        print "The site is not recognize as a valid site"
        print "Valid sites are: "+str(sup_site.keys())
        return
    else:
      if card:
        print "The -c option mast be used in conjunct with the -s and -P options"
      if pool:
        print "The -p option mast be used in conjunct with the -s and -P options"
      return
  if files and pool is None and gaudi is None and card is None:
    print "Warning: No output file was specified, expected -p or -g or -c option"
    return usage()
  all_files = []
  cat = None
  for f in files :
    parser = JobOptionParser()
    in_files = parser.Parse(f,components)
    for ff in in_files:
      all_files.append(ff)
  if ( len(all_files) > 0 ):
    #if quick:
    cat = obj.gaudiCatalog(all_files, strict, depth)
    #else:
    #  cat = obj.gaudiCatalogBkk(all_files, strict, depth)
    if ( gaudi and not obj.incomplete['Global'] ):
      if(string.upper(gaudi[-4:])!='.XML'):
        gaudi=gaudi+'.xml'
      cat_file = open(gaudi, "w")
      print >>cat_file, cat
      cat_file.close()
      print 'Wrote Gaudi catalog file:', gaudi
    elif obj.incomplete['Global']:
      print '+---------------------------------------------------------------------+'
      print '| You are going to analyse non-existing datafiles. Your job will FAIL |'
      print '| Catalog processing ABORTED.                                         |'
      print '|                                                                     |'
      print '| The system by default check that all files are found.               |'
      print '| To disable such behavior use the -N option.                         |'
      print '+---------------------------------------------------------------------+'
      return 0
  elif ( gaudi ):
    cat = open(gaudi,'r').read()
  #print cat,'\n\n',pool,site,protocol,
  if ( (pool or card) and site and cat and protocol):
    if(card is not None) and (string.upper(card[-5:])!='.OPTS'):
      card=card+'.opts'
    cat = obj.siteCatalog(cat, site, strict,protocol,card)
    if ( not obj.incomplete[site] ):
      if pool:
        if(string.upper(pool[-4:])!='.XML'):
          pool=pool+'.xml'
        cat_file = open(pool, "w")
        print >>cat_file, cat
        cat_file.close()
      if pool:
         print 'Wrote POOL catalog file:', pool
      if card:
         print 'Wrote Gaudi card:', card
      if not card:
          print 'Please add to your options file the following line:'
          print 'PoolDbCacheSvc.Catalog = { "xmlcatalog_file:'+pool+'" }';

    elif ( obj.incomplete[site] ):
      blnk ='|  Your Gaudi job is going to FAIL at site:                           |'
      print '+---------------------------------------------------------------------+'
      print '|  You are going to analyse non-existing or not valid datafiles.      |'
      print blnk[:44]+site+blnk[44+len(site):]
      print '| Catalog procesing ABORTED.                                          |'
      print '|                                                                     |'
      print '| The system by default check that all files are found and            |'
      print '| convertible to the right protocol.                                  |'
      print '| To disable such behavior use the -N option.                         |'
      print '+---------------------------------------------------------------------+'
      return 0
  elif (pool and cat and (site is None or protocol is None)):
        print "Warning: -p option must be used in conjuct with -s and -P options"
        print 'Try "genCatalog --help" for more information.'
  if (info):
     space='                        '
     print '\n                INFO\n'
     print 'Site'+space[len('Site'):] +'SEs\n'
     for site, se in sup_site.items():
       print site,space[len(site):],se
     print '\nSE'+space[len('SE'):]+'Protocol(s)\n'
     for se, pro in seTOprot.items():
       print se,space[len(se):],pro
  return 1

if __name__ == "__main__":
  execute(sys.argv[1:])
