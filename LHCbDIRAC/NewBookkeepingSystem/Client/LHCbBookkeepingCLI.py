
import string, sys, cmd, pydoc


from LHCbDIRAC.NewBookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient

#############################################################################
class LHCbBookkeepingCLI(cmd.Cmd):

  #############################################################################
  def __init__(self):
    cmd.Cmd.__init__(self)
    self.prompt = "$[/]$"
    self.__bk = LHCB_BKKDBClient()
    self.__currentPath = '/'
    self.do_setDataQualityFlags('OK')

  #############################################################################
  def setCurrentPath(self, path):
    self.__currentPath = path

  #############################################################################
  def addCurrentPath(self, path):

    if path[0] != '/' and len(self.__currentPath) == 1:
      self.__currentPath += path
    else:
      self.__currentPath += '/' + path

  #############################################################################
  def getCurrentPath(self):
    return self.__currentPath

  #############################################################################
  def __printPrompt(self,  path = '/'):
    self.prompt = '$[' + path + ']$'

  #############################################################################
  def __bklist(self, path):
    retVal =  self.__bk.list(path)
    retVal.sort()
    return retVal

  def __bkListAll(self, path):
    if path == '':
      path = self.getCurrentPath()
    res = self.__bk.list(path)
    for i in res:
      print i['name']
      for j in i:
        if j not in ['fullpath', 'selection','expandable', 'method','level','name']:
          print '   ',j,i[j]

  #############################################################################
  def __checkDirectory(self, path):
    res = self.__bk.list(path)
    retValue = False
    if len(res) > 0:
      retValue = True
    return retValue

  #############################################################################
  def __rootDirectory(self):
    self.setCurrentPath('/')
    self.__printPrompt(self.getCurrentPath())

  #############################################################################
  def __oneLevelback(self, logical = False):
    path = self.getCurrentPath().split('/')
    if path[0] == '' and path[1] == '':
      if not logical:
        self.setCurrentPath('/')
        self.__printPrompt(self.getCurrentPath())
      else:
        return '/'
    else:
      newpath = ''
      for i in range(len(path) -1):
        if path[i] != '':
          newpath += '/' + path[i]
      if newpath == '': newpath = '/'
      if not logical:
        self.setCurrentPath(newpath)
        self.__printPrompt(self.getCurrentPath())
      else:
        return newpath

  #############################################################################
  def help_ls(self):
    print "Usage: ls [OPTION]... [FILE]..."
    print "List information about the FILEs (the current directory by default)."
    print " Available options: -a"
    print 'Usage: ls or ls -a'
    print 'ls -a [FILE]'
    print 'Note: You can do paging using the | more'
    print 'For example: ls | more'

  def help_cd(self):
    print " cd <dir>"
    print "cd .."
    print "cd /"
    print "cd"

  #############################################################################
  def do_ls(self, path):
    paging = False
    if path.find('|')>-1:
      tmpPath = path.split('|')
      path = ''
      for i in range(len(tmpPath)-1):
        path += tmpPath[i].strip()
      paging = True

    text = ''
    if len(path) > 0 and path[0] == '-':
      if path[1] != 'a':
        print "ls: invalid option -- %s"%(path[1])
        print "Try `help ls' for more information."
      else:
        path = path[2:]
        self.__bkListAll(path)
    elif path == '':
      res =  self.__bklist(self.getCurrentPath())
      for i in res:
        if paging:
          text += i['name']+'\n'
        else:
          print i['name']
    else:
      res =  self.__bklist(path)
      for i in res:
        if paging:
          text += i['name']+'\n'
        else:
          print i['name']
    if paging:
      pydoc.ttypager(text)


  #############################################################################
  def do_list(self, path):
    print 'Not implemented!!!'

  #############################################################################
  def do_save(self, command):
    parameters = command.split(' ')
    filename = parameters[0]
    saveType = parameters[1]
    files = self.__bk.list(self.getCurrentPath())
    lfns = {}
    for i in range(len(files)):
      lfns[files[i]['FileName']] = files[i]

    if saveType == 'txt':
      text = self.__bk.writeJobOptions(lfns,'', saveType)
      f = open(filename,'w')
      f.write(text)
      f.close()
    elif saveType == 'py':
      text = self.__bk.writeJobOptions(lfns,filename, saveType)
      f = open(filename,'w')
      f.write(text)
      f.close()

  #############################################################################
  def do_cd(self, path):
    newpath = self.getCurrentPath()+ '/'+path
    if path == '':
      self.setCurrentPath('/')
      self.__printPrompt(self.getCurrentPath())
    elif path == '..':
      self.__oneLevelback()
    elif path == '/':
      self.__rootDirectory()
    elif path[0] == '/':
      if self.__checkDirectory(path):
        self.setCurrentPath(path)
        self.__printPrompt(self.getCurrentPath())
      else:
        print 'No such file or directory.'
    elif self.__checkDirectory(newpath):
      self.addCurrentPath(path)
      self.__printPrompt(self.getCurrentPath())
    else:
      print 'No such file or directory.'


  #############################################################################
  def do_pwd(self, path):
    print self.getCurrentPath()

  #############################################################################
  def do_queries(self, command=''):
    retVal = self.__bk.getPossibleParameters()
    print 'The following bookkeeping query types are available:'
    for i in retVal:
      print ' '.rjust(10)+i
    print "You can change the query types using the 'use' command"

  #############################################################################
  def do_use(self, command):
    self.__bk.setParameter(str(command))
    self.do_cd('/')

  #############################################################################
  def help_use(self):
    print 'Usage:'
    print 'use type'.rjust(10)
    print "The 'type' can be found using the 'queries' command!"
    print "EXAMPE:"
    print ' '.rjust(10)+"use 'Event type'"

  #############################################################################
  def help_queries(self):
    print "This method shows the available query types!"
    print "You can choose a query type using the 'use' command  "
    print " NOTE: the default query type is 'Configuration'"

  #############################################################################
  def do_advanceQuery(self, command = ''):
    self.__bk.setAdvancedQueries(True)
    self.do_cd('/')

  #############################################################################
  def help_advanceQuery(self):
    print "It allows to see more level of the Bookkeeping Tree"

  #############################################################################
  def do_standardQuery(self, command = ''):
    self.__bk.setAdvancedQueries(False)
    self.do_cd('/')

  #############################################################################
  def help_standardQuery(self):
    print "Using this command we only can see the important level of the Bookkeeping Tree"

  #############################################################################
  def do_dataQuality(self, command=''):
    print 'The following Data Qaulity flags are available in the boookkeeping!'
    retVal = self.__bk.getAvailableDataQuality()
    if retVal['OK']:
      for i in retVal['Value']:
        print ' '.ljust(10)+i
    else:
      print retVal["Message"]
    print "To set the data quality flags you heve to use 'setDataQualityFlags' command!"
    print "More information: 'help setDataQualityFlags'"

  #############################################################################
  def help_dataQuality(self):
    print 'This command shows the available data quality flags.'
    print 'To change the data quality flag use the setDataQualityFlags command'

  #############################################################################
  def do_setDataQualityFlags(self, command):
    qualities = command.split(' ')
    if len(qualities) > 0:
      d = {}
      for i in qualities:
        d[i] = True
      self.__bk.setDataQualities(d)
    else:
      print 'ERROR: Please give a data quality flag!'

  #############################################################################
  def __moreInfoProcpass(self, command):
    found = False
    retVal = self.__bk.getProcessingPassSteps({'StepName':command})
    if retVal['OK']:
      proc = retVal['Value']
      print '%d %s step founf in the bkk'%(proc['TotalRecords'],command)
      for i in proc['Records']:
        print ' '.ljust(5)+i
        for j in proc['Records'][i]:
          print ' '.ljust(10)+str(j[0])+':'+str(j[1])
        found = True
    else:
      print 'ERROR: ',retVal['Message']
    return found

  #############################################################################
  def do_moreinfo(self, command = ''):
    if command == '':
      previouspath = self.__oneLevelback(self.getCurrentPath())
      values = self.__bklist(previouspath)
      name = self.getCurrentPath().split('/')
      for i in values:
        if i.has_key('level') and i['level'] =='FileTypes':
          path = self.getCurrentPath()
          retVal = self.__bk.getLimitedFiles({'fullpath':str(path)},['nb'],-1,-1)
          print 'The selected dataset is:'
          for i in retVal['Extras']['Selection']:
            print ''.ljust(5)+i+' '+str(retVal['Extras']['Selection'][i])
          print 'Statistics:'
          print ' '.ljust(5)+'Number of files:'+str(retVal['TotalRecords'])
          for i in retVal['Extras']['GlobalStatistics']:
            print ''.ljust(5)+i+' '+str(retVal['Extras']['GlobalStatistics'][i])
          break
        if i['name'] == name[len(name)-1]:
          for j in i:
            if j not in ['fullpath', 'selection','expandable', 'method','level','name']:
              print '   ',j,i[j]
        if i.has_key('level') and i['level'] == 'Processing Pass':
          found = self.__moreInfoProcpass(command)
          break
    else:
      values = self.__bklist(self.getCurrentPath())
      found = False
      for i in values:
        if i['name'] == command:
          for j in i:
            if j not in ['fullpath', 'selection','expandable', 'method','level','name']:
              print '   ',j,i[j]
          found = True
        if i.has_key('level') and i['level'] == 'Processing Pass':
          found = self.__moreInfoProcpass(command)
          break

      if not found:
        print " The '%s' does not found"%(command)


  #############################################################################
  def help_setDataQualityFlags(self):
    print 'This command allows to use differnt data quality flags.'
    print 'For example:'
    print ' '.ljust(10)+'setDataQualityFlags OK UNCHECKED'

  #############################################################################
  def help_EOF(self):
    print "Quits the program"

  #############################################################################
  def do_EOF(self, line):
    sys.exit()

