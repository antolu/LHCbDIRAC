"""Bookkeeping file system"""
import sys, cmd, pydoc

__RCSID__ = ""

from LHCbDIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient

#############################################################################
class LHCbBookkeepingCLI(cmd.Cmd):
  """class"""
  #############################################################################
  def __init__(self):
    """constructor"""
    cmd.Cmd.__init__(self)
    self.prompt = "$[/]$"
    self.__bk = LHCB_BKKDBClient()
    self.__currentPath = '/'
    self.do_setDataQualityFlags('OK')

  #############################################################################
  def setCurrentPath(self, path):
    """current path(node)"""
    self.__currentPath = path

  #############################################################################
  def addCurrentPath(self, path):
    """add a path"""
    if path[0] != '/' and len(self.__currentPath) == 1:
      self.__currentPath += path
    else:
      self.__currentPath += '/' + path

  #############################################################################
  def getCurrentPath(self):
    """get current path"""
    return self.__currentPath

  #############################################################################
  def __printPrompt(self,  path = '/'):
    """prints the prompt"""
    self.prompt = '$[' + path + ']$'

  #############################################################################
  def __bklist(self, path):
    """list a path"""
    retVal =  self.__bk.list(path)
    return retVal

  def __bkListAll(self, path):
    """???"""
    if path == '':
      path = self.getCurrentPath()
    res = self.__bk.list(path)
    for i in res:
      print i['name']
      for j in i:
        if j not in ['fullpath', 'selection', 'expandable', 'method', 'level', 'name']:
          print '   ', j, i[j]

  #############################################################################
  def __checkDirectory(self, path):
    """is empty directory"""
    res = self.__bk.list(path)
    retValue = False
    if len(res) > 0:
      retValue = True
    return retValue

  #############################################################################
  def __rootDirectory(self):
    """root"""
    self.setCurrentPath('/')
    self.__printPrompt(self.getCurrentPath())

  #############################################################################
  def __oneLevelback(self, logical = False):
    """ .. """
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
      if newpath == '':
        newpath = '/'
      if not logical:
        self.setCurrentPath(newpath)
        self.__printPrompt(self.getCurrentPath())
      else:
        return newpath

  #############################################################################
  @staticmethod
  def help_ls():
    """provides help"""
    print "Usage: ls [OPTION]... [FILE]..."
    print "List information about the FILEs (the current directory by default)."
    print " Available options: -a"
    print 'Usage: ls or ls -a'
    print 'ls -a [FILE]'
    print 'Note: You can do paging using the | more'
    print 'For example: ls | more'

  @staticmethod
  def help_cd():
    """help cd command"""
    print " cd <dir>"
    print "cd .."
    print "cd /"
    print "cd"

  #############################################################################
  def do_ls(self, path):
    """ls command"""
    paging = False
    if path.find('|') > -1:
      tmpPath = path.split('|')
      path = ''
      for i in range(len(tmpPath) - 1):
        path += tmpPath[i].strip()
      paging = True

    text = ''
    if len(path) > 0 and path[0] == '-':
      if path[1] != 'a':
        print "ls: invalid option -- %s" % (path[1])
        print "Try `help ls' for more information."
      else:
        path = path[2:]
        self.__bkListAll(path)
    elif path == '':
      res = self.__bklist(self.getCurrentPath())
      for i in res:
        if paging:
          text += i['name'] + '\n'
        else:
          print i['name']
    else:
      res = self.__bklist(path)
      for i in res:
        if paging:
          text += i['name'] + '\n'
        else:
          print i['name']
    if paging:
      pydoc.ttypager(text)

  #############################################################################
  def do_list(self, path):
    """list commamd"""
    pass

  #############################################################################
  def do_save(self, command):
    """save command"""
    print command
    parameters = command.split(' ')
    filename = parameters[0]
    saveType = parameters[1]
    files = self.__bk.list(self.getCurrentPath())
    lfns = {}
    for i in range(len(files)):
      lfns[files[i]['FileName']] = files[i]

    if saveType == 'txt':
      text = self.__bk.writeJobOptions(lfns, '', saveType)
      fileName = open(filename, 'w')
      fileName.write(text)
      fileName.close()
    elif saveType == 'py':
      text = self.__bk.writeJobOptions(lfns, filename, saveType)
      fileName = open(filename, 'w')
      fileName.write(text)
      fileName.close()

  #############################################################################
  def do_cd(self, path):
    """cd command"""
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
    """pwd command"""
    print self.getCurrentPath()
   
  #############################################################################
  def do_queries(self, command=''):
    """execute query"""
    retVal = self.__bk.getPossibleParameters()
    print 'The following bookkeeping query types are available:'
    for i in retVal:
      print ' '.rjust(10)+i
    print "You can change the query types using the 'use' command"

  #############################################################################
  def do_use(self, command):
    """use command"""
    self.__bk.setParameter(str(command))
    self.do_cd('/')

  #############################################################################
  @staticmethod
  def help_use():
    """hel of use command"""
    print 'Usage:'
    print '  use type'.rjust(10)
    print 'Arguments:'
    print ' type: bookkeeping query type'.rjust(10)
    print "The 'type' can be found using the 'queries' command!"
    print "EXAMPE:"
    print ' '.rjust(10)+"use 'Event type'"

  #############################################################################
  @staticmethod
  def help_queries():
    """help of queries command"""
    print "This method shows the available query types!"
    print "Usage:"
    print "  queries"
    print "You can choose a query type using the 'use' command  "
    print " NOTE: the default query type is 'Configuration'"

  #############################################################################
  def do_advanceQuery(self, command = ''):
    """advancedQuery command"""
    self.__bk.setAdvancedQueries(True)
    self.do_cd('/')

  #############################################################################
  @staticmethod
  def help_advanceQuery():
    """help"""
    print "It allows to see more level of the Bookkeeping Tree"
    print "Usage:"
    print "   advanceQuery"
    
  #############################################################################
  def do_standardQuery(self, command = ''):
    """commmand"""
    self.__bk.setAdvancedQueries(False)
    self.do_cd('/')

  #############################################################################
  @staticmethod
  def help_standardQuery():
    """help"""
    print "This is used by default"
    print "It shows a reduced bookkeeping path."
    print "Usage:"
    print "   standardQuery"

  #############################################################################
  def do_dataQuality(self, command=''):
    """command"""
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
  @staticmethod
  def help_dataQuality():
    """help"""
    print 'This command shows the available data quality flags.'
    print "Usage:"
    print "  dataQuality"
    print 'To change the data quality flag use the setDataQualityFlags command'

  #############################################################################
  def do_setDataQualityFlags(self, command):
    """command"""
    qualities = command.split(' ')
    if len(qualities) > 0:
      dataquality = {}
      for i in qualities:
        dataquality[i] = True
      self.__bk.setDataQualities(dataquality)
    else:
      print 'ERROR: Please give a data quality flag!'

  #############################################################################
  def __moreInfoProcpass(self, command):
    """more information of a directory"""
    found = False
    retVal = self.__bk.getProcessingPassSteps({'StepName':command})
    if retVal['OK']:
      proc = retVal['Value']
      print '%d %s step founf in the bkk' % (proc['TotalRecords'], command)
      for i in proc['Records']:
        print ' '.ljust(5) + i
        for j in proc['Records'][i]:
          print ' '.ljust(10) + str(j[0]) + ':' + str(j[1])
        found = True
    else:
      print 'ERROR: ', retVal['Message']
    return found

  #############################################################################
  def do_moreinfo(self, command=''):
    """more info command"""
    if command == '':
      previouspath = self.__oneLevelback(self.getCurrentPath())
      values = self.__bklist(previouspath)
      name = self.getCurrentPath().split('/')
      for i in values:
        if i.has_key('level') and i['level'] == 'FileTypes':
          path = self.getCurrentPath()
          retVal = self.__bk.getLimitedFiles({'fullpath':str(path)}, ['nb'], -1, -1)
          print 'The selected dataset is:'
          for i in retVal['Extras']['Selection']:
            print ''.ljust(5) + i + ' ' + str(retVal['Extras']['Selection'][i])
          print 'Statistics:'
          print ' '.ljust(5) + 'Number of files:' + str(retVal['TotalRecords'])
          for i in retVal['Extras']['GlobalStatistics']:
            print ''.ljust(5) + i + ' ' + str(retVal['Extras']['GlobalStatistics'][i])
          break
        if i['name'] == name[len(name) - 1]:
          for j in i:
            if j not in ['fullpath', 'selection', 'expandable', 'method', 'level', 'name']:
              print '   ', j, i[j]
        if i.has_key('level') and i['level'] == 'Processing Pass':
          found = self.__moreInfoProcpass(command)
          break
    else:
      values = self.__bklist(self.getCurrentPath())
      found = False
      for i in values:
        if i['name'] == command:
          for j in i:
            if j not in ['fullpath', 'selection', 'expandable', 'method', 'level', 'name']:
              print '   ', j, i[j]
          found = True
        if i.has_key('level') and i['level'] == 'Processing Pass':
          found = self.__moreInfoProcpass(command)
          break

      if not found:
        print " The '%s' does not found" % (command)

  #############################################################################
  @staticmethod
  def help_setDataQualityFlags():
    """help"""
    print 'This command allows to use differnt data quality flags.'
    print "Usage:"
    print "  setDataQualityFlags flag1 [flag2 flag3, ... flagN]"
    print "Arguments:"
    print "  flag[1...N]:  Data qulaity flags."
    print 'For example:'
    print ' '.ljust(10)+'setDataQualityFlags OK UNCHECKED'

  #############################################################################
  @staticmethod
  def help_EOF():
    """quit"""
    print "Quits the program"

  #############################################################################
  def do_EOF(self, line):
    """quit command"""
    sys.exit()

  
  #############################################################################
  @staticmethod
  def help_save():
    """help"""
    print 'This command is used to save the dataset.'
    print 'Usage:' 
    print '  save fileName type'
    print 'Arguments:'
    print '  fileName: is a text for example: ex.txt'
    print '  type: txt or py '
    print 'For example:'
    print ' '.ljust(10)+'save files.dat txt'
    
  #############################################################################
  @staticmethod
  def help_moreinfo():
    """help method"""
    print "Display the statistics of the selected data."
    print "Usage:"
    print "  moreinfo"
  
  
