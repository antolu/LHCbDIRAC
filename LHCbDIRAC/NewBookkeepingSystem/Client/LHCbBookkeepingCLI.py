
import string, sys, cmd


from LHCbDIRAC.NewBookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient

#############################################################################
class LHCbBookkeepingCLI(cmd.Cmd):

  #############################################################################
  def __init__(self):
    cmd.Cmd.__init__(self)
    self.prompt = "$[/]$"
    self.__bk = LHCB_BKKDBClient()
    self.__currentPath = '/'

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
  def __oneLevelback(self):
    path = self.getCurrentPath().split('/')
    if path[0] == '' and path[1] == '':
      self.setCurrentPath('/')
      self.__printPrompt(self.getCurrentPath())
    else:
      newpath = ''
      for i in range(len(path) -1):
        if path[i] != '':
          newpath += '/' + path[i]
      if newpath == '': newpath = '/'
      self.setCurrentPath(newpath)
      self.__printPrompt(self.getCurrentPath())

  #############################################################################
  def help_ls(self):
    print "Usage: ls [OPTION]... [FILE]..."
    print "List information about the FILEs (the current directory by default)."
    print " Available options: -a"
    print 'Usage: ls or ls -a'
    print 'ls -a [FILE]'

  def help_cd(self):
    print " cd <dir>"
    print "cd .."
    print "cd /"
    print "cd"

  #############################################################################
  def do_ls(self, path):
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
        print i['name']
    else:
      res =  self.__bklist(path)
      for i in res:
        print i['name']


  #############################################################################
  def do_list(self, path):
    print 'Not implemented!!!'

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
  def help_EOF(self):
    print "Quits the program"

  #############################################################################
  def do_EOF(self, line):
    sys.exit()

