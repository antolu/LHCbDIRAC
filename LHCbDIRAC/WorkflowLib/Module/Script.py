
from DIRAC.Core.Utilities.Subprocess import shellCall

import sys,re

class Script(object):

  def __init__(self):
    # constructor code
    self.Result = 'None'
    self.Name = ''
    self.Executable = ''

  def execute(self):
    # main execution function
    print 'Executing Module = ',str(type(self))
    print 'Executable is: ',self.Name
    print 'The following is to be executed: ',self.Executable

    cmd = self.Executable
    if re.search('.py$',self.Executable):
      cmd = sys.executable+' '+self.Executable
    self.Result = shellCall(0,cmd)
    print "OUTPUT is",self.Result