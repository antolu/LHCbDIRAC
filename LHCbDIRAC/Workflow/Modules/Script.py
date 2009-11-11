########################################################################
# $Id$
########################################################################
""" Script Class """

__RCSID__ = "$Id$"


from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC                           import S_OK, S_ERROR

import os,sys,re

class Script(object):

  def __init__(self):
    # constructor code
    self.Name = 'Name'
    self.Executable = ''
    self.LogFile = 'None'
    self.Output = 'None'

  def execute(self):
    # main execution function
    print '---------------------------------------------------------------'
    print 'Executable Name: %s' %(self.Name)
    cmd = self.Executable
    if re.search('.py$',self.Executable):
      cmd = '%s %s' %(sys.executable,self.Executable)
    if re.search('.sh$',self.Executable) or re.search('.csh$',self.Executable):
      cmd = './%s' %(self.Executable)
    print cmd
    outputDict = shellCall(0,cmd)
    print '---------------------------------------------------------------'
    if not outputDict['OK']:
      print 'ERROR: Shell call execution failed:'
      print outputDict['Message']
    resTuple = outputDict['Value']
    status = resTuple[0]
    stdout = resTuple[1]
    stderr = resTuple[2]
    print 'Execution completed with status %s' %(status)
    print stdout
    print stderr
    if os.path.exists(self.LogFile):
      print 'Removing existing %s' % self.LogFile
      os.remove(self.LogFile)
    fopen = open(self.LogFile,'w')
    fopen.write('<<<<<<<<<< Standard Output >>>>>>>>>>\n\n%s ' % stdout)
    if stderr:
      fopen.write('<<<<<<<<<< Standard Error: >>>>>>>>>>\n\n%s ' % stderr)
    fopen.close()
    print 'Output written to %s' % (self.LogFile)
    self.Output = '%s\n%s' %(stdout,stderr)
    return S_OK()


