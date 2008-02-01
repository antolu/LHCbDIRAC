import os,string

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from WorkflowLib.Module.LHCbCheckLogFile import LHCbCheckLogFile
from WorkflowLib.Module.CheckLogFile import CheckLogFile
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig

class BooleCheckLogFile(LHCbCheckLogFile):

  def __init__(self):
    self.argv0     = 'BooleCheckLogFile'
    self.log = gLogger.getSubLogger("BooleCheckLogFile")
    self.iClient   = None
    self.info      = 1

  def checkApplicationLog(self,error):
    self.log.info(self.argv0 + '.BooleCheckLogFile: analyze %s '%(self.logfile))

    mailto = self.appName.upper()+'_EMAIL'

    last_line = self.grep(self.logfile,'dum','-l')
    if string.find(last_line,'User defined signal') != -1:
      mailto = 'DIRAC_EMAIL'
      result = S_ERROR(mailto + ' USER signal')
    else:
      result = S_ERROR(error)

    if result['Message'].find('EMAIL') != -1:
      self.sendErrorMail(result['Message'])

    return result
#
#------------------------------------------------------------------------------------------
#
if __name__ == '__main__':

   Chk = BooleCheckLogFile()

   rc = Chk.execute()

   print 'main-Boole - rc= ',rc


