########################################################################
# $Id: ProgressThread.py,v 1.4 2008/10/18 18:36:52 zmathe Exp $
########################################################################

from PyQt4.QtCore                                                                 import *
from PyQt4.QtGui                                                                  import *
#from DIRAC                                                                        import gLogger, S_OK, S_ERROR
import time

__RCSID__ = "$Id: ProgressThread.py,v 1.4 2008/10/18 18:36:52 zmathe Exp $"

class ProgressThread(QThread):
  def __init__(self, stop, message='', parent=None):
        QThread.__init__(self, parent)
        self.__stoped = stop
        self.__message = message
        #gLogger.info('Constructor')
                  

  def run(self):
    i = 0
    progressDialog = QProgressDialog(QString(), QString(),0,100)
    #progressDialog.setLabelText(self.__message)
    #progressDialog.setWindowTitle("Wait...")
    #progressDialog.setRange(0, 10000)
    #print 'Max',progressDialog.maximum()
    sleepingTime = 0
    #gLogger.info('Thread run')
    while ( not self.__stoped):
      i = i + 1
      if i == progressDialog.maximum():
        sleepingTime = 1
        i = 0
      #progressDialog.setLabelText(self.tr(self.__message))
      progressDialog.setValue(i)
      #QCoreApplication.processEvents()
      #qApp.processEvents()
      time.sleep(sleepingTime)
    self.__stoped = False
    #gLogger.info('Thread run end')
    
  def stop(self):
    #gLogger.info('Thread stoped')
    self.__stoped = True

