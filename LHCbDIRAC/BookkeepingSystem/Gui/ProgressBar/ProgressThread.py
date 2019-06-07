###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
# pylint: skip-file

"""This module can be used as a progress bar."""

import time

from PyQt4.QtCore                                                                 import QThread, QString
from PyQt4.QtGui                                                                  import QProgressDialog

__RCSID__ = "$Id$"

class ProgressThread(QThread):
  """" ProgressThread class"""
  def __init__(self, stop, message='', parent=None):
    """The constructor initialize the QThread."""
    QThread.__init__(self, parent)
    self.__stoped = stop
    self.__message = message
      #gLogger.info('Constructor')


  def run(self):
    """Run a thread"""
    i = 0
    progressDialog = QProgressDialog(QString(), QString(), 0, 100)
    #progressDialog.setLabelText(self.__message)
    #progressDialog.setWindowTitle("Wait...")
    #progressDialog.setRange(0, 10000)
    #print 'Max',progressDialog.maximum()
    sleepingTime = 0
    #gLogger.info('Thread run')
    while (not self.__stoped):
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
    """Stop a thread"""
    #gLogger.info('Thread stoped')
    self.__stoped = True

