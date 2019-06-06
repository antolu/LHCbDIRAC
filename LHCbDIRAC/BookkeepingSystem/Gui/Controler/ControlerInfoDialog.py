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
"""
Controller of the Info dialog window
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract    import ControlerAbstract
from DIRAC                                                          import gLogger

__RCSID__ = "$Id$"

#############################################################################
class ControlerInfoDialog(ControlerAbstract):
  """
  ControlerInfoDialog class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the actions sent by the parent controller"""
    gLogger.debug(message)
    if message.action() == 'list':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action() == 'showJobInfos':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action() == 'showAncestors':
      files = message['files']['Successful']
      res = self.getWidget().showDictionary(files)
      if res:
        self.getWidget().show()
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def close(self):
    """handles the close action of the window"""
    #self.getWidget().hide()
    self.getWidget().close()

  #############################################################################

