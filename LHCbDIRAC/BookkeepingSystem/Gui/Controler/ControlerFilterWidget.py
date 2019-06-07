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

"""
Controller of the Filter widget
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

from PyQt4.QtGui                                                         import QMessageBox
__RCSID__ = "$Id$"

#############################################################################
class ControlerFilterWidget(ControlerAbstract):
  """
  ControlerFilterWidget class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__model = None
    self.__list = []

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    if message.action() == 'list':
      self.__model = message['items']
      self.getWidget().setModel(self.__model)
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def textChanged(self):
    """handles the action generated by changing the text in the text box"""
    widget = self.getWidget()
    pattern = str(widget.getLineEdit().text())
    new_list = [item for item in self.__model if item.find(pattern) == 0]
    widget.getModel().setAllData(new_list)


  #############################################################################
  def okPressed(self):
    """handles the action of the ok button"""
    widget = self.getWidget()
    indexes = widget.getListView().selectedIndexes()
    selected = []
    if len(indexes) != 0:
      for i in indexes:
        item = str(i.data().toString())
        selected += [item]
      if len(selected) != 0:
        message = Message({'action':'applyFilter', 'items':selected})
        self.getParent().messageFromChild(self, message)
    else:
      QMessageBox.information(self.getWidget(), "More information...", "Please select TCKs", QMessageBox.Ok)

  #############################################################################
  def allPressed(self):
    """handless the all button acction"""
    flist = self.getWidget().getModel().getAllData()
    message = Message({'action':'applyFilter', 'items':flist})
    self.getParent().messageFromChild(self, message)

