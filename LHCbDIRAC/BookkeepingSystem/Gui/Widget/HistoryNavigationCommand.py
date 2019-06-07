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
history navigation command
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Command                      import Command
__RCSID__ = "$Id$"


########################################################################
class HistoryNavigationCommand(Command):
  """
  HistoryNavigationCommand class
  """
  ########################################################################
  def __init__(self, widget, tableView, tableModel):
    """iniztialize the concrete command"""
    Command.__init__(self)
    self.__tableView = tableView
    self.__historyWidget = widget
    self.__tableModel = tableModel

  ########################################################################
  def execute(self):
    """executes the command"""
    self.__historyWidget.setTableModel(self.__tableView, self.__tableModel)
    self.__historyWidget.show()
    self.__historyWidget.repaint()
    self.__tableView.repaint()
