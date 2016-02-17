"""
history navigation command
"""
########################################################################
# $Id: HistoryNavigationCommand.py 54014 2012-06-29 14:05:43Z zmathe $
########################################################################

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
