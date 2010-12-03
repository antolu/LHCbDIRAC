########################################################################
# $Id: HistoryNavigationCommand.py 18175 2009-11-11 14:02:19Z zmathe $
########################################################################

from DIRAC                                                               import gLogger, S_OK, S_ERROR

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Command                      import Command
__RCSID__ = "$Id: HistoryNavigationCommand.py 18175 2009-11-11 14:02:19Z zmathe $"


########################################################################
class HistoryNavigationCommand(Command):
  
  ########################################################################
  def __init__(self, widget, tableView, tableModel):
    Command.__init__(self)
    self.__tableView = tableView
    self.__historyWidget = widget
    self.__tableModel = tableModel
  
  ########################################################################
  def execute(self):
    self.__historyWidget.setTableModel(self.__tableView, self.__tableModel)
    self.__historyWidget.show()
    self.__historyWidget.repaint()
    self.__tableView.repaint()
  