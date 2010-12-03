########################################################################
# $Id$
########################################################################

from DIRAC                                                               import gLogger, S_OK, S_ERROR

from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.Command                      import Command
__RCSID__ = "$Id$"


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
  