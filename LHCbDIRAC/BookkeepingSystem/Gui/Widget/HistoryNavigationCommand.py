########################################################################
# $Id: HistoryNavigationCommand.py,v 1.1 2009/10/22 20:38:03 zmathe Exp $
########################################################################

from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Gui.Widget.Command                      import Command
__RCSID__ = "$Id: HistoryNavigationCommand.py,v 1.1 2009/10/22 20:38:03 zmathe Exp $"


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
  