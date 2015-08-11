# pylint: skip-file

"""
Table widget used by most widgets
"""
########################################################################
# $Id$
########################################################################


from PyQt4.QtGui   import QWidget

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_TableWidget     import Ui_TableWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerTable  import ControlerTable

__RCSID__ = "$Id$"

#############################################################################
class TableWidget(QWidget, Ui_TableWidget):
  """
  TableWidget class
  """
  def __init__(self, parent=None):
    """
    Constructor

    @param parent parent widget (QWidget)
    """
    QWidget.__init__(self, parent)
    Ui_TableWidget.__init__(self)
    self.setupUi(self)
    self.__controler = ControlerTable(self, parent.parentWidget().getControler())

  #############################################################################
  def clear(self):
    """clear the table"""
    self.tableWidget.clear()

  #############################################################################
  def setColumnCount(self, number):
    """sets the number of columns"""
    self.tableWidget.setColumnCount(number)

  #############################################################################
  def setRowCount(self, row):
    """sets the number of rows"""
    self.tableWidget.setRowCount(row)

  #############################################################################
  def setupControler(self, controler):
    """sets the controller"""
    pass

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################

