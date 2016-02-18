# pylint: skip-file

"""
Filter widget
"""

from PyQt4.QtGui                                                              import QWidget, QAbstractItemView
from PyQt4.QtCore                                                             import SIGNAL
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_FilterWidget                import Ui_FilterWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FilterListModel                import FilterListModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerFilterWidget       import ControlerFilterWidget


__RCSID__ = "$Id$"

#############################################################################
class FilterWidget(QWidget, Ui_FilterWidget):
  """
  FilterWidget class
  """
  #############################################################################
  def __init__(self, parent=None):
    QWidget.__init__(self, parent)
    Ui_FilterWidget.__init__(self)
    self.setupUi(self)
    self.__model = FilterListModel(self)
    self.listView.setSelectionMode(QAbstractItemView.ExtendedSelection)
    self.listView.setSelectionBehavior(QAbstractItemView.SelectRows)
    self.__controler = None

  def setupControler(self, parent):
    """initialize the controllers"""
    self.__controler = ControlerFilterWidget(self, parent.getControler())
    self.connect(self.okButton, SIGNAL("clicked()"), self.__controler.okPressed)
    self.connect(self.allButton, SIGNAL("clicked()"), self.__controler.allPressed)

    self.connect(self.lineEdit, SIGNAL("textChanged(QString)"),
                     self.__controler.textChanged)


  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def setModel(self, data):
    """sets the model of the widget"""
    self.__model.setData(data)
    self.listView.setModel(self.__model)

  #############################################################################
  def getListView(self):
    """returns the list"""
    return self.listView

  #############################################################################
  def getLineEdit(self):
    """returns the text box"""
    return self.lineEdit

  #############################################################################
  def getModel(self):
    """returns the model"""
    return self.__model

