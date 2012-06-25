########################################################################
# $Id: $
########################################################################
from PyQt4.QtGui                                                              import QWidget,QAbstractItemView
from PyQt4.QtCore                                                             import SIGNAL
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_FilterWidget                import Ui_FilterWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FilterListModel                import FilterListModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerFilterWidget       import ControlerFilterWidget


__RCSID__ = "$Id: $"

#############################################################################
class FilterWidget(QWidget, Ui_FilterWidget):

  #############################################################################
  def __init__(self, parent=None):
    QWidget.__init__(self, parent)
    self.setupUi(self)
    self.__model = FilterListModel(self)
    self.listView.setSelectionMode(QAbstractItemView.ExtendedSelection)
    self.listView.setSelectionBehavior(QAbstractItemView.SelectRows)

  def setupControler(self, parent):
    self.__controler = ControlerFilterWidget(self, parent.getControler())
    self.connect(self.okButton, SIGNAL("clicked()"), self.__controler.okPressed)
    self.connect(self.allButton, SIGNAL("clicked()"), self.__controler.allPressed)

    self.connect(self.lineEdit, SIGNAL("textChanged(QString)"),
                     self.__controler.textChanged)


  #############################################################################
  def getControler(self):
    return self.__controler

  #############################################################################
  def setModel(self, data):
    self.__model.setData(data)
    self.listView.setModel(self.__model)

  #############################################################################
  def getListView(self):
    return self.listView

  #############################################################################
  def getLineEdit(self):
    return self.lineEdit

  #############################################################################
  def getModel(self):
    return self.__model

