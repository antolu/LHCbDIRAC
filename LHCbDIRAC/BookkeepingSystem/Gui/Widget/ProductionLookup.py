# pylint: skip-file

"""
Production Lookup
"""
########################################################################
# $Id: ProductionLookup.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################
from PyQt4.QtGui                                                              import QDialog, QAbstractItemView
from PyQt4.QtCore                                                             import SIGNAL
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_ProductionLookup               import Ui_ProductionLookup
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.ProductionListModel               import ProductionListModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerProductionLookup      import ControlerProductionLookup

__RCSID__ = "$Id$"

#############################################################################
class ProductionLookup(QDialog, Ui_ProductionLookup):
  """
  ProductionLookup class
  """
  #############################################################################
  def __init__(self, data = None, parent = None):
    QDialog.__init__(self, parent)
    Ui_ProductionLookup.__init__(self)
    self.setupUi(self)
    self.__model = ProductionListModel(data, self)

    self.__controler = ControlerProductionLookup(self, parent.getControler())
    self.connect(self.pushButton, SIGNAL("clicked()"), self.__controler.close)
    self.connect(self.pushButton_2, SIGNAL("clicked()"), self.__controler.cancel)

    self.connect(self.lineEdit, SIGNAL("textChanged(QString)"),
                     self.__controler.textChanged)

    self.connect(self.allButton, SIGNAL("clicked()"), self.__controler.all)

    self.listView.setSelectionMode(QAbstractItemView.ExtendedSelection)
    self.listView.setSelectionBehavior(QAbstractItemView.SelectRows)

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def closeEvent(self, event):
    """ it close the window and change the query type"""
    event.accept()
    self.__controler.cancel()

  #############################################################################
  def setModel(self, data):
    """sets the model"""
    self.__model.setData(data)
    self.listView.setModel(self.__model)

  #############################################################################
  def getListView(self):
    """returns the list view"""
    return self.listView

  #############################################################################
  def getLineEdit(self):
    """returns the lineedit widget"""
    return self.lineEdit

  #############################################################################
  def getModel(self):
    """returns the model"""
    return self.__model
