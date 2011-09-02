########################################################################
# $Id$
########################################################################
from PyQt4.QtGui                                                              import *
from PyQt4.QtCore                                                             import *
from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.Ui_ProductionLookup               import Ui_ProductionLookup
from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.ProductionListModel               import ProductionListModel
from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerProductionLookup      import ControlerProductionLookup

__RCSID__ = "$Id$"

#############################################################################
class ProductionLookup(QDialog, Ui_ProductionLookup):

  #############################################################################
  def __init__(self, data = None, parent = None):
    QDialog.__init__(self, parent)
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
