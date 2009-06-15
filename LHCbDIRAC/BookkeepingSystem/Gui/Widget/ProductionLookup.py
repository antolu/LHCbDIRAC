########################################################################
# $Id: ProductionLookup.py,v 1.2 2009/06/15 12:50:26 zmathe Exp $
########################################################################
from PyQt4.QtGui                                                          import *
from PyQt4.QtCore                                                         import *
from DIRAC.BookkeepingSystem.Gui.Widget.ProductionLookup_ui               import Ui_Production
from DIRAC.BookkeepingSystem.Gui.Widget.ProductionListModel               import ProductionListModel
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerProductionLookup      import ControlerProductionLookup
import DIRAC,os

__RCSID__ = "$Id: ProductionLookup.py,v 1.2 2009/06/15 12:50:26 zmathe Exp $"

#############################################################################  
class ProductionLookup(QDialog, Ui_Production):
    
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
    
    
    self.listView.setSelectionMode(QAbstractItemView.ContiguousSelection)
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
  