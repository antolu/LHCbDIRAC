# pylint: skip-file

"""
Processing pass widget
"""
########################################################################
# $Id: ProcessingPassDialog.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################
from PyQt4.QtGui                                                              import QDialog, QHBoxLayout, QTabWidget
from PyQt4.QtCore                                                             import SIGNAL

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_ProcessingPassDialog           import Ui_ProcessingPassDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TabWidget                         import TabWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerProcessingPassDialog  import ControlerProcessingPassDialog

__RCSID__ = "$Id: ProcessingPassDialog.py 84842 2015-08-11 13:47:15Z fstagni $"

#############################################################################
class ProcessingPassDialog(QDialog, Ui_ProcessingPassDialog):
  """
  ProcessingPassDialog
  """
  #############################################################################
  def __init__(self, parent=None):
    QDialog.__init__(self, parent)
    Ui_ProcessingPassDialog.__init__(self)
    self.setupUi(self)
    self.__controler = ControlerProcessingPassDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def getTabWidget(self):
    """returns the widget"""
    return self.tabwidget

  #############################################################################
  def createEmptyTabWidget(self, name):
    """crates a dumy tab widget"""
    tab = TabWidget(['sasa'])
    tab.setObjectName("tab")

    hboxlayout = QHBoxLayout(tab)
    hboxlayout.setObjectName("hboxlayout")


    tabWidget = QTabWidget(tab)
    tabWidget.setObjectName("tabWidget")
    hboxlayout.addWidget(tabWidget)
    self.getTabWidget().addTab(tab, name)
    return tabWidget

  #############################################################################
  @staticmethod
  def createTabWidget(userObject):
    """create a tab widget"""
    tab = TabWidget(userObject)
    tab.setObjectName("tab")
    return tab
    #tab.createTable


#    www = self.createEmptyTabWidget('Proba')
#    harom = TabWidget(['sasa'])
#    harom.setObjectName("harom")
#    harom.createTable(['asasa','sasa','sas'],[['01','01','02'],
#            ['10','11','12'],
#            ['20','21','22']])
#    www.addTab(harom,"Harom")
#
#    negy = TabWidget(['sasa'])
#    negy.setObjectName("harom")
#    negy.createTable(['asasa','sasa','sas'],[['01','01','02'],
#            ['10','11','12'],
#            ['20','21','22']])
#    self.tabwidget.addTab(negy,"negy")

  #############################################################################
  def setTotalProccesingPass(self, text):
    """sets the total processing pass"""
    self.lineEdit.setText(text)
