########################################################################
# $Id$
########################################################################
from PyQt4.QtGui                                                              import *
from PyQt4.QtCore                                                             import *

from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.ProcessingPassDialog_ui           import Ui_ProcessingPassDialog
from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.TableModel                        import TableModel
from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.TabWidget                         import TabWidget
from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerProcessingPassDialog  import ControlerProcessingPassDialog
import LHCbDIRAC,os

__RCSID__ = "$Id$"

#############################################################################  
class ProcessingPassDialog(QDialog,Ui_ProcessingPassDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerProcessingPassDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)
    
    picturesPath = os.path.dirname(os.path.realpath(LHCbDIRAC.__path__[0]))+'/LHCbDIRAC/NewBookkeepingSystem/Gui/Widget'
    closeIcon = QIcon(picturesPath+"/images/close.png")
    self.closeButton.setIcon(closeIcon)
        
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def getTabWidget(self):
    return self.tabwidget
  
  #############################################################################  
  def createEmptyTabWidget(self, name):
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
  def createTabWidget(self, userObject):
    tab = TabWidget(userObject)
    tab.setObjectName("tab")
    return tab
    #tab.createTable
    
    '''      
    www = self.createEmptyTabWidget('Proba')
    harom = TabWidget(['sasa'])
    harom.setObjectName("harom")
    harom.createTable(['asasa','sasa','sas'],[['01','01','02'], 
            ['10','11','12'], 
            ['20','21','22']])
    www.addTab(harom,"Harom")
    
    negy = TabWidget(['sasa'])
    negy.setObjectName("harom")
    negy.createTable(['asasa','sasa','sas'],[['01','01','02'], 
            ['10','11','12'], 
            ['20','21','22']])
    self.tabwidget.addTab(negy,"negy")
    '''
  #############################################################################  
  def setTotalProccesingPass(self, text):
    self.lineEdit.setText(text)