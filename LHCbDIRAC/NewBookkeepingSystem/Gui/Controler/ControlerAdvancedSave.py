########################################################################
# $Id$
########################################################################


from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Message                       import Message
from PyQt4.QtGui                                                         import *

__RCSID__ = "$Id$"

#############################################################################
class ControlerAdvancedSave(ControlerAbstract):

  #############################################################################
  def __init__(self, widget, parent):
    super(ControlerAdvancedSave, self).__init__(widget, parent)
    self.__sites = {'Select a site':None,"CERN":"LCG.CERN.ch", "RAL":"LCG.RAL.uk", "IN2P3":"LCG.IN2P3.fr", "GRIDKA":"LCG.GRIDKA.de", "NIKHEF":"LCG.NIKHEF.nl", "CNAF":"LCG.CNAF.it", "PIC":"LCG.PIC.es"}

  #############################################################################
  def messageFromParent(self, message):
    if message.action()=='showWidget':
      widget = self.getWidget()
      widget.fillWindows(self.__sites)
      widget.show()
    else:
      print 'Unknown messageaa!',message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def lfnButtonChanged(self):
    widget = self.getWidget()
    widget.setLFNbutton()

  #############################################################################
  def pfnButtonChanged(self):
    widget = self.getWidget()
    widget.setPFNbutton()

  #############################################################################
  def saveButton(self):
    widget = self.getWidget()
    filename = str(widget.getLineEdit().text())
    if filename == '':
      QMessageBox.information(self.getWidget(), "Error...", "File name is missing!",QMessageBox.Ok)
    else:
      site = self.__sites[str(widget.getSite())]
      if site == None:
        QMessageBox.information(widget, "Error",'Please select a site!', QMessageBox.Ok)
        return
      site = self.__sites[str(widget.getSite())]
      infos = {'Site':site,'pfn':widget.isPFNbuttonChecked(),'lfn':widget.isLFNbuttonChecked(),'FileName':filename}
      message = Message({'action':'advancedSave','selection':infos})
      self.getWidget().waitCursor()
      self.getParent().messageFromChild(self, message)
      self.getWidget().arrowCursor()
      widget.close()