########################################################################
# $Id: $
########################################################################

from PyQt4.QtGui                                                              import *
from PyQt4.QtCore                                                             import *

from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Message                       import Message

__RCSID__ = "$Id: $"

#############################################################################  
class ControlerDataQualityDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerDataQualityDialog, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
    if message.action() == 'list':
      values = message['Values']
      self.getWidget().addDataQulity(values)
      self.getWidget().show()
    else:
      print 'Unknown message!',message.action()
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def close(self):
    checkboxes = self.getWidget().getCheckBoses()
    values = {}
    for i in checkboxes:
      if i.checkState() == Qt.Checked:
        values[str(i.text())] = True
      elif i.checkState() == Qt.Unchecked:
        values[str(i.text())] = False 
    message = Message({'action':'changeQualities','Values':values})
    self.getParent().messageFromChild(self, message)
    self.getWidget().close()
    
  #############################################################################  
