# pylint: skip-file

"""
Controls the data quality widget
"""
########################################################################
# $Id: $
########################################################################

from PyQt4.QtCore                                                             import Qt

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

__RCSID__ = "$Id: $"

#############################################################################
class ControlerDataQualityDialog(ControlerAbstract):
  """
  ControlerDataQualityDialog class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    if message.action() == 'list':
      values = message['Values']
      self.getWidget().addDataQulity(values)
      self.getWidget().show()
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the message sent by the children controllers"""
    pass

  #############################################################################
  def close(self):
    """handles the action of the close button"""
    checkboxes = self.getWidget().getCheckBoses()
    values = {}
    for i in checkboxes:
      if i.checkState() == Qt.Checked:
        values[str(i.text())] = True
      elif i.checkState() == Qt.Unchecked:
        values[str(i.text())] = False
    message = Message({'action':'changeQualities', 'Values':values})
    self.getParent().messageFromChild(self, message)
    self.getWidget().close()

  #############################################################################

