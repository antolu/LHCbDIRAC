"""
Controller of the Log widget
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id$"

#############################################################################
class ControlerLogInfo(ControlerAbstract):
  """
  ControlerLogInfo class
  """
  #############################################################################
  def __init__(self, widget, parent):
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    if message.action() == 'showLog':
      fileName = message['fileName']
      self.getWidget().setUrlUsingStorage(fileName)
      self.getWidget().show()
    else:
      print 'Unknown message!', message.action(), message

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def close(self):
    """handle the close action"""
    #self.getWidget().hide()
    self.getWidget().close()

  #############################################################################

