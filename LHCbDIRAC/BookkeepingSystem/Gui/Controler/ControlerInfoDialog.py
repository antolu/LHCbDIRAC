"""
Controller of the Info dialog window
"""
########################################################################
# $Id: ControlerInfoDialog.py 54002 2012-06-29 09:01:26Z zmathe $
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract    import ControlerAbstract
from DIRAC                                                          import gLogger

__RCSID__ = "$Id: ControlerInfoDialog.py 54002 2012-06-29 09:01:26Z zmathe $"

#############################################################################
class ControlerInfoDialog(ControlerAbstract):
  """
  ControlerInfoDialog class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the actions sent by the parent controller"""
    gLogger.debug(message)
    if message.action() == 'list':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action() == 'showJobInfos':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action() == 'showAncestors':
      files = message['files']['Successful']
      res = self.getWidget().showDictionary(files)
      if res:
        self.getWidget().show()
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def close(self):
    """handles the close action of the window"""
    #self.getWidget().hide()
    self.getWidget().close()

  #############################################################################

