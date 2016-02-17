"""
It controlls the bookmarks widget
"""
########################################################################


__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

from DIRAC                                                               import gLogger, S_OK, S_ERROR

#############################################################################
class ControlerAddBookmarks(ControlerAbstract):
  """class"""
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent from the parent"""
    if message.action() == 'showWidget':
      self.getWidget().show()
      return S_OK()
    elif message.action() == 'showValues':
      title = message['paths']['Title']
      path = message['paths']['Path']
      self.getWidget().setTitle(title)
      self.getWidget().setPath(path)
      self.getWidget().show()
      return S_OK()
    else:
      gLogger.error('UNKOWN Message!')
      return S_ERROR('UNKOWN Message!')

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the messages sent from its children"""
    gLogger.error('Unkown message')
    return S_ERROR('Unkown message')

  #############################################################################
  def okButton(self):
    """handles the action when the ok button pressed"""
    self.getWidget().waitCursor()
    title = self.getWidget().getTitle()
    path = self.getWidget().getPath()
    if len(path.split(':/')) > 0:
      message = Message({'action':'addBookmarks', 'bookmark':{'Title':title, 'Path':path}})
      feedback = self.getParent().messageFromChild(self, message)
      if not feedback['OK']:
        gLogger.error(feedback['Message'])
      else:
        self.getWidget().close()
    else:
      gLogger.error('Wrong path!')
    self.getWidget().arrowCursor()
  #############################################################################
  def cancelButton(self):
    """handles the action when the close button pressed"""
    self.getWidget().close()
