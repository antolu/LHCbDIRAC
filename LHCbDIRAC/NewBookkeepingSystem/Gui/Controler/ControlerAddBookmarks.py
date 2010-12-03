########################################################################
# $HeadURL:  $
########################################################################


__RCSID__ = "$Id: $"

from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Message                       import Message

from DIRAC                                                           import gLogger, S_OK, S_ERROR

import sys

#############################################################################  
class ControlerAddBookmarks(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerAddBookmarks, self).__init__(widget, parent)
            
  #############################################################################  
  def messageFromParent(self, message):
    if message.action()=='showWidget':
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
    gLogger.error('Unkown message')
    return S_ERROR('Unkown message')
  
  #############################################################################
  def okButton(self):
    title = self.getWidget().getTitle()
    path = self.getWidget().getPath()
    if len(path.split(':/')) > 0:
      message = Message({'action':'addBookmarks','bookmark':{'Title':title,'Path':path}})
      feedback = self.getParent().messageFromChild(self, message)
      if not feedback['OK']:
        gLogger.error(feedback['Message'])
      else:
        self.getWidget().close()
    else:
      gLogger.error('Wrong path!')
  
  #############################################################################
  def cancelButton(self):
    self.getWidget().close()