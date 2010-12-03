########################################################################
# $Id: ControlerInfoDialog.py 18175 2009-11-11 14:02:19Z zmathe $
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract
from DIRAC                                                       import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: ControlerInfoDialog.py 18175 2009-11-11 14:02:19Z zmathe $"

#############################################################################  
class ControlerInfoDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerInfoDialog, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
    gLogger.debug(message)
    if message.action()=='list':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action()=='showJobInfos':
      res = self.getWidget().showData(message['items'])
      if res:
        self.getWidget().show()
    elif message.action()=='showAncestors':
      files = message['files']['Successful']
      res = self.getWidget().showDictionary(files)
      if res:
        self.getWidget().show()
    else:
      print 'Unknown message!',message.action()
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def close(self):
    #self.getWidget().hide()
    self.getWidget().close()
    
  #############################################################################  
