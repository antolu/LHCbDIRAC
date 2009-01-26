########################################################################
# $Id: ControlerInfoDialog.py,v 1.2 2009/01/26 17:38:00 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id: ControlerInfoDialog.py,v 1.2 2009/01/26 17:38:00 zmathe Exp $"

#############################################################################  
class ControlerInfoDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerInfoDialog, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
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
