########################################################################
# $Id$
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id$"

#############################################################################  
class ControlerLogInfo(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerLogInfo, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
    if message.action()=='showLog':
      file = message['fileName']
      self.getWidget().setUrlUsingStorage(file)
      res = self.getWidget().show()
    else:
      print 'Unknown message!',message.action(),message
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def close(self):
    #self.getWidget().hide()
    self.getWidget().close()
    
  #############################################################################  
