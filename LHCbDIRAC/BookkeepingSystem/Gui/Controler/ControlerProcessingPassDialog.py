########################################################################
# $Id: ControlerProcessingPassDialog.py,v 1.1 2009/03/20 17:13:56 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
__RCSID__ = "$Id: ControlerProcessingPassDialog.py,v 1.1 2009/03/20 17:13:56 zmathe Exp $"

#############################################################################  
class ControlerProcessingPassDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerProcessingPassDialog, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
    if message.action()=='list':
      item = message['items']
      message = Message({'action':'procDescription','groupdesc':item['name'],'passid':item['passid']})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback != None:
        widget = self.getWidget()
        widget.setTotalProccesingPass(item['name'])
        tabwidget = widget.getTabWidget()
        tabwidget.clear()# cleaning, I have to delete the existing tabs
        records = feedback['Records'] 
        parameters = feedback['Parameters']
        tabs = {}
        for i in records:
          tab = widget.createTabWidget(records[i])
          tab.createTable(parameters, records[i])
          tabs[i] = tab
        
        mainWidget = {}
        for i in tabs:
          tab = tabs[i]
          desc = tab.getGroupDesc()
          if mainWidget.has_key(desc):
            main = mainWidget[desc]
            main.addTab(tab,i)
          else:
            main = widget.createEmptyTabWidget(desc)
            main.addTab(tab,i)
            mainWidget[desc]=main
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
