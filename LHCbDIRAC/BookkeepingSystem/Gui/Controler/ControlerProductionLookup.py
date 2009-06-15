########################################################################
# $Id: ControlerProductionLookup.py,v 1.3 2009/06/15 12:50:26 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from PyQt4.QtGui                                                     import *

__RCSID__ = "$Id: ControlerProductionLookup.py,v 1.3 2009/06/15 12:50:26 zmathe Exp $"

#############################################################################  
class ControlerProductionLookup(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerProductionLookup, self).__init__(widget, parent)
    self.__model = None
    self.__list = []
  
  #############################################################################  
  def messageFromParent(self, message):
    if message.action()=='list':
      self.__list = []
      self.__model = message['items']
      widget = self.getWidget()
      keys = self.__model.getChildren().keys()
      keys.sort()
      for i in keys:
        self.__list += [str(abs(int(i)))]
      widget.setModel(self.__list)
      widget.show()
    else:
      print 'Unknown message!',message.action()
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def close(self):
    widget = self.getWidget()
    indexes = widget.getListView().selectedIndexes()
    selected = []
    if len(indexes) != 0:
      for i in indexes:
        item = i.data().toString()
        if str(item) in self.__model.getChildren().keys():
          selected += [self.__model.getChildren()[str(item)]]
        else:
          i = -1 * int(item)
          if str(i) in self.__model.getChildren().keys():
            selected += [self.__model.getChildren()[str(i)]]
      if len(selected) != 0:
        message = Message({'action':'showOneProduction','paths':selected})
        self.getParent().messageFromChild(self, message)
    else:
      QMessageBox.information(self.getWidget(), "More information...", "Please select a production!",QMessageBox.Ok)
    widget.close()
   
  #############################################################################  
  def cancel(self): 
    self.getWidget().close()
  
  def all(self):
    message = Message({'action':'showAllProduction','items':self.__model})
    self.getParent().messageFromChild(self, message)
    self.getWidget().close()
    
  #############################################################################  
  def textChanged(self):
    widget = self.getWidget()
    pattern = str(widget.getLineEdit().text())
    new_list = [item for item in self.__list if item.find(pattern) == 0]
    widget.getModel().setAllData(new_list)