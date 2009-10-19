########################################################################
# $Id: ControlerProductionLookup.py,v 1.5 2009/10/19 11:17:39 zmathe Exp $
########################################################################


from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from PyQt4.QtGui                                                     import *
from DIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item

__RCSID__ = "$Id: ControlerProductionLookup.py,v 1.5 2009/10/19 11:17:39 zmathe Exp $"

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
      keys.reverse()
      for i in keys:
        self.__list += [str(i)]
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
    self.getWidget().getListView().reset()
    self.getWidget().close()
    
  
  def all(self):
    widget = self.getWidget()
    data = widget.getModel().getAllData()
    parent = Item({'fullpath':'/'},None)
    for i in data:
      parent.addItem(self.__model.getChildren()[i])
    message = Message({'action':'showAllProduction','items':parent})
    self.getParent().messageFromChild(self, message)
    self.getWidget().close()
    
  #############################################################################  
  def textChanged(self):
    widget = self.getWidget()
    pattern = str(widget.getLineEdit().text())
    new_list = [item for item in self.__list if item.find(pattern) == 0]
    widget.getModel().setAllData(new_list)