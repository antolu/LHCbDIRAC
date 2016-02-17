# pylint: skip-file

"""
Controller of the Production lookup widget
"""
########################################################################
# $Id: ControlerProductionLookup.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Item                          import Item

from PyQt4.QtGui                                                         import QMessageBox

__RCSID__ = "$Id$"

#############################################################################
class ControlerProductionLookup(ControlerAbstract):
  """
  ControlerProductionLookup class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__model = None
    self.__list = []

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    if message.action() == 'list':
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
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def close(self):
    """handles the close action"""
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
        message = Message({'action':'showOneProduction', 'paths':selected})
        self.getParent().messageFromChild(self, message)
    else:
      QMessageBox.information(self.getWidget(),
                              "More information...",
                              "Please select a production or run number!",
                              QMessageBox.Ok)
    widget.hide()

  #############################################################################
  def cancel(self):
    """handles the cancel button action"""
    self.getWidget().getListView().reset()
    self.getWidget().close()
    message = Message({'action':'configbuttonChanged'})
    self.getParent().messageFromChild(self, message)


  def all(self):
    """handles the all button action"""
    widget = self.getWidget()
    data = widget.getModel().getAllData()
    parent = Item({'fullpath':'/'}, None)
    for i in data:
      parent.addItem(self.__model.getChildren()[i])
    message = Message({'action':'showAllProduction', 'items':parent})
    self.getParent().messageFromChild(self, message)
    self.getWidget().close()

  #############################################################################
  def textChanged(self):
    """handles the action created by the text editor"""
    widget = self.getWidget()
    pattern = str(widget.getLineEdit().text())
    new_list = [item for item in self.__list if item.find(pattern) == 0]
    widget.getModel().setAllData(new_list)
