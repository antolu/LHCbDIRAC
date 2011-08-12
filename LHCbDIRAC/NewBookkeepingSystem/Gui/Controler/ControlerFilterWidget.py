########################################################################
# $Id: $
########################################################################


from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Message                       import Message
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Item                          import Item

from PyQt4.QtGui                                                     import *
__RCSID__ = "$Id: $"

#############################################################################
class ControlerFilterWidget(ControlerAbstract):

  #############################################################################
  def __init__(self, widget, parent):
    super(ControlerFilterWidget, self).__init__(widget, parent)
    self.__model = None

  #############################################################################
  def messageFromParent(self, message):
    if message.action()=='list':
      self.__list = []
      self.__model = message['items']
      self.getWidget().setModel(self.__model)
    else:
      print 'Unknown message!',message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def textChanged(self):
    widget = self.getWidget()
    pattern = str(widget.getLineEdit().text())
    new_list = [item for item in self.__model if item.find(pattern) == 0]
    widget.getModel().setAllData(new_list)


  #############################################################################
  def okPressed(self):
    widget = self.getWidget()
    indexes = widget.getListView().selectedIndexes()
    selected = []
    if len(indexes) != 0:
      for i in indexes:
        item = str(i.data().toString())
        selected += [item]
      if len(selected) != 0:
        message = Message({'action':'applyFilter','items':selected})
        self.getParent().messageFromChild(self, message)
    else:
      QMessageBox.information(self.getWidget(), "More information...", "Please select TCKs",QMessageBox.Ok)

  #############################################################################
  def allPressed(self):
    list = self.getWidget().getModel().getAllData()
    message = Message({'action':'applyFilter','items':list})
    self.getParent().messageFromChild(self, message)
