"""
It controlles the Processing Pass dialog
"""
########################################################################
# $Id$
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

__RCSID__ = "$Id$"

#############################################################################
class ControlerProcessingPassDialog(ControlerAbstract):
  """
  ControlerProcessingPassDialog class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    if message.action() == 'showprocessingpass':
      feedback = message['items']
      proc = ''
      records = feedback['Records']
      parameters = feedback['Parameters']
      if feedback['TotalRecords'] > 0:
        proc = records[records.keys()[0]][1][1]

      widget = self.getWidget()
      widget.setTotalProccesingPass(proc)
      tabwidget = widget.getTabWidget()
      tabwidget.clear()# cleaning, I have to delete the existing tabs

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
          main.addTab(tab, i)
        else:
          main = widget.createEmptyTabWidget(desc)
          main.addTab(tab, i)
          mainWidget[desc] = main
      self.getWidget().show()

    elif message.action() == 'list':
      item = message['items']
      message = Message({'action':'procDescription', 'groupdesc':item['name']})
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
            main.addTab(tab, i)
          else:
            main = widget.createEmptyTabWidget(desc)
            main.addTab(tab, i)
            mainWidget[desc] = main
        self.getWidget().show()
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

  #############################################################################
  def close(self):
    """handles the close action"""
    #self.getWidget().hide()
    self.getWidget().close()

  #############################################################################

