# pylint: skip-file

"""
It used to control the advanced save widgets
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from PyQt4.QtGui                                                         import QMessageBox

__RCSID__ = "$Id$"

#############################################################################
class ControlerAdvancedSave( ControlerAbstract ):
  """ ControlerAdvancedSave class"""

  #############################################################################
  def __init__( self, widget, parent ):
    """initialize the controller"""
    ControlerAbstract.__init__( self, widget, parent )
    self.__sites = {'Select a site':None}

    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
    try:
      shortSiteNames = DMSHelpers().getShortSiteNames( withStorage = False, tier = ( 0, 1 ) )
    except AttributeError:
      shortSiteNames = {"CERN":"LCG.CERN.ch",
                        "RAL":"LCG.RAL.uk",
                        "IN2P3":"LCG.IN2P3.fr",
                        "GRIDKA":"LCG.GRIDKA.de",
                        "NIKHEF":"LCG.NIKHEF.nl",
                        "CNAF":"LCG.CNAF.it",
                        "RRCKI":"LCG.RRCKI.ru",
                        "PIC":"LCG.PIC.es"}
    self.__sites.update( shortSiteNames )

  #############################################################################
  def messageFromParent( self, message ):
    """handles the messages sent from the parent"""
    if message.action() == 'showWidget':
      widget = self.getWidget()
      widget.fillWindows( self.__sites )
      widget.show()
    else:
      print 'Unknown messageaa!', message.action()

  #############################################################################
  def messageFromChild( self, sender, message ):
    pass

  #############################################################################
  def lfnButtonChanged( self ):
    """handles the lfn button action"""
    widget = self.getWidget()
    widget.setLFNbutton()

  #############################################################################
  def pfnButtonChanged( self ):
    """handles the action of the pfn button"""
    widget = self.getWidget()
    widget.setPFNbutton()

  #############################################################################
  def saveButton( self ):
    """handles the action of the save button"""
    widget = self.getWidget()
    filename = str( widget.getLineEdit().text() )
    if filename == '':
      QMessageBox.information( self.getWidget(), "Error...", "File name is missing!", QMessageBox.Ok )
    else:
      site = self.__sites[str( widget.getSite() )]
      if site == None:
        QMessageBox.information( widget, "Error", 'Please select a site!', QMessageBox.Ok )
        return
      site = self.__sites[str( widget.getSite() )]
      infos = {'Site':site, 'pfn':widget.isPFNbuttonChecked(), 'lfn':widget.isLFNbuttonChecked(), 'FileName':filename}
      message = Message( {'action':'advancedSave', 'selection':infos} )
      self.getWidget().waitCursor()
      self.getParent().messageFromChild( self, message )
      self.getWidget().arrowCursor()
      widget.close()
