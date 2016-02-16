########################################################################
# $Id: Help.py 69359 2013-08-08 13:57:13Z phicharp $
########################################################################
"""
Help class
"""
from DIRAC                                                               import gLogger, S_OK

__RCSID__ = "$Id: Help.py 69359 2013-08-08 13:57:13Z phicharp $"

#############################################################################
class Help:
  """ Class for help"""
  #############################################################################
  def __init__( self ):
    """ constructor"""
    pass

  #############################################################################
  @staticmethod
  def helpConfig(treeLevels):
    """ configure the help"""
    if treeLevels == -1:
      print "-------------------------------------"
      print "| Please use the following comand:   |"
      print "| client.list()                      |"
      print "--------------------------------------"
    elif treeLevels == 0:
      print "-----------------------------------------"
      print "| Please choose one configuration!       |"
      print "| For example:                           |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5') |"
      print "------------------------------------------"

    elif treeLevels == 1:
      print "-------------------------------------------------------"
      print "| Please choose one event type!                       |"
      print "| For example:                                        |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010') |"
      print "-------------------------------------------------------"

    elif treeLevels == 2:
      print "-----------------------------------------------------------------"
      print "| Please choose one production!                                 |"
      print "| For example:                                                  |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010/PROD_1933') |"
      print "-----------------------------------------------------------------"

    elif treeLevels == 3:
      print "------------------------------------------------------------------------|"
      print "| Please choose one file type!                                          |"
      print "| For example:                                                          |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010/PROD_1933/FTY_RDST Brunel v30r17')|"
      print "-------------------------------------------------------------------------|"
    return S_OK()

  #############################################################################
  @staticmethod
  def helpProcessing(treeLevels ):
    """ help """
    if treeLevels == -1:
      print "-------------------------------------"
      print "| Please use the following comand:   |"
      print "| client.list()                      |"
      print "--------------------------------------"
    elif treeLevels == 0:
      print "-----------------------------------------"
      print "| Please choose one Processing Pass!     |"
      print "| For example:                           |"
      print "| client.list('/PPA_Pass342')            |"
      print "------------------------------------------"

    elif treeLevels == 1:
      print "-------------------------------------------------------"
      print "| Please choose one production!                       |"
      print "| For example:                                        |"
      print "| client.list('/PPA_Pass342/PRO_1858')                |"
      print "-------------------------------------------------------"

    elif treeLevels == 2:
      print "-----------------------------------------------------------------"
      print "| Please choise one event type!                                 |"
      print "| For example:                                                  |"
      print "| client.list('/PPA_Pass342/PRO_1858/EVT_10000000')             |"
      print "-----------------------------------------------------------------"

    elif treeLevels == 3:
      print "-----------------------------------------------------------------"
      print "| Please choose one file type!                                  |"
      print "| For example:                                                  |"
      print "| client.list('/PPA_Pass342/PRO_1858/EVT_10000000/FTY_RDST')    |"
      print "-----------------------------------------------------------------"

  #############################################################################
  @staticmethod
  def helpEventType(treeLevels ):
    """ ...."""
    gLogger.debug(treeLevels)
    gLogger.warn( "Not Implemented!" )
    return

