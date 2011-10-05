"""
  Utilities for scripts dealing with transformations
"""

from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.Core.Utilities.List                                import sortList

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

class setter:
  def __init__( self, obj, name ):
    self.name = name
    self.obj = obj
  def setOption( self, val ):
    self.obj.options[self.name] = val.split( ',' )
    return DIRAC.S_OK()

class PluginScript( DMScript ):
  def __init__( self, useBKQuery = False ):
    DMScript.__init__( self, useBKQuery = useBKQuery )

  def registerPluginSwitches( self ):
    self.registerBKSwitches()
    self.setSEs = {}
    parameterSEs = ( "KeepSEs", "Archive1SEs", "Archive2SEs", "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs" )

    Script.registerSwitch( "", "Plugin=", "   Plugin name (mandatory)", self.setPlugin )
    Script.registerSwitch( "t:", "Type=", "   Transformation type [Replication] (Removal automatic)", self.setTransType )
    Script.registerSwitch( "", "NumberOfReplicas=", "   Number of copies to create or to remove", self.setReplicas )
    for param in parameterSEs:
      self.setSEs[param] = setter( self, param )
      Script.registerSwitch( "", param + '=', "   List of SEs for the corresponding parameter of the plugin", self.setSEs[param].setOption )
    Script.registerSwitch( "g:", "GroupSize=", "   GroupSize parameter for merging (GB) or nb of files" , self.setGroupSize )
    Script.registerSwitch( "", "Parameters=", "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}", self.setParameters )
    Script.registerSwitch( "", "RequestID=", "   Sets the request ID (default 0)", self.setRequestID )

  def setPlugin( self, val ):
    self.options['Plugin'] = val
    return DIRAC.S_OK()

  def setTransType( self, val ):
    self.options['Type'] = val
    return DIRAC.S_OK()

  def setReplicas ( self, val ):
    self.options['Replicas'] = val
    return DIRAC.S_OK()

  def setGroupSize ( self, val ):
    self.options['GroupSize'] = float( val )
    return DIRAC.S_OK()

  def setParameters ( self, val ):
    self.options['Parameters'] = val
    return DIRAC.S_OK()

  def setRequestID ( self, val ):
    self.options['RequestID'] = val
    return DIRAC.S_OK()

