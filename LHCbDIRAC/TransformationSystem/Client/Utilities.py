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
    if val:
      val = val.split( ',' )
    else:
      val = []
    self.obj.options[self.name] = val

    return DIRAC.S_OK()

class PluginScript( DMScript ):
  def __init__( self ):
    DMScript.__init__( self )

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
    Script.registerSwitch( "", "ProcessingPasses=", "   List of processing passes for the DeleteReplicasWhenProcessed plugin", self.setProcessingPasses )
    Script.registerSwitch( "", "Period=", "   minimal period at which a plugin is executed (if instrumented)", self.setPeriod )

  def setPlugin( self, val ):
    self.options['Plugin'] = val
    return DIRAC.S_OK()

  def setTransType( self, val ):
    self.options['Type'] = val
    return DIRAC.S_OK()

  def setReplicas ( self, val ):
    self.options['NumberOfReplicas'] = val
    return DIRAC.S_OK()

  def setGroupSize ( self, groupSize ):
    try:
      if float( int( groupSize ) ) == float( groupSize ):
        groupSize = int( groupSize )
      else:
        groupSize = float( groupSize )
      self.options['GroupSize'] = groupSize
    except:
      pass
    return DIRAC.S_OK()

  def setParameters ( self, val ):
    self.options['Parameters'] = val
    return DIRAC.S_OK()

  def setRequestID ( self, val ):
    self.options['RequestID'] = val
    return DIRAC.S_OK()

  def setProcessingPasses( self, val ):
    self.options['ProcessingPasses'] = val.split( ',' )
    return DIRAC.S_OK()

  def setPeriod( self, val ):
    self.options['Period'] = val
    return DIRAC.S_OK()

  def getPluginParameters( self ):
    from DIRAC import gConfig
    if 'Parameters' in self.options:
      params = eval( self.options( 'Parameters' ) )
    else:
      params = {}
    pluginParams = ( 'NumberOfReplicas', 'GroupSize', 'ProcessingPasses', 'Period' )
    #print self.options
    for key in [k for k in self.options if k in pluginParams]:
      params[key] = self.options[key]
    for key in [k for k in self.options if k.endswith( 'SE' ) or k.endswith( 'SEs' )]:
      ses = self.options[key]
      seList = []
      for se in ses:
        seConfig = gConfig.getValue( '/Resources/StorageElementGroups/%s' % se, se )
        if seConfig != se:
          seList += [se.strip() for se in seConfig.split( ',' )]
          #print seList
        else:
          seList.append( se )
      params[key] = seList
    return params

  def getRemovalPlugins( self ):
    return ( "DestroyDataset", "DeleteDataset", "DeleteReplicas", 'DeleteReplicasWhenProcessed' )
  def getReplicationPlugins( self ):
    return ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset",
             "RAWShares", 'FakeReplication', 'ReplicateToLocalSE' )
