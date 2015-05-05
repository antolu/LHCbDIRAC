""" PluginScript module holds PluginScript class, which is an extension of DMScript class
"""

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, convertSEs

class Setter( object ):
  def __init__( self, obj, name ):
    self.name = name
    self.obj = obj
  def setOption( self, val ):
    if val:
      val = val.split( ',' )
    else:
      val = []
    self.obj.options[self.name] = val

    return S_OK()

class PluginScript( DMScript ):
  """ Scripts utilities class
  """

  def __init__( self ):
    super( PluginScript, self ).__init__()

  def registerPluginSwitches( self ):
    self.registerBKSwitches()
    self.setSEs = {}
    parameterSEs = ( "KeepSEs", "Archive1SEs", "Archive2SEs",
                     "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs" )

    Script.registerSwitch( "", "Plugin=",
                           "   Plugin name (mandatory)", self.setPlugin )
    Script.registerSwitch( "t:", "Type=",
                           "   Transformation type [Replication] (Removal automatic)", self.setTransType )
    Script.registerSwitch( "", "NumberOfReplicas=",
                           "   Number of copies to create or to remove", self.setReplicas )
    for param in parameterSEs:
      self.setSEs[param] = Setter( self, param )
      Script.registerSwitch( "", param + '=',
                             "   List of SEs for the corresponding parameter of the plugin",
                             self.setSEs[param].setOption )
    Script.registerSwitch( "g:", "GroupSize=",
                           "   GroupSize parameter for merging (GB) or nb of files" , self.setGroupSize )
    Script.registerSwitch( "", "Parameters=",
                           "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}", self.setParameters )
    Script.registerSwitch( "", "RequestID=",
                           "   Sets the request ID (default 0)", self.setRequestID )
    Script.registerSwitch( "", "ProcessingPasses=",
                           "   List of processing passes for the DeleteReplicasWhenProcessed plugin",
                           self.setProcessingPasses )
    Script.registerSwitch( "", "Period=",
                           "   minimal period at which a plugin is executed (if instrumented)", self.setPeriod )
    Script.registerSwitch( "", "CacheLifeTime=", "   plugin cache life time", self.setCacheLifeTime )
    Script.registerSwitch( "", "CleanTransformations",
                           "   (only for DestroyDataset) clean transformations from the files being destroyed",
                           self.setCleanTransformations )
    Script.registerSwitch( '', 'Debug', '   Sets a debug flag in the plugin', self.setDebug )

  def setPlugin( self, val ):
    self.options['Plugin'] = val
    return S_OK()

  def setTransType( self, val ):
    self.options['Type'] = val
    return S_OK()

  def setReplicas ( self, val ):
    self.options['NumberOfReplicas'] = val
    return S_OK()

  def setGroupSize ( self, groupSize ):
    try:
      if float( int( groupSize ) ) == float( groupSize ):
        groupSize = int( groupSize )
      else:
        groupSize = float( groupSize )
      self.options['GroupSize'] = groupSize
    except:
      pass
    return S_OK()

  def setParameters ( self, val ):
    self.options['Parameters'] = val
    return S_OK()

  def setRequestID ( self, val ):
    self.options['RequestID'] = val
    return S_OK()

  def setProcessingPasses( self, val ):
    self.options['ProcessingPasses'] = val.split( ',' )
    return S_OK()

  def setCacheLifeTime( self, val ):
    try:
      self.options['CacheLifeTime'] = int( val )
    except:
      gLogger.error( 'Invalid value for CacheLifeTime: %s' % val )
    return S_OK()

  def setPeriod( self, val ):
    self.options['Period'] = val
    return S_OK()

  def setCleanTransformations( self, val ):
    self.options['CleanTransformations'] = True
    return S_OK()

  def setDebug( self, val ):
    self.options['Debug'] = True
    return S_OK()

  def getPluginParameters( self ):
    if 'Parameters' in self.options:
      params = eval( self.options['Parameters'] )
    else:
      params = {}
    pluginParams = ( 'NumberOfReplicas', 'GroupSize', 'ProcessingPasses', 'Period', 'CleanTransformations', 'Debug', 'CacheLifeTime' )
    # print self.options
    for key in [k for k in self.options if k in pluginParams]:
      params[key] = self.options[key]
    for key in [k for k in self.options if k.endswith( 'SE' ) or k.endswith( 'SEs' )]:
      params[key] = convertSEs( self.options[key] )
    return params

