#!/usr/bin/env python

"""
 Create a new dataset replication or removal transformation according to plugin
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.TransformationSystem.Utilities.PluginScript import PluginScript, Script

if __name__ == "__main__":

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()
  pluginScript.registerFileSwitches()
  Script.registerSwitch("", "Name=", "   Give a name to the transformation, only if files are given")
  Script.registerSwitch(
      "",
      "SetInvisible",
      "Before creating the transformation, set the files in the BKQuery as invisible (default for DeleteDataset)")
  Script.registerSwitch("S", "Start", "   If set, the transformation is set Active and Automatic [False]")
  Script.registerSwitch("", "Force", "   Force transformation to be submitted even if no files found")
  Script.registerSwitch("", "Test", "   Just print out but not submit")
  Script.registerSwitch("", "NoFCCheck", "   Suppress the check in FC for removal transformations")
  Script.registerSwitch("", "Unique", "   Refuses to create a transformation with an existing name")
  Script.registerSwitch("", "Depth=", "   Depth in path for replacing /... in processing pass")
  Script.registerSwitch("", "Chown=", "   Give user/group for chown of the directories of files in the FC")
  Script.registerSwitch(
      "",
      "MCVersion=",
      "   (list of) BK ConfigVersion; gets active MC processing passes ('All' for all years)")
  Script.registerSwitch("", "ListProcessingPasses", "   Only lists the processing passes")

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ]))

  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.DataManagementSystem.Client.AddTransformation import executeAddTransformation
  executeAddTransformation(pluginScript)
