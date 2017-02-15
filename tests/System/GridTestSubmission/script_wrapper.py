#!/usr/bin/env python
import os, sys
def formatVar(var):
    try:
        float(var)
        return str(var)
    except ValueError as v:
        return '\"%s\"' % str(var)

script_args = ''

del sys.argv[sys.argv.index('script_wrapper.py')]
script_args=[formatVar(v) for v in script_args]
if script_args == []: script_args = ''
os.system('export DISPLAY="localhoast:0.0" && root -l -q "runToys.C(%s)"' % script_args)
###INJECTEDCODE###
