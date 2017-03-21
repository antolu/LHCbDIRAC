#! /usr/bin/env python
""" Just importing stuff that should be present
"""
#pylint: disable=unused-import,import-error

import pyparsing
import PyQt4
import GSI
import XRootD
import gfal2
import stomp
import requests
import futures
import certifi
import pexpect

from distutils.spawn import find_executable

res = find_executable('voms-proxy-init')
if not res:
  raise RuntimeError()
res = find_executable('ldapsearch')
if not res:
  raise RuntimeError()
