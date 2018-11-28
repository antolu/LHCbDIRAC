#! /usr/bin/env python
""" Just importing stuff that should be present
"""
# pylint: disable=unused-import,import-error

import pyparsing
import GSI
import XRootD
import gfal2
import stomp
import requests
#import futures
import certifi
import pexpect
import fts3
import arc

from distutils.spawn import find_executable

for cmd in ['voms-proxy-init2', 'voms-proxy-info2', ]:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError()

for cmd in ['glite-ce-job-submit', 'glite-ce-job-status', 'glite-ce-delegate-proxy', 'glite-ce-job-cancel']:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError("No %s" % cmd)

for cmd in ['condor_submit', 'condor_history', 'condor_q', 'condor_rm', 'condor_transfer_data']:
  res = find_executable(cmd)
  if not res:
    raise RuntimeError("No %s" % cmd)


res = find_executable('ldapsearch')
if not res:
  raise RuntimeError()
