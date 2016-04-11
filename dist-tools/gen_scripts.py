#!/usr/bin/env python

import os
import shutil
import logging

log = logging.getLogger()

prjroot='.'
scripts_dir = os.path.join( prjroot, 'scripts' )


if not os.path.isdir( scripts_dir ):
  os.makedirs( scripts_dir )

for root, dirs, files in os.walk( os.path.join( prjroot, 'LHCbDIRAC' ) ):
  if 'scripts' in dirs:
    log.info( '  - %s', root )
        # we are only interested in the content of the scripts directories
    dirs[:] = ['scripts']
  elif os.path.basename( root ) == 'scripts':
    dirs[:] = []  # avoid further recursion (it should not be needed)
    for f in files:
      if f.endswith( '.py' ):
        dst = os.path.join( scripts_dir, f[:-3] )
      else:
        dst = os.path.join( scripts_dir, f )
      shutil.copyfile( os.path.join( root, f ), dst )
      os.chmod( dst, 0755 )  # ensure that the new file is executable
