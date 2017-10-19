#!/usr/bin/env python
"""
Create symlinks to all teh scripts found in the LHCbDIRAC tree into a single directory
"""

import os
import logging

log = logging.getLogger()

prjroot = '.'
scripts_dir = os.path.join( prjroot, 'scripts' )


if not os.path.isdir( scripts_dir ):
  os.makedirs( scripts_dir )

for root, dirs, files in os.walk( os.path.join( prjroot, 'LHCbDIRAC' ) ):
  if 'scripts' in dirs:
    # Print directory as it contains the script directory
    log.info( '  - %s', root )
  elif os.path.basename( root ) == 'scripts':
    # Loop over the files in the scripts directory
    for f in files:
      # Remove extension if it exists and replace underscore with dash
      f = os.path.splitext( f )[0].replace( '_', '-' )
      dst = os.path.join( scripts_dir, f )
      # Only create a symlink if it doesn't exist or it is broken
      if not os.path.exists( dst ):
        # If the link however exists but is broken, remove it
        if os.path.lexists( dst ):
          os.remove( dst )
        # Make a link relative to scripts_dir in order to be portable
        os.symlink( os.path.relpath( os.path.join( root, f ), scripts_dir ), dst )
      os.chmod( dst, 0755 )  # ensure that the new file is executable
