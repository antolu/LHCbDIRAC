########################################################################
# File :   dirac-lhcb-certificates-install.py
# Author : Paul Szczypka
########################################################################
#
########################################################################
import os, urllib, tarfile, time, sys

# Flags:
verbose = False

def untarFile():
      myFileName = "grid-security.tar.gz"
      myBaseURL = "http://lhcb-wdqa.web.cern.ch/lhcb-wdqa/distribution/"
      # Get the tar file.
      localFilePath, fileMessage = urllib.urlretrieve('%s%s' %(myBaseURL,myFileName), myFileName)
      # Check the file you've just got, possibly overkill.
      if not os.path.isfile(localFilePath):
            print localFilePath, "is not a simple file!"
            sys.exit(1)
      if not tarfile.is_tarfile(localFilePath):
            print localFilePath, "is not a tar file!"
            sys.exit(1)
      else:
            myTARFile = tarfile.open(localFilePath)
            for member in myTARFile.getmembers():
                  myTARFile.extract(member)
            myTARFile.close()      
      if os.path.isfile(myFileName):
            os.remove(myFileName)
      return

if not os.environ.get("DIRACROOT"):
      print "DIRACROOT is not set"
      print "Set DIRACROOT and re-run"
      sys.exit(1)
else:
      print ("%s%s") %("Using DIRACROOT=", os.environ.get("DIRACROOT")  )
      myDIRACROOT = os.environ.get("DIRACROOT")
      if os.path.exists("%s%s" %(myDIRACROOT,"/lcg/etc/grid-security/certificates/")):
            myPath = "%s%s" %(myDIRACROOT,"/lcg/etc/grid-security/certificates/")
            # Check dir age:
            if (time.localtime().tm_yday - time.localtime(os.stat(myPath).st_mtime).tm_yday > 7) \
                      or (time.localtime().tm_year - time.localtime(os.stat(myPath).st_mtime).tm_year):
                  untarFile()
      else:
      # Path doesn't exist -> untar for the first time.
            untarFile()

print ""
print "%s" %("Please set the folowing environment variables in all future DIRAC sessions:\n")
print "%s" %("export X509_CERT_DIR=$DIRACROOT/lcg/etc/grid-security/certificates/")
print "%s" %("export X509_VOMS_DIR=$DIRACROOT/lcg/etc/grid-security/vomsdir/\n")
