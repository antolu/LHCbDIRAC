#!/bin/bash
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

# Entry point of docker to setup the DIRAC environment before executing the command

source /opt/dirac/bashrc


if [ -z $DIRAC_REPO ];
then
        echo "No alternative DIRAC repository specified";
else
        echo "Using alternative DIRAC repository $DIRAC_REPO";
        mv /opt/dirac/DIRAC /opt/dirac/DIRAC.orig

        if [  -z $DIRAC_BRANCH ];
        then
           echo "Using default branch";
        else
          echo "Using specific branch $DIRAC_BRANCH";
          DIRAC_BRANCH="-b $DIRAC_BRANCH";
        fi

        git clone $DIRAC_BRANCH $DIRAC_REPO /opt/dirac/DIRAC_alt
        ln -s /opt/dirac/DIRAC_alt /opt/dirac/DIRAC
fi


if [ -z $LHCB_DIRAC_REPO ];
then
        echo "No alternative LHCbDIRAC repository specified";
else
        echo "Using alternative LHCbDIRAC repository $LHCB_DIRAC_REPO";
        mv /opt/dirac/LHCbDIRAC /opt/dirac/LHCbDIRAC.orig

        if [  -z $LHCB_DIRAC_BRANCH ];
        then
           echo "Using default branch";
        else
          echo "Using specific branch $LHCB_DIRAC_BRANCH";
          LHCB_DIRAC_BRANCH="-b $LHCB_DIRAC_BRANCH";
        fi

        git clone $LHCB_DIRAC_BRANCH $LHCB_DIRAC_REPO /opt/dirac/LHCbDIRAC_alt
        # The code is contained in a subfolder of LHCbDIRAC !
        ln -s /opt/dirac/LHCbDIRAC_alt/LHCbDIRAC /opt/dirac/LHCbDIRAC
fi


exec "$@"
