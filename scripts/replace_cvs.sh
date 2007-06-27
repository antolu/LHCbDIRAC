grep -l -R gkuznets * | grep 'CVS' | gawk '{print "rm -f " $1  "; echo ':ext:isscvs.cern.ch:/local/reps/dirac' > " $1}' | sh
