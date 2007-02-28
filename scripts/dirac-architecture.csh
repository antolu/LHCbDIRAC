# !/bin/csh
#-----------------------------------------
# initialize CMT
#-------------------------------------
#set echo on

# set default values for CMT and gcc versions
set comp = `gcc --version | grep gcc | awk '{print $3}' | tr -d "."`
if ("$comp" >= "340") then
  set gcc = `gcc --version | grep gcc | awk '{print $3}' | awk -F. '{for(i=1; i<=2; i++){print $i}}'`
  set comp = `echo $gcc | tr -d " "`
endif


set compdef = gcc$comp
#==============================================================
# deal with different linux distributions
if (-e /etc/redhat-release) then
  set distrib = `cat /etc/redhat-release | awk '{print $1}'`
  set rhv = `cat /etc/redhat-release | tr -d '[a-z][A-Z]()'`
  if ("$distrib" == "Scientific") then
    set nativehw = `uname -i`
    set hw = $nativehw
    if ($hw == "i386") set hw = "ia32"
    # for the moment use ia32 even on amd64
    # if ($hw == "x86_64") set hw = "amd64"
    if ($hw == "x86_64") set hw = "ia32"
    set rhv = `echo ${rhv} | awk -F "." '{print $1}'`
    set rh = "slc"${rhv}"_"${hw}
  else
    set rhv = `echo ${rhv} | tr -d "."`
    set rh = "rh$rhv"
  endif
endif
# deal with OS type ===========================================
if ( "$OSTYPE" == "darwin" ) then
  set rh = `sw_vers | grep ProductVersion | awk '{print $2}' | awk -F .'{print $1 $2}'`
  set rh = "osx$rh"
#
else if ( "$OSTYPE" == "linux" ) then
# get the compiler from the arguments
  if ("$?newcomp" == "0") then
    set comp = $compdef
  else
    set comp = $newcomp
  endif

  if ( $nativehw == "x86_64" && $comp == "gcc323") then

#======= compile on SLC4 pretending it is SLC3
    setenv CMTOPT "slc3_ia32_${comp}"
    set rh = "slc3_ia32"
 endif

#==========================
  set binary =  ${rh}_${comp}
  if ($hw == "ia64") set binary = `echo $binary | sed -e 's/ia64/amd64/'`
  setenv CMTOPT  ${binary}
endif

setenv CMTCONFIG "${CMTOPT}"

echo $CMTCONFIG

exit 
