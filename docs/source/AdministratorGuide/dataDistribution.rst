=================
Data distribution
=================

*******
Archive
*******

The defautl option is at `Operations/<Setup>/TransformationPlugins/Archive2SEs`. it can be overwritten in each plugin.
The choice is done randomly.

*************
DST broadcast
*************

The broadcast done by LHCbDSTBroadcast plugin is done according to the free space

=====================================
RAW files processing and distribution
=====================================

The RAW files all have a copy at CERN, and are then distributed across the Tier1. The processing is shared between CERN and the Tier1.

The selection of the site for copying the data and the site where the data will be processed (so called *RunDestination*) is done by the *RAWReplication* plugin. To do so, it uses shares that are defined in `Operations/<Setup>/Shares`


**********************************************
Selection of a Tier1 for the data distribution
**********************************************

The quota are defined in `Operations/<Setup>/Shares/RAW`.

Since CERN has a copy of every file, it does not appear in the quota.

In practice, the absolute values are meaningless, what matters is their relative values. The total is normalized to a 100 in the code.

When choosing where a run will be copied, we look at the current status of the distribution, based on the run duration. The site which is the furthest from its objectives is selected.


********************************************
Selection of a Tier1 for the data processing
********************************************

Once a Tier1 has been selected to copy the RAW file, one needs to select a site where the data will be processed: either CERN or the Tier1 where the data is: the *RunDestination*. Note that the destination is chosen per Run, and will stay as is: all the production will process the run at the same location.

This is done using `Operations/<Setup>/Shares/CPUforRAW`. There, the values are independent: they should be between 0 and 1, and represents the fraction of data it will process compared to CERN. So if the value is 0.8, it means 80% of the data copied to that site will be processed at that site, and the 20 other percent at CERN.

This share is used by the processing plugin `DataProcessing`.
The equivalent exists when reprocessing (plugin `DataReprocessing`): `Operations/<Setup>/Shares/CPUforReprocessing`


******************************
Change of values in the shares
******************************

Note: if a change is to be made after a transformation has already distributed a lot of files, it is better to start a new transformation.

The principle goes as follow, but is obviously better done with an Excel sheet.

From Rebus (https://gstat-wlcg.cern.ch/apps/pledges/resources/), we take for each T1 the CPUPledge (in MHS06) and the TapePledge (PB). We deduce easily the CPUPledgePercent and TapePledgePercent.

From the StorageUsageSummary, we get the CurrentTapeUsage (e.g. dirac-dms-storage-usage-summary --LCG --Site LCG.CERN.cern
)

We then have::

  AdditionalTape = TapePledge - CurrentTape

From which we deduce AdditionalTapePercent.

We then compute the ratio::

  CPU / NewTape = CPUPledgePercent / AdditionalTapePercent

It represents the increase of CPU pledge vs the increase of Tape with respect to the total.

We then chose a certain percentage of data which is going to be processed at CERN. Say 20%. We then get::

  CPUShare = CPUPledgePercent*(1-0.2)

The next step is to assign a CPUFraction (in [0:1]) by hand following this guideline: the lower the CPU/Tape ratio, the lower the fraction processed "locally".

The final step is to compute::

  RAWShare = CPUShare/CPUFraction

It represents the percentage of data to be copied to the given T1.

Obviously, since we have an extra constraint, we have to give a degree of freedom. We normally give it to RAL with the following::

  RALRAWShare = 100% - Sum(OtherShares)
  RALCPUFraction = RALCpuShare / RALRAWShare

CPUFraction corresponds to `Operations/<Setup>/Shares/CPUforRAW`

RAWShare corresponds to `Operations/<Setup>/Shares/RAW`
