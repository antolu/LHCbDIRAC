====================
Productions flushing
====================

*************************
Flushing a transformation
*************************

Transformations normally have grouping factors: total size of the input files, number of files, etc. There are cases when the grouping conditions cannot be reached, for example if there are not enough files in the run to reach the threshold defined. In that case, the transformation can be *flushed*, meaning create tasks anyway with whatever is there.

The flushing is a manual operation that only has an impact on the files present at the moment of triggering it, meaning that if new files arrive later, they will accumulate again: a transformation does not stay in "flush mode".


**********************************
Flushing a run in a transformation
**********************************

Many transformations have a grouping by Run on top of a running by size/files. The same as described previously can happen: within a given run, the grouping conditions cannot be reached. In that case, it is possible to flush the run. There are two major differences compared to flushing a transformation:

  1. Flushing a run is definitive
  2. The procedure *can* be automatic

1. Flushing a run is definitive
*******************************

Once a run is set to flush, it will stay in this state. This means that if new files arrive after flushing the run, they will not be accumulated, and a new task will be create for each and every file that arrives. This is not what you want normally.

2. Automatic run flushing for Merging
*************************************

The principle always consists in going back to the RAW files of a run, and making sure that all of them have descendants in the current production. In practice, we count the number of RAW ancestors of the files in the production, and compare it with the number of RAW files declared in the BKK. These two numbers must match. This count is done by stream.

The only runs that are considered for flushing are the runs marked as 'finished' in the bookkeeping.

However, it might happen that a run does not get flushed. This normally shows an issue at the Stripping level. Consider the following example, with a Run that contains 3 raw files:


+----------+-----------+---------------------------------+
| RAW file | RDST file | Stripping output                |
+----------+-----------+---------------------------------+
| A.RAW    | A.RDST    | A.stream1, A.stream2, A.stream3 |
+----------+-----------+---------------------------------+
| B.RAW    | B.RDST    | B.stream2, B.stream3, B.stream4 |
+----------+-----------+---------------------------------+
| C.RAW    | C.RDST    | C.stream3, c.stream4            |
+----------+-----------+---------------------------------+

So, when looking at the ancestors per stream, we find:

+---------+---------------------+
| Stream  | Nb of RAW ancestors |
+---------+---------------------+
| stream1 | 1                   |
+---------+---------------------+
| stream2 | 2                   |
+---------+---------------------+
| stream3 | 3                   |
+---------+---------------------+
| stream4 | 2                   |
+---------+---------------------+

In that case, the flushing of the run will be triggered by stream3, since it finds the 3 ancestors. However, if in the stripping production, one file is never stripped because problematic, no stream will ever have all the raw files as ancestors, and the run will never be flushed. Hence, the run status in the merging is a good way to check the stripping :-)

Note that the script transformation-debug is more clever that the plugin, and can warn of such situations.
