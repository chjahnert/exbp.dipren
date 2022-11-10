# Dipren - Distributed Processing Engine

Dipren is a distributed processing engine that supports iterating through large ordered data sets in batches. Dipren
is written in .NET 6.

### Notes
#### Batch Processing Timeout
The value must selected must be greater than the time it takes to process a batch and to perform a split request.
A split request includes two range size estimations. A good rule of thumb is `(b + 2e) * m` where `b` is the average
duration for processing a batch and `e` is the average time to perform a range size estimation. When running a small
number of processing nodes the value for `m` might be as low as 2, for a large number of processing nodes a
significantly greater value can be used. The batch processing timeout has no impact on processing speed.