# Zeisel

| machine | # targets | # boosting rounds | duration s |
| ---     | ---:      | ---:              | ---:             |
| nostromo | 1 | 100 | 19 s |
| nostromo | 5 | 100 | 11 s |
| nostromo | 10 | 100 | 12 s |
| nostromo | 25 | 100 | 18 s |
| nostromo | 100 | 100 | 43 s |
| nostromo | 250 | 100 | 01 m, 33 s |
| nostromo | 1000 | 100 | 05 m, 54 s |
| nostromo | 2500 | 100 | 14 m, 29 s |
| nostromo | 10000 | 100 | 52 m, 44 s |
| nostromo | 19972 | 100 | 01 h, 40 m, 36 s |

`RegressionParams(Map(seed -> 777, silent -> 1),100,false,Some(1),false)`

```
[info] ZeiselBenchmark:
[info] the emb.par pipeline from parquet
[info] - should run
[info] ScalaTest
[info] Run completed in 2 hours, 57 minutes, 16 seconds.
[info] Total number of tests run: 1
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```
