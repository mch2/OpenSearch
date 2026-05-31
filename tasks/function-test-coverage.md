# PPL High-Priority Function — Test Coverage & Pass Rate

_Source: full `analytics-engine-rest` suite, 2-shard + all unmuted, latest main. Pass = the test query/method ran and passed; for monolithic `*PplIT` suites pass/fail is per-query._

| Function | Test units | Ran | Passing | Failing | Status |
|----------|-----------:|----:|--------:|--------:|--------|
| `round` | 17 | 17 | 7 | 10 | ⚠️ 7/17 passing |
| `pow` | 10 | 10 | 5 | 5 | ⚠️ 5/10 passing |
| `substr` | 1 | 1 | 0 | 1 | 🔴 0/1 passing |
| `substring` | 5 | 5 | 3 | 2 | ⚠️ 3/5 passing |
| `upper` | 5 | 5 | 1 | 4 | ⚠️ 1/5 passing |
| `lower` | 4 | 4 | 1 | 3 | ⚠️ 1/4 passing |
| `replace` | 5 | 5 | 4 | 1 | ⚠️ 4/5 passing |
| `now` | 4 | 4 | 1 | 3 | ⚠️ 1/4 passing |
| `strftime` | 7 | 7 | 6 | 1 | ⚠️ 6/7 passing |
| `date_add` | 1 | 1 | 0 | 1 | 🔴 0/1 passing |
| `curdate` | 1 | 1 | 0 | 1 | 🔴 0/1 passing |
| `date_format` | 18 | 18 | 17 | 1 | ⚠️ 17/18 passing |
| `from_unixtime` | 3 | 3 | 1 | 2 | ⚠️ 1/3 passing |
| `case` | 13 | 13 | 11 | 2 | ⚠️ 11/13 passing |
| `if` | 17 | 17 | 13 | 4 | ⚠️ 13/17 passing |
| `coalesce` | 7 | 7 | 5 | 2 | ⚠️ 5/7 passing |
| `isnull` | 4 | 4 | 1 | 3 | ⚠️ 1/4 passing |
| `isnotnull` | 12 | 12 | 8 | 4 | ⚠️ 8/12 passing |
| `nullif` | 4 | 4 | 3 | 1 | ⚠️ 3/4 passing |
| `ifnull` | 5 | 5 | 4 | 1 | ⚠️ 4/5 passing |
| `tonumber` | 8 | 8 | 7 | 1 | ⚠️ 7/8 passing |
| `cast` | 13 | 13 | 9 | 4 | ⚠️ 9/13 passing |
| `split` | 19 | 19 | 2 | 17 | ⚠️ 2/19 passing |
| `mvindex` | 6 | 6 | 5 | 1 | ⚠️ 5/6 passing |
| `mvappend` | 7 | 7 | 6 | 1 | ⚠️ 6/7 passing |
| `mvjoin` | 4 | 4 | 3 | 1 | ⚠️ 3/4 passing |
| `avg` | 74 | 73 | 44 | 29 | ⚠️ 44/73 passing (1 not-run) |
| `count` | 231 | 227 | 161 | 66 | ⚠️ 161/227 passing (4 not-run) |
| `sum` | 59 | 58 | 46 | 12 | ⚠️ 46/58 passing (1 not-run) |
| `max` | 39 | 39 | 26 | 13 | ⚠️ 26/39 passing |
| `min` | 31 | 31 | 20 | 11 | ⚠️ 20/31 passing |
| `distinct_count` | 9 | 9 | 3 | 6 | ⚠️ 3/9 passing |
| `percentile` | 8 | 8 | 2 | 6 | ⚠️ 2/8 passing |
| `stddev_pop` | 13 | 12 | 8 | 4 | ⚠️ 8/12 passing (1 not-run) |
| `span` | 27 | 27 | 16 | 11 | ⚠️ 16/27 passing |
| `width_bucket` | 0 | 0 | 0 | 0 | ❌ NO TESTS |
| `row_number` | 1 | 1 | 1 | 0 | ✅ 1/1 passing |
| `rank` | 0 | 0 | 0 | 0 | ❌ NO TESTS |
| `dense_rank` | 0 | 0 | 0 | 0 | ❌ NO TESTS |
| `lag` | 0 | 0 | 0 | 0 | ❌ NO TESTS |
| `lead` | 0 | 0 | 0 | 0 | ❌ NO TESTS |
| `grok` | 9 | 9 | 0 | 9 | 🔴 0/9 passing |
| `parse` | 9 | 9 | 8 | 1 | ⚠️ 8/9 passing |
| `rex` | 36 | 36 | 26 | 10 | ⚠️ 26/36 passing |
