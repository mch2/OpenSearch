# PPL High-Priority Function — Test Coverage & Pass Rate

_`1-shard` isolates the function (no distributed noise). `2-shard` is the same tests under the distributed stress config — failures there are mostly limit-doubling / ordering, **not** the function. Latest `main`, all unmuted. A function's "fail" can also mean another op in the same query failed (over-attribution), except where marked unimplemented._

| Function | Test units | 1-shard | 2-shard | Verdict |
|---|--:|:--:|:--:|---|
| `round` | 17 | 9/17 | 7/17 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `pow` | 10 | 9/10 | 5/10 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `substr` | 1 | 1/1 | 0/1 | ✅ function OK |
| `substring` | 5 | 3/5 | 3/5 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `upper` | 5 | 2/5 | 1/5 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `lower` | 4 | 2/4 | 1/4 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `replace` | 5 | 4/5 | 4/5 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `now` | 4 | 1/4 | 1/4 | 🔴 likely broken / unimplemented |
| `strftime` | 7 | 7/7 | 6/7 | ✅ function OK |
| `date_add` | 1 | 0/1 | 0/1 | 🔴 likely broken / unimplemented — *No backend supports* failures |
| `curdate` | 1 | 0/1 | 0/1 | 🔴 likely broken / unimplemented |
| `date_format` | 18 | 17/18 | 17/18 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `from_unixtime` | 3 | 3/3 | 1/3 | ✅ function OK |
| `case` | 13 | 11/13 | 11/13 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `if` | 17 | 16/17 | 13/17 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `coalesce` | 7 | 7/7 | 5/7 | ✅ function OK |
| `isnull` | 4 | 3/4 | 1/4 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `isnotnull` | 12 | 11/12 | 8/12 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `nullif` | 4 | 4/4 | 3/4 | ✅ function OK |
| `ifnull` | 5 | 5/5 | 4/5 | ✅ function OK |
| `tonumber` | 8 | 8/8 | 7/8 | ✅ function OK |
| `cast` | 13 | 13/13 | 9/13 | ✅ function OK |
| `split` | 19 | 2/19 | 2/19 | 🔴 likely broken / unimplemented — *No backend supports* failures |
| `mvindex` | 6 | 5/6 | 5/6 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `mvappend` | 7 | 6/7 | 6/7 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `mvjoin` | 4 | 3/4 | 3/4 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `avg` | 74 | 62/73 | 44/73 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `count` | 231 | 181/227 | 161/227 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `sum` | 59 | 48/58 | 46/58 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `max` | 39 | 34/39 | 26/39 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `min` | 31 | 27/31 | 20/31 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `distinct_count` | 9 | 5/9 | 3/9 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `percentile` | 8 | 2/8 | 2/8 | 🔴 likely broken / unimplemented |
| `stddev_pop` | 13 | 12/12 | 8/12 | ✅ function OK |
| `span` | 27 | 25/27 | 16/27 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `width_bucket` | 0 | 0/0 | 0/0 | ❌ NO TESTS (function not implemented) |
| `row_number` | 1 | 1/1 | 1/1 | ✅ function OK |
| `rank` | 0 | 0/0 | 0/0 | ❌ NO TESTS (function not implemented) |
| `dense_rank` | 0 | 0/0 | 0/0 | ❌ NO TESTS (function not implemented) |
| `lag` | 0 | 0/0 | 0/0 | ❌ NO TESTS (function not implemented) |
| `lead` | 0 | 0/0 | 0/0 | ❌ NO TESTS (function not implemented) |
| `grok` | 9 | 0/9 | 0/9 | 🔴 likely broken / unimplemented — *No backend supports* failures |
| `parse` | 9 | 8/9 | 8/9 | ⚠️ mostly OK; remaining fails co-located or distributed |
| `rex` | 36 | 30/36 | 26/36 | ⚠️ mostly OK; remaining fails co-located or distributed |
