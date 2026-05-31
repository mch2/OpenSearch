# PPL High-Priority — Test Coverage & 1-shard Pass Rate (full High set)

_One row per High item (commands, operators, functions). 1-shard isolates the item from distributed noise. `pervasive` = symbol operators used in nearly every query._

| Category | Item | Kind | Test units | 1-shard pass | Status |
|---|---|---|--:|:--:|---|
| Commands (Pushdown) | `search` | cmd | 0 | 0/0 | ❌ NO TESTS |
| Commands (Pushdown) | `eval` | cmd | 522 | 421/522 | ⚠️ mostly OK |
| Commands (Pushdown) | `fields` | cmd | 799 | 683/799 | ⚠️ mostly OK |
| Commands (Pushdown) | `where` | cmd | 269 | 214/265 | ⚠️ mostly OK |
| Commands (Pushdown) | `sort` | cmd | 225 | 181/225 | ⚠️ mostly OK |
| Commands (Pushdown) | `head` | cmd | 296 | 189/296 | ⚠️ mostly OK |
| Commands (Pushdown) | `stats` | cmd | 287 | 226/276 | ⚠️ mostly OK |
| Commands (Pushdown) | `dedup` | cmd | 9 | 3/9 | ⚠️ mostly OK |
| Commands (Pushdown) | `rename` | cmd | 8 | 7/8 | ⚠️ mostly OK |
| Commands (Pushdown) | `top` | cmd | 6 | 6/6 | ✅ OK |
| Commands (Pushdown) | `rare` | cmd | 5 | 5/5 | ✅ OK |
| Commands (Pushdown) | `fillnull` | cmd | 16 | 13/16 | ⚠️ mostly OK |
| Commands (Pushdown) | `timechart` | cmd | 1 | 1/1 | ✅ OK |
| Commands (Pushdown) | `parse` | cmd | 18 | 16/18 | ⚠️ mostly OK |
| Commands (Pushdown) | `rex` | cmd | 72 | 60/72 | ⚠️ mostly OK |
| Commands (Pushdown) | `spath` | cmd | 17 | 16/17 | ⚠️ mostly OK |
| Commands (Pushdown) | `bin` | cmd | 14 | 14/14 | ✅ OK |
| Commands (Pushdown) | `chart` | cmd | 1 | 1/1 | ✅ OK |
| Commands (Pushdown) | `table` | cmd | 5 | 5/5 | ✅ OK |
| Commands (Pushdown) | `regex` | cmd | 12 | 12/12 | ✅ OK |
| Commands (Pushdown) | `reverse` | cmd | 8 | 8/8 | ✅ OK |
| Commands (Pushdown) | `replace` | cmd | 12 | 11/12 | ⚠️ mostly OK |
| Commands (Coord-Exec) | `join` | cmd | 3 | 2/3 | ⚠️ mostly OK |
| Commands (Coord-Exec) | `lookup` | cmd | 6 | 4/6 | ⚠️ mostly OK |
| Commands (Coord-Exec) | `eventstats` | cmd | 39 | 34/39 | ⚠️ mostly OK |
| Commands (Coord-Exec) | `appendcol` | cmd | 1 | 1/1 | ✅ OK |
| Operators | `+` | op | — | pervasive | ✅ used throughout |
| Operators | `-` | op | — | pervasive | ✅ used throughout |
| Operators | `*` | op | — | pervasive | ✅ used throughout |
| Operators | `/` | op | — | pervasive | ✅ used throughout |
| Operators | `%` | op | — | pervasive | ✅ used throughout |
| Operators | `=` | op | — | pervasive | ✅ used throughout |
| Operators | `!=` | op | — | pervasive | ✅ used throughout |
| Operators | `<` | op | — | pervasive | ✅ used throughout |
| Operators | `>` | op | — | pervasive | ✅ used throughout |
| Operators | `<=` | op | — | pervasive | ✅ used throughout |
| Operators | `>=` | op | — | pervasive | ✅ used throughout |
| Operators | `and` | opkw | 77 | 64/77 | ⚠️ mostly OK |
| Operators | `or` | opkw | 29 | 26/28 | ⚠️ mostly OK |
| Operators | `not` | opkw | 88 | 79/85 | ⚠️ mostly OK |
| Operators | `xor` | opkw | 1 | 1/1 | ✅ OK |
| Operators | `in` | opkw | 96 | 88/96 | ⚠️ mostly OK |
| Operators | `like` | opkw | 33 | 22/33 | ⚠️ mostly OK |
| Operators | `between` | opkw | 5 | 5/5 | ✅ OK |
| Operators | `is null` | opkw | 10 | 10/10 | ✅ OK |
| Operators | `is not null` | opkw | 23 | 18/23 | ⚠️ mostly OK |
| Math | `round` | fn | 17 | 9/17 | ⚠️ mostly OK |
| Math | `pow` | fn | 10 | 9/10 | ⚠️ mostly OK |
| String/Text | `substr` | fn | 1 | 1/1 | ✅ OK |
| String/Text | `upper` | fn | 5 | 2/5 | ⚠️ mostly OK |
| String/Text | `lower` | fn | 4 | 2/4 | ⚠️ mostly OK |
| String/Text | `replace` | fn | 12 | 11/12 | ⚠️ mostly OK |
| Date/Time | `now` | fn | 4 | 1/4 | 🔴 broken/unimpl |
| Date/Time | `strftime` | fn | 7 | 7/7 | ✅ OK |
| Date/Time | `date_add` | fn | 1 | 0/1 | 🔴 broken/unimpl |
| Date/Time | `curdate` | fn | 1 | 0/1 | 🔴 broken/unimpl |
| Date/Time | `date_format` | fn | 18 | 17/18 | ⚠️ mostly OK |
| Date/Time | `from_unixtime` | fn | 3 | 3/3 | ✅ OK |
| Conditional/Null | `case` | fn | 13 | 11/13 | ⚠️ mostly OK |
| Conditional/Null | `if` | fn | 17 | 16/17 | ⚠️ mostly OK |
| Conditional/Null | `coalesce` | fn | 7 | 7/7 | ✅ OK |
| Conditional/Null | `isnull` | fn | 4 | 3/4 | ⚠️ mostly OK |
| Conditional/Null | `isnotnull` | fn | 12 | 11/12 | ⚠️ mostly OK |
| Conditional/Null | `nullif` | fn | 4 | 4/4 | ✅ OK |
| Conditional/Null | `ifnull` | fn | 5 | 5/5 | ✅ OK |
| Convert | `tonumber` | fn | 8 | 8/8 | ✅ OK |
| Convert | `cast` | fn | 13 | 13/13 | ✅ OK |
| Collection/MV | `split` | fn | 19 | 2/19 | 🔴 broken/unimpl |
| Collection/MV | `mvindex` | fn | 6 | 5/6 | ⚠️ mostly OK |
| Collection/MV | `mvappend` | fn | 7 | 6/7 | ⚠️ mostly OK |
| Collection/MV | `mvjoin` | fn | 4 | 3/4 | ⚠️ mostly OK |
| Aggregates (Simple) | `avg` | fn | 74 | 62/73 | ⚠️ mostly OK |
| Aggregates (Simple) | `count` | fn | 231 | 181/227 | ⚠️ mostly OK |
| Aggregates (Simple) | `sum` | fn | 59 | 48/58 | ⚠️ mostly OK |
| Aggregates (Simple) | `max` | fn | 39 | 34/39 | ⚠️ mostly OK |
| Aggregates (Simple) | `min` | fn | 31 | 27/31 | ⚠️ mostly OK |
| Aggregates (Simple) | `distinct_count` | fn | 9 | 5/9 | ⚠️ mostly OK |
| Aggregates (Statistical) | `percentile` | fn | 8 | 2/8 | 🔴 broken/unimpl |
| Aggregates (Statistical) | `stddev_pop` | fn | 13 | 12/12 | ✅ OK |
| Aggregates (Binning) | `span` | fn | 27 | 25/27 | ⚠️ mostly OK |
| Aggregates (Binning) | `width_bucket` | fn | 0 | 0/0 | ❌ NO TESTS |
| Aggregates (Misc/State) | `earliest` | fn | 6 | 4/6 | ⚠️ mostly OK |
| Aggregates (Misc/State) | `latest` | fn | 7 | 6/7 | ⚠️ mostly OK |
| Aggregates (Misc/State) | `values` | fn | 10 | 8/10 | ⚠️ mostly OK |
| Aggregates (Misc/State) | `list` | fn | 4 | 4/4 | ✅ OK |
| Aggregates (Misc/State) | `take` | fn | 4 | 3/4 | ⚠️ mostly OK |
| Window | `row_number` | fn | 1 | 1/1 | ✅ OK |
| Window | `rank` | fn | 0 | 0/0 | ❌ NO TESTS |
| Window | `dense_rank` | fn | 0 | 0/0 | ❌ NO TESTS |
| Window | `lag` | fn | 0 | 0/0 | ❌ NO TESTS |
| Window | `lead` | fn | 0 | 0/0 | ❌ NO TESTS |
| Pattern/Parse | `grok` | cmd | 9 | 0/9 | 🔴 broken/unimpl |
| Pattern/Parse | `parse` | cmd | 18 | 16/18 | ⚠️ mostly OK |
| Pattern/Parse | `rex` | cmd | 72 | 60/72 | ⚠️ mostly OK |
| Relevance | `match` | fn | 24 | 15/20 | ⚠️ mostly OK |
| Relevance | `match_phrase` | fn | 1 | 1/1 | ✅ OK |
| Relevance | `match_bool_prefix` | fn | 1 | 1/1 | ✅ OK |
| Relevance | `query_string` | fn | 1 | 0/1 | 🔴 broken/unimpl |
| Relevance | `simple_query_string` | fn | 1 | 0/1 | 🔴 broken/unimpl |
| Relevance | `multi_match` | fn | 1 | 0/1 | 🔴 broken/unimpl |
