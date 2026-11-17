| log file | harness | what | jobs | worker kill -9 | freezes | API kills | DB kills | fault points fired | jobs re-run | zombie writes refused | seconds | result |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 20261116-155404.log | v1 | clean | 500 | 9 | 2 | 0 | 0 | 0 | 2 | - | 55.7 | PASS |
| 20261116-160011.log | v1 | clean | 1000 | 6 | 2 | 0 | 0 | 0 | 1 | - | 44.9 | PASS |
| 20261116-160410.log | v1 | clean | 10000 | 43 | 9 | 0 | 0 | 0 | 9 | - | 186.2 | PASS |
| 20261116-160727.log | v1 | clean | 10000 | 43 | 11 | 0 | 0 | 0 | 7 | - | 186.2 | PASS |
| 20261116-161042.log | v1 | clean | 10000 | 46 | 10 | 0 | 0 | 0 | 13 | - | 190.3 | PASS |
| 20261116-161402-BUG_NO_FENCING.log | v1 | control: BUG_NO_FENCING | 3000 | 15 | 4 | 0 | 0 | 0 | 2 | - | 75.8 | FAIL I4,I5,I7,I9,I10 |
| 20261116-161535-BUG_RACY_CLAIM.log | v1 | control: BUG_RACY_CLAIM | 3000 | 30 | 7 | 0 | 0 | 0 | 1937 | - | 130.2 | FAIL I8 |
| 20261116-161801-BUG_SPLIT_TX.log | v1 | control: BUG_SPLIT_TX | 3000 | 19 | 5 | 0 | 0 | 0 | 4 | - | 95.1 | FAIL I4,I5,I6,I7 |
| 20261116-161953.log | v1 | clean, paced 100 jobs/s | 10000 | 40 | 10 | 0 | 0 | 0 | 0 | - | 180.1 | PASS |
| 20261116-164127-smoke.log | v2 | clean | 1500 | 26 | 8 | 2 | 0 | 12 | 20 | 2 | 69.8 | PASS |
| 20261116-164317-smoke-dbkill.log | v2 | clean, DB kills | 1500 | 33 | 8 | 4 | 6 | 16 | 38 | 0 | 92.7 | PASS |
| 20261116-164531.log | v2 | clean | 10000 | 138 | 39 | 12 | 0 | 80 | 146 | 22 | 295.3 | PASS |
| 20261116-165037.log | v2 | clean | 10000 | 139 | 36 | 11 | 0 | 107 | 160 | 24 | 295.9 | PASS |
| 20261116-165545.log | v2 | clean | 10000 | 148 | 39 | 14 | 0 | 100 | 152 | 22 | 320.5 | PASS |
| 20261116-170118-rps100.log | v2 | clean, paced 100 jobs/s | 10000 | 140 | 42 | 16 | 0 | 94 | 164 | 25 | 311.5 | PASS |
| 20261116-170642-dbkill.log | v2 | clean, DB kills | 10000 | 155 | 47 | 17 | 9 | 96 | 179 | 15 | 373.2 | PASS |
| 20261116-171307-BUG_NO_FENCING.log | v2 | control: BUG_NO_FENCING | 3000 | 47 | 14 | 5 | 0 | 28 | 41 | 0 | 119.3 | FAIL I4,I5,I7,I9,I10 |
| 20261116-171524-BUG_RACY_CLAIM.log | v2 | control: BUG_RACY_CLAIM | 3000 | 74 | 20 | 8 | 0 | 36 | 1554 | 1972 | 169.9 | FAIL I8 |
| 20261116-171831-BUG_SPLIT_TX.log | v2 | control: BUG_SPLIT_TX | 3000 | 48 | 14 | 5 | 0 | 26 | 51 | 3 | 125.3 | FAIL I4,I5,I6,I7,I14,I16 |
| 20261116-172054-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 110 | 0 | 0 | 0 | 0 | 85 | 0 | 244.3 | FAIL I4,I5,I7 |
| 20261116-172517-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 122 | 0 | 0 | 0 | 0 | 102 | 0 | 260.3 | FAIL I4,I5,I7 |
| 20261116-172948-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 116 | 0 | 0 | 0 | 0 | 92 | 1 | 1345.6 | FAIL I4,I5,I7 |
| 20261117-110515-BUG_SPLIT_TX-killonly.log | v2 | control: BUG_SPLIT_TX, kill -9 only | 10000 | 112 | 0 | 0 | 0 | 0 | 82 | 0 | 239.6 | FAIL I4,I5,I7 |
| 20261117-110933-dbkill.log | v2 | clean, DB kills | 10000 | 169 | 50 | 16 | 12 | 111 | 188 | 11 | 381.9 | PASS |

clean runs, harness v2 (final): 8 runs, 8 PASS, 0 not PASS; 63000 jobs, 948 worker kills, 269 freezes, 92 API kills, 27 DB kills, 616 fault points, 1047 jobs re-run, 121 zombie writes refused
clean runs, harness v1 (superseded): 6 runs, 6 PASS, 0 not PASS; 41500 jobs, 187 worker kills, 44 freezes, 0 API kills, 0 DB kills, 0 fault points, 32 jobs re-run, not recorded: zombie writes refused
negative controls, all versions: 10 runs, 0 PASS, 10 not PASS; 58000 jobs, 693 worker kills, 64 freezes, 18 API kills, 0 DB kills, 90 fault points, 3950 jobs re-run, 1976 zombie writes refused
