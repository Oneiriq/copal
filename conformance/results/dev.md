# Conformance results: Copal dev

Produced by `conformance/run.sh`: the stack in
`docker-compose.yml`, one scenario per client, every check
named. Run the same command to reproduce the table.

## aws: 7/7

| check | result |
| --- | --- |
| sync_in | PASS |
| listing_complete | PASS |
| head_last_modified | PASS |
| multipart_roundtrip_digest | PASS |
| abort_leaves_no_ghost | PASS |
| sync_noop | PASS |
| delete_propagates | PASS |

## mc: 6/8

| check | result |
| --- | --- |
| bucket_reachable | PASS |
| mirror_full | PASS |
| listing_complete | PASS |
| second_pass_noop | PASS |
| diff_empty | FAIL |
| stat_readable | PASS |
| multipart_roundtrip_digest | FAIL |
| delete_propagates | PASS |

## rclone: 5/5

| check | result |
| --- | --- |
| sync_in | PASS |
| check_sizes | PASS |
| roundtrip_digest | PASS |
| sync_noop | PASS |
| delete_propagates | PASS |
