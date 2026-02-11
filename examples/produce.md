# Example: Produce

By default, the mock broker handles `Produce` requests as if they were always the first produce to an empty topic.

If, instead of producing to `/dev/null`, you'd prefer to produce to an ETS table; take a look at [ets.md](ets.md).

## Wrong leader?

If you want to fake a produce to a broker that's not the leader for the partition, do something like this:

```erlang
meck:expect(
    kamock_partition_produce_response,
    make_partition_produce_response,
    [
        {
            ['_', meck:is(fun(#{index := P}) -> P =:= 0 end), '_'],
            kamock_partition_produce_response:return_error(?NOT_LEADER_OR_FOLLOWER)
        },
        {['_', '_', '_'], meck:passthrough()}
    ]
),
```
