# Example: OffsetFetch

By default, for group consumers, kamock responds with zero for the committed offset for any group, topic and partition.

To change this, do the following:

```erlang
% Start the mock broker, as usual.
kamock_broker:start(make_ref(), #{port => 9292}).
```

```erlang
meck:expect(kamock_offset_fetch_response_partition, make_offset_fetch_response_partition,
    fun(_T, PartitionIndex, _Env) ->
        CommittedOffset = 27317,
        #{
            partition_index => PartitionIndex,
            committed_offset => CommittedOffset,
            metadata => <<>>,
            error_code => 0
        }
    end).
```

Note that the above only works in compiled code, because it needs the `?NONE` macro. In the REPL, use `error_code => 0`.

Then, when you use `kcat`, for example:

```
$ kcat -b localhost:9292 -G group cars
```

...it will start fetching from offset 27317, rather than zero. To see any output, you'll also need to mock the `Fetch`
request; see [fetch.md](fetch.md) for details.

```
% Waiting for group rebalance
% Group group rebalanced (memberid rdkafka-f36c1aae-6660-4642-8b12-a5be43c97d5e): assigned: cars [0], cars [1], cars [2], cars [3]
value-0-27317
value-1-27317
value-2-27317
value-3-27317
...
```

Note that `OffsetCommit` requests are silently ignored, so if you restart the client, it will start from the beginning
again. See [offset-commit.md](offset-commit.md) for details.
