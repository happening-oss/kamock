# Example: Fetch

## Defaults to empty

By default, the mock broker responds to a Fetch for any topic, any partition as if it's a newly-created,
completely-empty partition.

```erlang
% Start the mock broker, as usual.
kamock_broker:start(make_ref(), #{port => 9990}).
```

```
$ kcat -b localhost:9990 -C -t cars -e
% Reached end of topic cars [0] at offset 0
% Reached end of topic cars [1] at offset 0
% Reached end of topic cars [2] at offset 0
% Reached end of topic cars [3] at offset 0: exiting
```

## Explicitly empty

```erlang
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:empty()).
```

## Single message

```erlang
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:single(#{key => <<"key">>, value => <<"value">>})).
```

## Infinitely long partition

To mock an infinitely-long partition, where each message is retrieved one at a time, you can do the following:

```erlang
MessageBuilder = fun(_Topic, Partition, Offset) ->
    Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
    Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
    #{key => Key, value => Value}
end,
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:repeat(MessageBuilder)).
```

```
$ kcat -b localhost:9990 -C -t cars
value-0-0
value-1-0
value-1-1
value-2-0
...
```

## Fixed-length partition

To mock a fixed-length partition, you can use `kamock_partition_data:range/3`, as follows:

```erlang
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:range(0, 10, MessageBuilder)).
```

If you want to mock different lengths for different topics and partitions, you can make use of `meck:is/1` as follows:

```erlang
meck:expect(kamock_partition_data, make_partition_data, [
    {[<<"cars">>, meck:is(fun(#{partition := P}) -> P div 2 == 0 end), '_'],
        kamock_partition_data:range(0, 3, MessageBuilder)},
    {[<<"cars">>, '_', '_'],
        kamock_partition_data:range(0, 4, MessageBuilder)},
    {['_', '_', '_'],
        kamock_partition_data:range(0, 5, MessageBuilder)}
]).
```

## Batches

By default, we return single-record batches. To mock records in batches:

```erlang
MessageBuilder = fun(_Topic, Partition, Offset) ->
    Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
    Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
    #{key => Key, value => Value}
end,
% 1000 messages, in batches of 7.
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:batches(0, 1_000, 7, MessageBuilder)).
```

You can have batches of different sizes by providing a "batch locator function" instead of a batch size:

```erlang
BatchLocator = fun(FirstOffset, LastOffset, FetchOffset) when
    FetchOffset >= FirstOffset, FetchOffset < LastOffset
->
    % TODO: Which batch does 'FetchOffset' land in?
    % TODO: Figure out the base offset of that batch and the batch size.

    % Note: we don't have to clamp the last batch to the end of the partition; that's done for us.
    {BatchOffset, BatchSize}
end.
```

Implementation details left as an exercise for the reader.

The default batch locator implements fixed size batches. We also have one that "round-robins" through batch sizes:

```erlang
% The first batch has 3 messages, the second has 6, then 1, then 7, then back to 3 again, and so on...
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:batches(0, 1_000, kamock_batch_locator:round_robin([3, 6, 1, 7]), MessageBuilder)).
```

## Non-zero offset

If you want to mock a fixed-length partition that starts at a non-zero offset, you can use
`kamock_partition_data:range(11662, 11668, MessageBuilder)`. You will also need to mock the `ListOffsets` call; see
[list-offsets.md](list-offsets.md).

## Topic not found

If you want to return `UNKNOWN_TOPIC_OR_PARTITION`, then you need to replace the default behaviour.

```erlang
meck:expect(kamock_partition_data, make_partition_data,
    fun(_Topic, #{partition := P}, _Env) ->
        kamock_partition_data:make_error(P, 3)
    end).
```

## Top-level error

To return a top-level error code in the Fetch response (e.g. `CORRUPT_MESSAGE`), use
`kamock_fetch:return_error/1`:

```erlang
meck:expect(kamock_fetch, handle_fetch_request,
    kamock_fetch:return_error(?CORRUPT_MESSAGE)).
```

This returns an empty response with the given error code and resets the session ID to zero, which causes
the client to start a new fetch session.
