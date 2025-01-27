# Example: Fetch

## Defaults to empty

By default, the mock broker responds to a Fetch for any topic, any partition as if it's a newly-created,
completely-empty partition.

```erlang
% Start the mock broker, as usual.
kamock_broker:start(make_ref(), #{port => 9292}).
```

```
$ kcat -b localhost:9292 -C -t cars -e
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
$ kcat -b localhost:9292 -C -t cars
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

## Non-zero offset

If you want to mock a fixed-length partition that starts at a non-zero offset, you can use
`kamock_partition_data:range(11662, 11668, MessageBuilder)`. You will also need to mock the `ListOffsets` call; see
[list-offsets.md](list-offsets.md).
