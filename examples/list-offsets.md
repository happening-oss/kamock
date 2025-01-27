# Example: ListOffsets

If you want to mock a fixed-length partition that starts at a non-zero offset, you can mock the partition data with the
following (also see [fetch.md](fetch.md)):

```erlang
% Start the mock broker, as usual.
kamock_broker:start(make_ref(), #{port => 9292}).
```

## Default

The default mock broker behaviour is to pretend that every partition starts at zero and has a latest offset of zero.
That is: it's brand-new and empty.

## Non-zero log start

```erlang
meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response,
    kamock_list_offsets_partition_response:range(11662, 11668)).
```

```erlang
MessageBuilder = fun(_Topic, Partition, Offset) ->
    Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
    Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
    #{key => Key, value => Value}
end.

meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:range(11662, 11668, MessageBuilder)).
```

## Non-zero log start, empty

Set the first offset and last offset equal:

```erlang
meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response,
    kamock_list_offsets_partition_response:range(12576, 12576)).
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:range(12576, 12576, fun(_, _, _) -> error(unexpected) end)).
```

```
$ kcat -b localhost:9292 -C -t cars -f "%t [%p] at offset %o: %k = %s\n" -e
% Reached end of topic cars [0] at offset 12576
% Reached end of topic cars [1] at offset 12576
% Reached end of topic cars [2] at offset 12576
% Reached end of topic cars [3] at offset 12576: exiting
```

## Fetch Latest

If you use `kcat ... -o end`, it will send a `ListOffsets` request, asking for the latest offset. This is the offset one
past the end.

This partition has messages with offsets 0, 1, 2:

```
+--+--+--+..+
| 0| 1| 2|  :
+--+--+--+..+
           ^
           3
```

To do this with the mock broker, you'll need to use the following:

```erlang
meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response,
    kamock_list_offsets_partition_response:range(11662, 11668)).

% MessageBuilder is from above.
meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:repeat(MessageBuilder)).
```

The mock broker does _not_ keep track of the last offset "produced" by default, so if you restart a client, it'll start
from the mocked last offset passed to `range/2`.

## Using kcat with negative offsets

If you've got a "snapshot" topic, you want to get the last message. You can use `kcat -o -1` to do that.

```erlang
% We'll use this matcher for both ListOffsets and Fetch; the requests have different fields. We'll cope.
IsPartition = fun(P) ->
    IsPartition_ = fun(P) ->
        fun
            (_FetchPartition = #{partition_index := PartitionIndex}) -> PartitionIndex == P;
            (_ListOffsetsPartition = #{partition := Partition}) -> Partition == P;
            (_) -> false
        end
    end,
    meck:is(IsPartition_(P))
end.
```

```erlang
meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response, [
    {['_', IsPartition(0), '_'], kamock_list_offsets_partition_response:range(30258, 30260)},
    {['_', IsPartition(1), '_'], kamock_list_offsets_partition_response:range(4849, 4849)},
    {['_', IsPartition(2), '_'], kamock_list_offsets_partition_response:range(24391, 24400)},
    {['_', '_', '_'], kamock_list_offsets_partition_response:range(18915, 18919)}
]).
```

```erlang
% MessageBuilder is from above.
meck:expect(kamock_partition_data, make_partition_data, [
    {['_', IsPartition(0), '_'], kamock_partition_data:range(30258, 30260, MessageBuilder)},
    {['_', IsPartition(1), '_'], kamock_partition_data:range(4849, 4849, MessageBuilder)},
    {['_', IsPartition(2), '_'], kamock_partition_data:range(24391, 24400, MessageBuilder)},
    {['_', '_', '_'], kamock_partition_data:range(18915, 18919, MessageBuilder)}
]).
```

Nothing in the mock broker forces you to use matching ranges here. In fact, if you know your client won't try to read
outside the lines, you could just use `kamock_partition_data:repeat(MessageBuilder)`.

```
$ kcat -b localhost:9292 -C -t cars -f "%t [%p] at offset %o: %k = %s\n" -o -1 -e
cars [0] at offset 30259: key-0-30259 = value-0-30259
%4|1727425365.711|OFFSET|rdkafka#consumer-1| [thrd:main]: cars [1]: offset reset (at offset 4848 (leader epoch 0), broker 101) to offset END (leader epoch -1): fetch failed due to requested offset not available on the broker: Broker: Offset out of range
% Reached end of topic cars [0] at offset 30260
cars [2] at offset 24399: key-2-24399 = value-2-24399
cars [3] at offset 18918: key-3-18918 = value-3-18918
% Reached end of topic cars [2] at offset 24400
% Reached end of topic cars [3] at offset 18919
% Reached end of topic cars [1] at offset 4849: exiting
```

Note that, because one of the partitions is empty (first offset equal to last offset), we get an 'Offset out of range'
error.
