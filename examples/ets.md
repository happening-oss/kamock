# ETS-backed topics

By default, `Produce` requests go into the void. Instead, you can use an ETS table:

```erlang
kamock_broker:start(make_ref(), #{port => 9990}).

ets:new(Table, [public, ordered_set, named_table]).
meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)).
meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)).
meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)).
```

Note that the `min_bytes`, `max_bytes`, `max_wait_ms` fetch options are ignored.

Also note that kamock doesn't (currently) support timestamps in `ListOffsets`.

## Inserting

If you don't want to use `Produce`, you can use `kamock_ets:insert/4`:

```erlang
Message = #{key => <<"key">>, value => <<"value">>}.
kamock_ets:insert(Table, Topic, Partition, Message).
```

## Querying

Use `kamock_ets:lookup/3`, as follows:

```erlang
5> kamock_ets:lookup(Table, Topic, Partition).
[#{attributes => 0,offset => 0,timestamp => 1761750653020,
   value => <<"value1">>,key => <<"key1">>,headers => [],
   offset_delta => 0,timestamp_delta => 0},
 #{attributes => 0,offset => 1,timestamp => 1761750653020,
   value => <<"value2">>,key => <<"key2">>,headers => [],
   offset_delta => 1,timestamp_delta => 0},
 ...]
```

It will return all of the messages for the specified topic and partition, in offset order.

## Table structure

This information is subject to change.

As shown above, your test code is responsible for creating an ETS table, of type `ordered_set`, which will be used to
back the topics and partitions in the mock broker.

### Messages

Messages are stored in batches, as almost exact copies of those in the `Produce` request.

So if you run the following `kcat` command, for example...

```sh
echo -n "key1=value1:key2=value2:key3=value3" | \
    kcat -P -b localhost:9990 -t example -p 0 -D : -K =
```

...then `kcat` will produce a batch containing the 3 messages (`key1`, `key2`, `key3`), and that batch will be stored as
a single record in ETS. It looks like this (cut down slightly):

```
{{<<"example">>,0,0,3},
  #{base_offset => 0,
    records =>
        [#{value => <<"value1">>,key => <<"key1">>,
           headers => [],offset_delta => 0,timestamp_delta => 0},
         #{attributes => 0,value => <<"value2">>,key => <<"key2">>,
           headers => [],offset_delta => 1,timestamp_delta => 0},
         #{attributes => 0,value => <<"value3">>,key => <<"key3">>,
           headers => [],offset_delta => 2,timestamp_delta => 0}],
    max_timestamp => 1761748967869,last_offset_delta => 2,
    base_timestamp => 1761748967869, ...}}
```

The ETS entry is `{{Topic, Partition, BaseOffset, NextOffset}, RecordBatch}`.

We store `NextOffset` for each batch, so that we can use it to easily filter on `FetchOffset`. Consider three batches,
at offsets 0, 4 and 8. If we set `FetchOffset = 6`, we want the last two batches:

```
+-+-+-+-+  +-+-+-+-+  +-+-+-+-+
| [0-4) |  | [4-8) |  | [8-12)|
+-+-+-+-+  +-+-+-+-+  +-+-+-+-+
                ^
```

We can't use `BaseOffset > FetchOffset`, because we'd only fetch the final batch. So we store the `NextOffset` for
convenience, and filter on `FetchOffset < NextOffset` (because `6 < 8` and `6 < 12`). That gets us the correct batches.

Note that we return the entire batch that `FetchOffset` falls within. This is the same behaviour as a real broker; the
client is expected to ignore the unwanted initial records in the batch.

### Offsets

The first message is always at offset zero (we don't delete or compact). The offset for the _next_ message (the high
water mark) is stored as `{{Topic, Partition}, NextOffset}`.
