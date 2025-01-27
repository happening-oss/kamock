# Closed Connections

Similar to [dropped-requests.md](dropped-requests.md), you can cause kamock to drop the connection instead of sending a response.

First, we set up the usual stream of single messages. See [fetch.md](fetch.md) for similar examples.

```erlang
kamock_broker:start(make_ref(), #{port => 9292}).

MessageBuilder = fun(_Topic, Partition, Offset) ->
                 Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
                 Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
                 #{key => Key, value => Value}
             end,

meck:expect(kamock_partition_data, make_partition_data,
    kamock_partition_data:repeat(MessageBuilder)).
```

Then we mock the message handler.

```erlang
meck:new(kamock_broker_handler, [passthrough]).
meck:expect(kamock_broker_handler, handle_request,
    [
        {
            [?FETCH, '_', '_', '_'],
            meck:seq([
                meck:seq([meck:passthrough() || _ <- lists:seq(1, 10)]),
                meck:exec(fun(_, _, _, _) -> {stop, closed} end),
                meck:loop([meck:passthrough()])
            ])
        },
        {['_', '_', '_', '_'], meck:passthrough()}
    ]).
```
