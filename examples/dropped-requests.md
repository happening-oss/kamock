# Dropped Requests

You can do error injection with kamock. Here's an example that drops every tenth Fetch request.

First, we set up the usual stream of single messages. See [fetch.md](fetch.md) for similar examples.

```erlang
kamock_broker:start(make_ref()).

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
        % Match Fetch requests; ?FETCH=1, if you're running this in the Erlang shell.
        {[?FETCH, '_', '_', '_'],
            % Repeatedly:
            meck:loop([
                % 9 successful fetches
                meck:seq([meck:passthrough() || _ <- lists:seq(1, 9)]),
                % 1 that just ... hangs
                meck:exec(fun(_, _, _, _) -> noreply end)
                % then continue
            ])},
        % Match all other requests; pass them through.
        {['_', '_', '_', '_'], meck:passthrough()}
    ]).
```
