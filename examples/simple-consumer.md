# Simple Consumer

## Motivation

For some tests, it's sufficient to [mock `Produce`](produce.md) and wait for the message, or to use [ETS-backed
topics](ets.md).

Sometimes, however, you want to test something that uses a request-reply strategy. This means that you have to mock the
`Produce` to catch the request and also mock the `Fetch`, so that you can fake the correct reply.

This is, frankly, too complicated.

Instead, you could use a _real_ client implementation (still with `kamock`), but that means bringing in another
dependency. To avoid that, `kamock` provides some really simple implementations for consuming and producing messages.

## Example

Here's a simple, message-at-a-time, consumer. The example uses ETS-backed topics, so we'll set that up first:

```erlang
{ok, Broker} = kamock_broker:start(make_ref(), #{port => 9990}).

Table = ets:new(kamock, [public, ordered_set, named_table]).
meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)).
meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)).
meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)).
```

Then we can start the simple consumer:

```erlang
Topic = <<"example">>.
Partitions = [0, 1, 2, 3].

Fun =
    fun(Topic, Partition, Message) ->
        io:format("~s [~B]: ~p~n", [Topic, Partition, Message])
    end.
{ok, Consumer} = kamock_simple_consumer:start_link(Broker, Topic, Partitions, Fun).
```

## Caveats

- The simple consumer won't reconnect automatically; it'll just die.
