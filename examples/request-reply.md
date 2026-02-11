# Request/Reply Pattern

This example builds on the [simple consumer](simple-consumer.md) and [simple producer](simple-producer.md)
implementation and demonstrates how you might use the mock broker to test a request/reply message pattern.

We start with ETS-backed topics:

```erlang
{ok, Broker} = kamock_broker:start(make_ref(), #{port => 9990}).

Table = ets:new(kamock, [public, ordered_set, named_table]).
meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)).
meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)).
meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)).
```

Then we need a simple consumer. This one repeats the request back, via a different topic:

```erlang
Broker = #{host => <<"localhost">>, port => 9990}.

RequestTopic = <<"requests">>.
Partitions = [0, 1, 2, 3].
ReplyTopic = <<"replies">>.

Repeater =
    fun(Topic, Partition, Request) ->
        io:format("~s [~B]: ~p~n", [Topic, Partition, Request]),
        % We don't do anything with the request, just repeat it back.
        Reply = Request,
        ok = kamock_simple_producer:produce(Broker, ReplyTopic, Partition, Reply),
    end.

{ok, Consumer} = kamock_simple_consumer:start_link(Broker, RequestTopic, Partitions, Repeater).
```

Then, with `kcat`, we could do something like this:

```sh
# listen for replies
kcat -b localhost:9099 -C -t replies
```

```sh
# send a request
echo 'Hello World' | kcat -b localhost:9099 -P -t requests
```

Note that there's about 250 ms latency; this is due to the polling interval of `kamock_simple_consumer`.
