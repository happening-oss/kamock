# Simple Producer

Similar to the motivation for [Simple Consumer](simple-consumer.md), sometimes you want to produce a message to the mock
broker from your tests.

To do this, use `kamock_simple_producer:produce/4`, as follows. The example uses ETS-backed topics.

```erlang
{ok, Broker} = kamock_broker:start(make_ref(), #{port => 9990}).

Table = ets:new(kamock, [public, ordered_set, named_table]).
meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)).
meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)).
meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)).
```

```erlang
Topic = <<"example">>.
Partition = 0.
Message = #{value => <<"Hello World!">>}.
ok = kamock_simple_producer:produce(Broker, Topic, Partition, Message).
```

Note that the `Produce` request is sent directly to the specified broker. If it's not the leader for the partition,
you'll get an error. With `kamock`'s default implementation, this doesn't matter -- there's no real partition leaders.
