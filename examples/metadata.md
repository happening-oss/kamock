# Example: Metadata

## Available Topics

By default, the mock broker responds to `Fetch` requests for any topic name, but the topics don't appear in `Metadata`
responses unless specifically named.

For this example, we'll use the mock cluster:

```erlang
kamock_cluster:start(make_ref(), [101, 102, 103], #{port => 9292}).
```

So `kcat -L` won't show any topics:

```
$ kcat -b localhost:9292 -L
Metadata for all topics (from broker 101: localhost:9292/101):
 3 brokers:
  broker 101 at localhost:9292 (controller)
  broker 102 at localhost:50627
  broker 103 at localhost:50628
 0 topics:
```

If you specify a topic, it will appear:

```
$ kcat -b localhost:9292 -L -t cars
Metadata for cars (from broker 101: localhost:9292/101):
 3 brokers:
  broker 101 at localhost:9292
  broker 102 at localhost:50753
  broker 103 at localhost:50754
 1 topics:
  topic "cars" with 4 partitions:
    partition 0, leader 101, replicas: 101,102,103, isrs: 101,102,103
    partition 1, leader 102, replicas: 102,101,103, isrs: 102,101,103
    partition 2, leader 103, replicas: 103,101,102, isrs: 103,101,102
    partition 3, leader 101, replicas: 101,102,103, isrs: 101,102,103
```

If you want to force topics to appear in the metadata, you can do the following:

```erlang
meck:expect(kamock_metadata, handle_metadata_request,
    kamock_metadata:with_topics([<<"cats">>, <<"dogs">>])
).
```

And then topics named "cats" and "dogs" appear in the `Metadata` response:

```
% kcat -b localhost:9292 -L
Metadata for all topics (from broker 101: localhost:9292/101):
 3 brokers:
  ...
 2 topics:
  topic "cats" with 4 partitions:
    partition 0, leader 101, replicas: 101,102,103, isrs: 101,102,103
    ...
  topic "dogs" with 4 partitions:
    partition 0, leader 101, replicas: 101,102,103, isrs: 101,102,103
    ...
```

## Number of Partitions

By default, topics have 4 partitions, numbered from 0-3. You can change the number of partitions reported for all topics
as follows:

```erlang
meck:new(kamock_metadata_response_topic, [passthrough]).

meck:expect(kamock_metadata_response_topic, make_metadata_response_topic,
  kamock_metadata_response_topic:partitions(lists:seq(0, 63))).
```
