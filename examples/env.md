# The Env variable

We pass this thing, `Env` around. What's it for?

It's usually (but we missed some) passed all of the way from the Kafka protocol handler down the implementation tree.
This means that your mock function can access it.

One reason you might want to do that is, if you're enforcing node affinity. For example, here's a way to ensure that
you're only allowing produces to the partition leader:

```erlang
    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        fun(
            Topic,
            PartitionProduceData = #{index := PartitionIndex},
            Env = #{node_id := NodeId, node_ids := NodeIds}
        ) ->
            LeaderId = kamock_metadata_response_partition:get_partition_leader(PartitionIndex, NodeIds),
            case LeaderId of
                NodeId ->
                    meck:passthrough([Topic, PartitionProduceData, Env]);
                _ ->
                    kamock_partition_produce_response:make_error_response(
                        PartitionIndex, ?NOT_LEADER_OR_FOLLOWER
                    )
            end
        end.
    ),
```

By default, `Env` contains the following:

- `ref`: the `Ref` you passed to `kamock_broker:start()` or `kamock_cluster:start()`.
- `host`, `port`: the host and port of the mock broker handling the request; use `node_id` instead.
- `node_id`: the node ID of the mock broker handling the request.
- `node_ids`: the list of node IDs in the mock cluster. Contains a single ID if you used `kamock_broker:start()`.
- `controller_id`: the node ID of the controller; defaults to the first ID in `node_ids`; used in `Metadata` and
  `DescribeCluster`.
- `cluster_id`: the cluster ID.
- `cluster`: the PID of the cluster registry; ignore it.

You can override most of the above settings by passing them in `Options` when calling `kamock_broker:start()` or
`kamock_cluster:start()`.

To add custom entries, add an `env` key to `Options`:

```erlang
Env = #{
    % ...
}.
{ok, Broker} = kamock_broker:start(Ref, #{env => Env}).
```
