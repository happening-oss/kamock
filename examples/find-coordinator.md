# Example: FindCoordinator

When a consumer wants to join a consumer group, it first needs to find the coordinator for that group.

By default, the mock broker provides a very simple consumer group implementation:

- It replies with itself as the coordinator for any request.
- It supports only a single group member, and that member is always the leader.
- It responds with offset zero to `OffsetFetch` requests. See [offset-fetch.md](offset-fetch.md).
- It silently ignores `OffsetCommit` messages. See [offset-commit.md](offset-commit.md).

To that first point, you might want to test whether your code follows coordinator moves correctly.

## Mocking FindCoordinator

The following example makes the _second_ broker in the cluster be the coordinator, rather than every broker claim that
it's the coordinator:

```erlang
{ok, Cluster, [Bootstrap, Coordinator | _]} = kamock_cluster:start(make_ref(), [101, 102, 103]),

meck:new(kamock_find_coordinator, [passthrough]),
meck:expect(kamock_find_coordinator, handle_find_coordinator_request,
    fun(_Req = #{correlation_id := CorrelationId}, _Env) ->
        kamock_find_coordinator:make_find_coordinator_response(CorrelationId, Coordinator)
    end
).
```

Or you could spread the coordinators across the cluster:

```erlang
{ok, Cluster, Brokers} = kamock_cluster:start(make_ref(), [101, 102, 103]),

meck:new(kamock_find_coordinator, [passthrough]),
meck:expect(kamock_find_coordinator, handle_find_coordinator_request,
    fun(_Req = #{correlation_id := CorrelationId, key := Key}, _Env) ->
        N = (erlang:phash2(Key) rem length(NodeIds) + 1),
        Coordinator = lists:nth(N, Brokers),
        kamock_find_coordinator:make_find_coordinator_response(CorrelationId, Coordinator)
    end
).
```
