# Example: SyncGroup

By default, the mock broker supports a single group member, who is elected as the leader.

For basic examples, including how to assign topics and partitions to members, start with
the [JoinGroup examples](join-group.md).

## Wait for members

If your test has multiple group members, consider using the `wait_for_members/2` function.

It blocks all the followers in `SyncGroup` until the leader sends a `SyncGroup` with member assignments.

```erlang
LeaderId = <<"leader">>,
FollowerId1 = <<"follower1">>,
FollowerId2 = <<"follower2">>,

meck:expect(
    kamock_sync_group,
    handle_sync_group_request,
    kamock_sync_group:wait_for_members(LeaderId, [FollowerId1, FollowerId2])
).
```

## Errors

You can return an error like so:

```erlang
meck:expect(kamock_sync_group, handle_sync_group_request, kamock_sync_group:return_error(?NOT_COORDINATOR)).
```

Or to correctly implement NOT_COORDINATOR behaviour:

```erlang
meck:expect(
    kamock_sync_group,
    handle_sync_group_request,
    [
        {['_', #{node_id => CoordinatorNodeId}], meck:passthrough()},
        {['_', '_'], kamock_sync_group:return_error(?NOT_COORDINATOR)}
    ]
).
```
