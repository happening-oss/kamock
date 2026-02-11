# Example: SyncGroup

For basic examples, start with the [JoinGroup examples](join-group.md).

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
