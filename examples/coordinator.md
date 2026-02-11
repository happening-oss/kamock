# Coordinator

By default, the mock broker supports a single group member, which is elected as the leader.

As an alternative, it also provides an experimental group coordinator.

```erlang
{ok, Broker} = kamock_broker:start(make_ref(), #{port => 9990}).
{ok, Coordinator} = kamock_coordinator:start(make_ref()).
meck:new(kamock_join_group, [passthrough]).
meck:new(kamock_sync_group, [passthrough]).
meck:new(kamock_leave_group, [passthrough]).
meck:new(kamock_heartbeat, [passthrough]).
meck:expect(kamock_join_group, handle_join_group_request, kamock_coordinator:join_group(Coordinator)).
meck:expect(kamock_sync_group, handle_sync_group_request, kamock_coordinator:sync_group(Coordinator)).
meck:expect(kamock_leave_group, handle_leave_group_request, kamock_coordinator:leave_group(Coordinator)).
meck:expect(kamock_heartbeat, handle_heartbeat_request, kamock_coordinator:heartbeat(Coordinator)).
```

Run kcat as many times as you want:

```sh
kcat -b localhost:9990 -G group cars
```

If you want more control over the group membership then look at the examples for [JoinGroup](join-group.md) and
[SyncGroup](sync-group.md) instead of using `kamock_coordinator`.

## Multiple groups

The mock group coordinator doesn't pay any attention to the group name.

This is deliberate: kafire only allows one consumer for each group ID within the same process, so if you want to have
multiple consumers for the same group in the same test, you'd be out of luck.

Instead, your tests can use different group IDs, and -- because kamock ignores them -- you'll get the desired behaviour
in your tests.

If you _actually_ want the groups to be unique, you can do something like this (see `test/kamock_coordinator_multiple_groups_tests.erl`):

```erlang
{ok, Coordinator1} = kamock_coordinator:start(make_ref()).
{ok, Coordinator2} = kamock_coordinator:start(make_ref()).

% ...

IsGroupId = fun(G_) -> fun(#{group_id := G}) -> G =:= G_ end end.
meck:expect(kamock_join_group, handle_join_group_request, [
    {[meck:is(IsGroupId(Group1)), '_'], kamock_coordinator:join_group(Coordinator1)},
    {[meck:is(IsGroupId(Group1)), '_'], kamock_coordinator:join_group(Coordinator2)}
]).

% ... and so on for SyncGroup, LeaveGroup, and Heartbeat.
```
