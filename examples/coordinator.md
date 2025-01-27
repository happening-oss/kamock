# Coordinator

By default, the mock broker supports a single group member, who is elected as the leader. As an alternative, it also
provides an extremely experimental group coordinator.

```erlang
{ok, Broker} = kamock_broker:start(make_ref(), #{port => 9292}).
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
kcat -b localhost:9292 -G group cars
```
