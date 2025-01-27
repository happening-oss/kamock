# Example: JoinGroup

## Defaults

By default, the mock broker supports a single group member, who is elected as the leader. If you want to be explicit
about it, you can do the following:

```erlang
meck:expect(
    kamock_join_group,
    handle_join_group_request,
    kamock_join_group:as_leader()
).
```

## As follower

It's often simpler for unit tests if everyone is a follower:

```erlang
meck:expect(
    kamock_join_group,
    handle_join_group_request,
    kamock_join_group:as_follower()
).
```

If you don't specify a `LeaderId` (use `kamock_join_group:as_follower/0`), the mock broker will generate one for you.

You also need to mock out `SyncGroup`:

```erlang
meck:expect(
    kamock_sync_group,
    handle_sync_group_request,
    kamock_sync_group:assign([#{topic => <<"cars">>, partitions => [0, 1, 2, 3]}])).
```

If you want two group members with different assignments, you can do this:

```erlang
meck:expect(
    kamock_sync_group,
    handle_sync_group_request,
    ['_', '_'],
    meck:seq([
        kamock_sync_group:assign([#{topic => <<"cars">>, partitions => [0, 1]}]),
        kamock_sync_group:assign([#{topic => <<"cars">>, partitions => [2, 3]}])
    ])
).
```

Note that this only supports two group members (because we only provide two assignments). The use of `meck:seq` also
means that any group member joining the group later (or leaving and re-joining) will get partitions `[2, 3]` (because
`meck:seq` repeats the last entry).

## Group IDs

The mock broker doesn't care about the group name. For example, here's `kcat` joining two different groups (using the
assignments above):

```
$ kcat -b localhost:9292 -G foo cars
% Waiting for group rebalance
% Group foo rebalanced (memberid rdkafka-837c298c-6291-41fe-a936-34cafaba9351): assigned: cars [0], cars [1]
% Reached end of topic cars [0] at offset 0
% Reached end of topic cars [1] at offset 0
```

```
$ kcat -b localhost:9292 -G bar cars
% Waiting for group rebalance
% Group bar rebalanced (memberid rdkafka-4437c8da-3e30-47d3-bad8-ff69d314e283): assigned: cars [2], cars [3]
% Reached end of topic cars [2] at offset 0
% Reached end of topic cars [3] at offset 0
```

## Assigning member IDs explicitly

It's possible (though verbose) to assign member IDs explicitly.

```erlang
meck:expect(kamock_join_group, handle_join_group_request, [
    {
        [meck:is(fun(#{member_id := MemberId}) -> MemberId == <<>> end), '_'],
        meck:loop([
            fun(#{correlation_id := CorrelationId}, _) ->
                kamock_join_group:member_id_required(CorrelationId, MemberId)
            end
            || MemberId <- [<<"member1">>, <<"member2">>]
        ])
    },
    {['_', '_'], meck:passthrough()}
]).
```

Then you can match on the member ID later. Something like this:

```erlang
meck:expect(kamock_sync_group, handle_sync_group_request, [
    {
        [meck:is(fun(#{member_id := M}) -> M == <<"member1">> end), '_'],
        kamock_sync_group:assign([#{topic => <<"cars">>, partitions => [0, 1]}])
    },
    {
        [meck:is(fun(#{member_id := M}) -> M == <<"member2">> end), '_'],
        kamock_sync_group:assign([#{topic => <<"cars">>, partitions => [2, 3]}])
    }
]).
```
