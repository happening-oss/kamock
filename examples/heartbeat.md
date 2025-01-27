# Example: Heartbeat

## Default

By default, the mock broker just replies to a `Heartbeat` request with no error.

It does not implement any logic beyond that: if your client fails to send a heartbeat, the mock broker doesn't trigger a
group rebalance, for example.

## Rebalance

If you'd like to trigger a single group rebalance, do something like this:

```erlang
meck:expect(kamock_heartbeat, handle_heartbeat_request, ['_', '_'],
    meck:seq([
        fun(Req, Env) ->
            Res = meck:passthrough([Req, Env]),
            Res#{error_code => ?REBALANCE_IN_PROGRESS}      % REBALANCE_IN_PROGRESS = 27
        end,
        fun(Req, Env) ->
            meck:passthrough([Req, Env])
        end
    ])
).
```

## Generation ID

If you have multiple consumers, you need to trigger a group rebalance for all of them. The correct way to do this is to
use the group generation ID.

You'll need to do something like this:

```erlang
meck:expect(kamock_join_group, handle_join_group_request,
    kamock_join_group:as_follower(<<"leader">>, 1)).
```

```erlang
meck:expect(kamock_heartbeat, handle_heartbeat_request,
    kamock_heartbeat:expect_generation_id(1)).
```
