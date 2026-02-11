-module(kamock_coordinator_multiple_groups_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(COORDINATOR_REF_1, {?MODULE, ?FUNCTION_NAME}).
-define(COORDINATOR_REF_2, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID_1, iolist_to_binary(io_lib:format("~s___g_1", [?MODULE]))).
-define(GROUP_ID_2, iolist_to_binary(io_lib:format("~s___g_2", [?MODULE]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

% Significantly shorter than the defaults; too short for a real broker.
-define(MEMBER_OPTIONS, #{
    rebalance_timeout_ms => 450,
    session_timeout_ms => 900
}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {with, [fun members_join_different_groups/1]}
    ]}.

setup() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % By default, kamock ignores the group ID. This is deliberate; see `coordinator.md`. If you want one coordinator per
    % group ID, then you can steer JoinGroup, SyncGroup, etc. to the correct coordinator as follows:
    {ok, Coordinator1} = kamock_coordinator:start(?COORDINATOR_REF_1, #{
        initial_rebalance_delay_ms => 500
    }),
    {ok, Coordinator2} = kamock_coordinator:start(?COORDINATOR_REF_2, #{
        initial_rebalance_delay_ms => 500
    }),

    IsGroupId = fun(GroupId) -> fun(#{group_id := G}) -> G =:= GroupId end end,
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        [
            {[meck:is(IsGroupId(?GROUP_ID_1)), '_'], kamock_coordinator:join_group(Coordinator1)},
            {[meck:is(IsGroupId(?GROUP_ID_2)), '_'], kamock_coordinator:join_group(Coordinator2)},
            {['_', '_'], meck:raise(error, unrecognised_group_id)}
        ]
    ),

    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        [
            {[meck:is(IsGroupId(?GROUP_ID_1)), '_'], kamock_coordinator:sync_group(Coordinator1)},
            {[meck:is(IsGroupId(?GROUP_ID_2)), '_'], kamock_coordinator:sync_group(Coordinator2)},
            {['_', '_'], meck:raise(error, unrecognised_group_id)}
        ]
    ),
    meck:expect(
        kamock_leave_group,
        handle_leave_group_request,
        [
            {[meck:is(IsGroupId(?GROUP_ID_1)), '_'], kamock_coordinator:leave_group(Coordinator1)},
            {[meck:is(IsGroupId(?GROUP_ID_2)), '_'], kamock_coordinator:leave_group(Coordinator2)},
            {['_', '_'], meck:raise(error, unrecognised_group_id)}
        ]
    ),
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        [
            {[meck:is(IsGroupId(?GROUP_ID_1)), '_'], kamock_coordinator:heartbeat(Coordinator1)},
            {[meck:is(IsGroupId(?GROUP_ID_2)), '_'], kamock_coordinator:heartbeat(Coordinator2)},
            {['_', '_'], meck:raise(error, unrecognised_group_id)}
        ]
    ),
    {Broker, Coordinator1, Coordinator2}.

cleanup({Broker, Coordinator1, Coordinator2}) ->
    kamock_coordinator:stop(Coordinator1),
    kamock_coordinator:stop(Coordinator2),
    kamock_broker:stop(Broker),
    meck:unload(),
    ok.

members_join_different_groups({Broker, _, _}) ->
    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID_1, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID_2, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members have joined. They should both be leaders, because they're in different groups.
    receive
        {kamock_test_member, M1, {sync_group, leader, _}} -> ok
    end,
    receive
        {kamock_test_member, M2, {sync_group, leader, _}} -> ok
    end,

    kamock_test_member:stop(M1),
    kamock_test_member:stop(M2),
    ok.
