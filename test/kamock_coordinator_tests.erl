-module(kamock_coordinator_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(COORDINATOR_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

% Significantly shorter than the defaults; too short for a real broker.
-define(MEMBER_OPTIONS, #{
    rebalance_timeout_ms => 450,
    session_timeout_ms => 900
}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {with, [fun single_member_joins/1]},
        {with, [fun multiple_members_join/1]},
        {with, [fun leader_stops_heartbeating/1]},
        {with, [fun follower_leaves_group/1]},
        {with, [fun new_member_joins/1]},
        {with, [fun trigger_rebalance/1]}
    ]}.

setup() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Coordinator} = kamock_coordinator:start(?COORDINATOR_REF, #{
        initial_rebalance_delay_ms => 500
    }),

    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_coordinator:join_group(Coordinator)
    ),
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        kamock_coordinator:sync_group(Coordinator)
    ),
    meck:expect(
        kamock_leave_group,
        handle_leave_group_request,
        kamock_coordinator:leave_group(Coordinator)
    ),
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_coordinator:heartbeat(Coordinator)
    ),
    {Broker, Coordinator}.

cleanup({Broker, Coordinator}) ->
    kamock_coordinator:stop(Coordinator),
    kamock_broker:stop(Broker),
    meck:unload(),
    ok.

single_member_joins({Broker, _}) ->
    {ok, M} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until the member is the leader.
    receive
        {kamock_test_member, M, {sync_group, leader}} -> ok
    end,

    kamock_test_member:stop(M),
    ok.

multiple_members_join({Broker, _}) ->
    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members have joined.
    receive
        {kamock_test_member, _, {sync_group, leader}} -> ok
    end,
    receive
        {kamock_test_member, _, {sync_group, follower}} -> ok
    end,

    kamock_test_member:stop(M1),
    kamock_test_member:stop(M2),
    ok.

leader_stops_heartbeating({Broker, Coordinator}) ->
    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members have joined.
    Leader =
        receive
            {kamock_test_member, L, {sync_group, leader}} -> L
        end,
    Follower =
        receive
            {kamock_test_member, F, {sync_group, follower}} -> F
        end,

    % Leader stops heartbeating.
    unlink(Leader),
    exit(Leader, kill),

    % Wait until the other member rejoins as the leader
    receive
        {kamock_test_member, Follower, {sync_group, leader}} -> ok
    end,

    ?assertMatch({stable, _}, sys:get_state(Coordinator)),

    % Cleanup: stop the other member.
    [kamock_test_member:stop(M) || M <- [M1, M2], M /= L],
    ok.

% TODO: Follower stops heartbeating. Reuse the old leader. Will need > 2 members to prove this.
% TODO: Real coordinator (somehow) waits for previous members to rejoin. Not sure how that works.

follower_leaves_group({Broker, Coordinator}) ->
    % kamock_coordinator calls 'kamock_coordinator:trace/1' at various points; hook that and send those calls here.
    dbg:tracer(process, {
        fun({trace, _Pid, call, {_M, _F, Args}}, Self) ->
            Self ! Args,
            Self
        end,
        self()
    }),
    dbg:tpl(kamock_coordinator, trace, '_', []),
    dbg:p(Coordinator, c),

    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members have joined.
    Leader =
        receive
            {kamock_test_member, L, {sync_group, leader}} -> L
        end,
    Follower =
        receive
            {kamock_test_member, F, {sync_group, follower}} -> F
        end,

    % Follower leaves the group normally.
    ?LOG_DEBUG("stable; stopping follower"),

    % BUG: This isn't actually causing a rebalance. It looks (from logs) like the real broker _does_ bump the generation ID immediately.
    % TODO: Investigate some more; use an easily-recognisable group name, so it's easy to find in the broker logs.
    kamock_test_member:stop(Follower),

    ?LOG_DEBUG("stopped follower"),

    % Wait until the other member rejoins as the leader
    receive
        {kamock_test_member, Leader, {sync_group, leader}} -> ok
    end,

    ?LOG_DEBUG("leader is leader again"),

    % We should see a leave_group, immediately followed by rebalance_in_progress; no missed_heartbeat.
    Flush = flush(),
    ?assert(contains_sublist([[leave_group], [rebalance_in_progress]], Flush)),
    ?assertNot(lists:member([missed_heartbeat], Flush)),

    ?assertMatch({stable, _}, sys:get_state(Coordinator)),

    % Cleanup: stop the other member.
    [kamock_test_member:stop(M) || M <- [M1, M2], M /= L],
    dbg:stop(),
    ok.

new_member_joins({Broker, Coordinator}) ->
    % Member 1 joins the group.
    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until the first member has joined.
    receive
        {kamock_test_member, _, {sync_group, leader}} -> ok
    end,

    % Member 2 joins the group; this should cause a rebalance.
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members rejoin.
    receive
        {kamock_test_member, _, {sync_group, leader}} -> ok
    end,
    receive
        {kamock_test_member, _, {sync_group, follower}} -> ok
    end,

    ?assertMatch(
        {stable, #{generation_id := 3, members := [_ | _]}},
        kamock_coordinator:info(Coordinator)
    ),

    kamock_test_member:stop(M1),
    kamock_test_member:stop(M2),
    ok.

trigger_rebalance({Broker, Coordinator}) ->
    {ok, M1} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),
    {ok, M2} = kamock_test_member:start_link(
        Broker, self(), ?GROUP_ID, [?TOPIC_NAME], ?MEMBER_OPTIONS
    ),

    % Wait until both members have joined.
    receive
        {kamock_test_member, _, {sync_group, leader}} -> ok
    end,
    receive
        {kamock_test_member, _, {sync_group, follower}} -> ok
    end,

    % Trigger a rebalance _without_ joining/leaving.
    kamock_coordinator:rebalance(Coordinator),

    % Wait until both members have rejoined.
    receive
        {kamock_test_member, _, {sync_group, leader}} -> ok
    end,
    receive
        {kamock_test_member, _, {sync_group, follower}} -> ok
    end,

    ?assertMatch(
        {stable, #{generation_id := 4, members := [_, _]}},
        kamock_coordinator:info(Coordinator)
    ),

    kamock_test_member:stop(M1),
    kamock_test_member:stop(M2),
    ok.

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M -> flush([M | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

contains_sublist(Sub, List) ->
    case lists:prefix(Sub, List) of
        true ->
            true;
        _ ->
            contains_sublist(Sub, tl(List))
    end.

% TODO: Wrong generation ID in SyncGroup.
% TODO: Late SyncGroup.
