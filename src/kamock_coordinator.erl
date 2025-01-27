-module(kamock_coordinator).
-export([
    start/1,
    start/2,
    stop/1
]).
-export([call/2]).
-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4
]).
-export([
    join_group/1,
    sync_group/1,
    leave_group/1,
    heartbeat/1
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(DEFAULT_INITIAL_REBALANCE_DELAY_MS, 3_000).

% The intention is that you'd have one of these per group. If you need more, use meck:is(), or invent a something that
% sits in front of this one and associates a group name with a coordinator pid (hook find_coordinator to start it,
% e.g.).
-record(state, {
    options,
    generation_id,
    leader_id,
    assignments,
    session_timeout_ms
}).

start(Ref) ->
    start(Ref, default_options()).

start(_Ref, Options) ->
    gen_statem:start(?MODULE, [maps:merge(default_options(), Options)], start_options()).

start_options() -> [].
% start_options() -> [{debug, [trace]}].

default_options() ->
    #{
        initial_rebalance_delay_ms => ?DEFAULT_INITIAL_REBALANCE_DELAY_MS
    }.

stop(Coordinator) ->
    gen_statem:stop(Coordinator).

callback_mode() ->
    [handle_event_function].

init([Options]) ->
    StateData = #state{
        options = Options,
        generation_id = 1,
        leader_id = undefined,
        assignments = [],
        session_timeout_ms = 45_000
    },
    {ok, stable, StateData}.

handle_event(
    {call, From},
    {join_group, JoinGroupRequest = #{session_timeout_ms := SessionTimeoutMs}},
    stable,
    StateData = #state{
        options = #{initial_rebalance_delay_ms := InitialRebalanceDelayMs}
    }
) ->
    ?LOG_DEBUG("join_group"),
    trace(join_group),
    % First one in is the leader.
    Joiner = {From, JoinGroupRequest},
    StateData2 = StateData#state{session_timeout_ms = SessionTimeoutMs},
    {next_state, {joining, [Joiner]}, StateData2, [
        {{timeout, initial_rebalance}, InitialRebalanceDelayMs, initial_rebalance}
    ]};
handle_event({call, From}, {join_group, JoinGroupRequest}, {joining, Joiners}, StateData) ->
    ?LOG_DEBUG("join_group"),
    trace(join_group),
    Joiner = {From, JoinGroupRequest},
    {next_state, {joining, [Joiner | Joiners]}, StateData};
handle_event(
    {timeout, initial_rebalance},
    initial_rebalance,
    {joining, Joiners},
    StateData = #state{generation_id = GenerationId}
) when Joiners =/= [] ->
    ?LOG_DEBUG("rebalance over"),
    trace(initial_rebalance),
    % Rebalance over. Notify everyone.
    GenerationId2 = GenerationId + 1,
    % Naive election: pick the first joiner as leader.
    % Note that a real broker would remember the previous leader and use them again, if they're joining again.
    % * We assume that all members have provided _exactly_ the same list of protocols. *
    [Leader | Followers] = Joiners,
    {_, #{member_id := LeaderId}} = Leader,

    Members = lists:map(
        fun(
            _Joiner =
                {_, #{
                    member_id := MemberId,
                    group_instance_id := GroupInstanceId,
                    protocols := [Protocol | _]
                }}
        ) ->
            #{metadata := Metadata} = Protocol,
            #{
                member_id => MemberId,
                group_instance_id => GroupInstanceId,
                metadata => Metadata
            }
        end,
        Joiners
    ),

    Replies = lists:foldl(
        fun(Follower, Acc) ->
            [make_follower_reply(Follower, LeaderId, GenerationId2) | Acc]
        end,
        [make_leader_reply(Leader, Members, GenerationId2)],
        Followers
    ),
    % TODO: Start timers for each member -- if they don't SyncGroup, we need another rebalance.
    StateData2 = StateData#state{generation_id = GenerationId2, leader_id = LeaderId},
    {next_state, await_sync, StateData2, Replies};
handle_event(
    {timeout, initial_rebalance},
    initial_rebalance,
    {joining, []},
    StateData
) ->
    % Nobody joined; go back to stable.
    trace(initial_rebalance),
    {next_state, stable, StateData};
handle_event(
    {call, _From},
    {sync_group, _SyncGroupRequest = #{member_id := MemberId}},
    await_sync,
    _StateData = #state{leader_id = LeaderId}
) when MemberId =/= LeaderId ->
    ?LOG_DEBUG("sync_group (follower)"),
    trace(sync_group),
    % SyncGroup from a follower. Postpone it until we've got the leader's response.
    {keep_state_and_data, [postpone]};
handle_event(
    {call, _From},
    {sync_group,
        _SyncGroupRequest = #{
            member_id := MemberId,
            generation_id := GenerationId,
            assignments := Assignments
        }},
    await_sync,
    StateData = #state{
        leader_id = LeaderId,
        generation_id = GenerationId
    }
) when MemberId =:= LeaderId ->
    ?LOG_DEBUG("sync_group (leader)"),
    trace(sync_group),
    % SyncGroup from the leader. Save the assignments. Don't reply just yet.
    StateData2 = StateData#state{assignments = Assignments},
    {next_state, stable, StateData2, [postpone]};
handle_event(
    {call, From},
    {sync_group,
        _SyncGroupRequest = #{
            correlation_id := CorrelationId,
            generation_id := GenerationId,
            member_id := MemberId,
            protocol_type := ProtocolType,
            protocol_name := ProtocolName
        }},
    stable,
    _StateData = #state{
        generation_id = GenerationId,
        session_timeout_ms = SessionTimeoutMs,
        assignments = Assignments
    }
) ->
    ?LOG_DEBUG("sync_group (member)"),
    trace(sync_group),
    % If this member doesn't appear in the leader's assignments, return <<>>. Verified with a real broker.
    Assignment =
        case lists:search(fun(#{member_id := M}) -> M == MemberId end, Assignments) of
            {value, #{assignment := A}} -> A;
            false -> <<>>
        end,

    SyncGroupResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,

        assignment => Assignment,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName
    },
    {keep_state_and_data, [
        % Rather than try to find all the places where we should cancel the timers, we'll tag them with the generation
        % ID, so we know they're stale.
        {{timeout, {MemberId, GenerationId}}, SessionTimeoutMs, heartbeat},
        {reply, From, SyncGroupResponse}
    ]};
handle_event(
    {call, From},
    {leave_group,
        _LeaveGroupRequest = #{
            correlation_id := CorrelationId,
            members := [#{member_id := MemberId, group_instance_id := GroupInstanceId}]
        }},
    stable,
    StateData = #state{
        options = #{initial_rebalance_delay_ms := InitialRebalanceDelayMs}
    }
) ->
    ?LOG_DEBUG("leave_group"),
    trace(leave_group),
    LeaveGroupResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        members => [
            #{member_id => MemberId, group_instance_id => GroupInstanceId, error_code => ?NONE}
        ]
    },
    {next_state, {joining, []}, StateData, [
        {{timeout, initial_rebalance}, InitialRebalanceDelayMs, initial_rebalance},
        {reply, From, LeaveGroupResponse}
    ]};
handle_event(
    {call, From},
    {leave_group,
        _LeaveGroupRequest = #{
            correlation_id := CorrelationId,
            members := [#{member_id := MemberId, group_instance_id := GroupInstanceId}]
        }},
    {joining, _},
    StateData
) ->
    ?LOG_DEBUG("leave_group while rebalancing; ignoring it"),
    trace(leave_group),
    LeaveGroupResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        members => [
            #{member_id => MemberId, group_instance_id => GroupInstanceId, error_code => ?NONE}
        ]
    },
    {next_state, {joining, []}, StateData, [
        % we don't start the rebalance timer; we're already rebalancing.
        {reply, From, LeaveGroupResponse}
    ]};
handle_event(
    {call, From},
    {heartbeat,
        _HeartbeatRequest = #{
            correlation_id := CorrelationId,
            group_id := _GroupId,
            member_id := MemberId,
            generation_id := GenerationId
        }},
    stable,
    _StateData = #state{
        generation_id = GenerationId,
        session_timeout_ms = SessionTimeoutMs
    }
) ->
    ?LOG_DEBUG("heartbeat"),
    trace(heartbeat),
    % TODO: What does a real broker do if the generation is wrong, or if the member isn't? We'll just crash the coordinator.
    HeartbeatResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0
    },
    {keep_state_and_data, [
        % Restart the member's timer.
        {{timeout, {MemberId, GenerationId}}, SessionTimeoutMs, heartbeat},
        {reply, From, HeartbeatResponse}
    ]};
handle_event(
    {call, From},
    {heartbeat,
        _HeartbeatRequest = #{
            correlation_id := CorrelationId,
            group_id := _GroupId
        }},
    {joining, _},
    _StateData
) ->
    ?LOG_DEBUG("rebalance in progress"),
    trace(rebalance_in_progress),
    HeartbeatResponse = #{
        correlation_id => CorrelationId,
        error_code => ?REBALANCE_IN_PROGRESS,
        throttle_time_ms => 0
    },
    {keep_state_and_data, [
        {reply, From, HeartbeatResponse}
    ]};
handle_event(
    {timeout, {_MemberId, GenerationId}},
    heartbeat,
    _State,
    StateData = #state{
        options = #{initial_rebalance_delay_ms := InitialRebalanceDelayMs},
        generation_id = GenerationId
    }
) ->
    ?LOG_DEBUG("missed heartbeat"),
    trace(missed_heartbeat),

    % Missed a heartbeat; need to trigger a rebalance.
    {next_state, {joining, []}, StateData, [
        {{timeout, initial_rebalance}, InitialRebalanceDelayMs, initial_rebalance}
    ]};
handle_event(
    {timeout, {_MemberId, _StaleGenerationId}},
    heartbeat,
    _State,
    _StateData
) ->
    % Stale heartbeat timer; ignore it.
    keep_state_and_data.

make_leader_reply({From, JoinGroupRequest}, Members, GenerationId) ->
    #{
        correlation_id := CorrelationId,
        member_id := MemberId,
        protocol_type := ProtocolType,
        protocols := [Protocol | _]
    } = JoinGroupRequest,
    LeaderId = MemberId,
    #{name := ProtocolName} = Protocol,
    JoinGroupResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        generation_id => GenerationId,
        leader => LeaderId,
        member_id => MemberId,
        members => Members,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName
    },
    {reply, From, JoinGroupResponse}.

make_follower_reply({From, JoinGroupRequest}, LeaderId, GenerationId) ->
    #{
        correlation_id := CorrelationId,
        member_id := MemberId,
        protocol_type := ProtocolType,
        protocols := [Protocol | _]
    } = JoinGroupRequest,
    #{name := ProtocolName} = Protocol,
    JoinGroupResponse = #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        generation_id => GenerationId,
        leader => LeaderId,
        member_id => MemberId,
        members => [],
        protocol_type => ProtocolType,
        protocol_name => ProtocolName
    },
    {reply, From, JoinGroupResponse}.

join_group(Coordinator) when is_pid(Coordinator) ->
    fun
        (
            _JoinGroupRequest = #{
                correlation_id := CorrelationId,
                client_id := ClientId,
                member_id := <<>>
            },
            _Env
        ) ->
            GeneratedMemberId = kamock_join_group:generate_member_id(ClientId),
            kamock_join_group:member_id_required(CorrelationId, GeneratedMemberId);
        (JoinGroupRequest, _Env) ->
            kamock_coordinator:call(Coordinator, {join_group, JoinGroupRequest})
    end.

sync_group(Coordinator) when is_pid(Coordinator) ->
    fun(SyncGroupRequest, _Env) ->
        kamock_coordinator:call(Coordinator, {sync_group, SyncGroupRequest})
    end.

leave_group(Coordinator) when is_pid(Coordinator) ->
    fun(LeaveGroupRequest, _Env) ->
        kamock_coordinator:call(Coordinator, {leave_group, LeaveGroupRequest})
    end.

heartbeat(Coordinator) when is_pid(Coordinator) ->
    fun(HeartbeatRequest, _Env) ->
        kamock_coordinator:call(Coordinator, {heartbeat, HeartbeatRequest})
    end.

call(Coordinator, Request) when is_pid(Coordinator) ->
    gen_statem:call(Coordinator, Request).

trace(_Trace) ->
    ok.
