-module(kamock_test_member).
-export([
    start_link/5,
    stop/1
]).
-export([init/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/coordinator_type.hrl").

% kcat uses 3 seconds; we'll use something shorter.
-define(DEFAULT_HEARTBEAT_INTERVAL_MS, 300).

-define(DEFAULT_REBALANCE_TIMEOUT_MS, 45_000).
-define(DEFAULT_SESSION_TIMEOUT_MS, 90_000).

-record(state, {
    owner,
    connection,
    group_id,
    topics,
    options,
    member_id,
    generation_id
}).

start_link(Broker, Owner, GroupId, Topics, Options) ->
    Options2 = maps:merge(default_options(), Options),
    proc_lib:start_link(?MODULE, init, [[Broker, Owner, GroupId, Topics, Options2]]).

default_options() ->
    #{
        rebalance_timeout_ms => ?DEFAULT_REBALANCE_TIMEOUT_MS,
        session_timeout_ms => ?DEFAULT_SESSION_TIMEOUT_MS
    }.

stop(Pid) ->
    MRef = monitor(process, Pid),
    Pid ! stop,
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end.

init([Broker, Owner, GroupId, Topics, Options]) ->
    process_flag(trap_exit, true),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    StateData = #state{
        owner = Owner,
        connection = Connection,
        group_id = GroupId,
        topics = Topics,
        options = Options,
        member_id = <<>>
    },

    proc_lib:init_ack({ok, self()}),
    find_coordinator(StateData).

find_coordinator(StateData = #state{connection = Connection, group_id = GroupId}) ->
    FindCoordinatorRequest = #{
        key_type => ?COORDINATOR_TYPE_GROUP,
        key => GroupId
    },
    handle_find_coordinator_response(
        kafcod_connection:call(
            Connection,
            fun find_coordinator_request:encode_find_coordinator_request_3/1,
            FindCoordinatorRequest,
            fun find_coordinator_response:decode_find_coordinator_response_3/1
        ),
        StateData
    ).

handle_find_coordinator_response(
    {ok, FindCoordinatorResponse = #{error_code := ?NONE}},
    StateData = #state{connection = Connection}
) ->
    kafcod_connection:stop(Connection),
    Broker = maps:with([host, port], FindCoordinatorResponse),
    {ok, Connection2} = kafcod_connection:start_link(Broker),
    StateData2 = StateData#state{connection = Connection2},
    join_group(StateData2);
handle_find_coordinator_response({ok, #{error_code := _}}, StateData) ->
    timer:sleep(100),
    find_coordinator(StateData).

join_group(
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        topics = Topics,
        member_id = MemberId,
        options = Options
    }
) ->
    ?LOG_DEBUG("~s: joining group", [StateData#state.member_id]),
    JoinGroupRequest = make_join_group_request(GroupId, Topics, MemberId, Options),
    handle_join_group_response(
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest,
            fun join_group_response:decode_join_group_response_7/1
        ),
        StateData#state{member_id = MemberId}
    ).

handle_join_group_response(
    {ok, #{
        error_code := ?MEMBER_ID_REQUIRED,
        member_id := MemberId
    }},
    StateData
) ->
    join_group(StateData#state{member_id = MemberId});
handle_join_group_response(
    {ok, #{
        error_code := ?NONE,
        leader := LeaderId,
        generation_id := GenerationId,
        protocol_type := ProtocolType,
        protocol_name := ProtocolName,
        members := Members
    }},
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        topics = Topics,
        member_id = MemberId
    }
) when LeaderId =:= MemberId ->
    % We're the leader; do some assigning; send SyncGroup.
    ?LOG_DEBUG("~s: leader", [StateData#state.member_id]),
    Assignments = assign(Topics, Members),
    SyncGroupRequest = #{
        group_id => GroupId,
        generation_id => GenerationId,
        member_id => MemberId,
        group_instance_id => null,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName,
        assignments => kafcod_consumer_protocol:encode_assignments(Assignments)
    },
    handle_sync_group_response(
        kafcod_connection:call(
            Connection,
            fun sync_group_request:encode_sync_group_request_5/1,
            SyncGroupRequest,
            fun sync_group_response:decode_sync_group_response_5/1
        ),
        leader,
        StateData#state{generation_id = GenerationId}
    );
handle_join_group_response(
    {ok, #{
        error_code := ?NONE,
        leader := LeaderId,
        generation_id := GenerationId,
        protocol_type := ProtocolType,
        protocol_name := ProtocolName,
        members := []
    }},
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId
    }
) when LeaderId =/= MemberId ->
    ?LOG_DEBUG("~s: follower", [StateData#state.member_id]),
    SyncGroupRequest = #{
        group_id => GroupId,
        generation_id => GenerationId,
        member_id => MemberId,
        group_instance_id => null,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName,
        assignments => []
    },
    handle_sync_group_response(
        kafcod_connection:call(
            Connection,
            fun sync_group_request:encode_sync_group_request_5/1,
            SyncGroupRequest,
            fun sync_group_response:decode_sync_group_response_5/1
        ),
        follower,
        StateData#state{generation_id = GenerationId}
    ).

handle_sync_group_response(
    {ok, #{error_code := ?NONE}},
    Role,
    StateData = #state{owner = Owner}
) ->
    ?LOG_DEBUG("~s: sync_group", [StateData#state.member_id]),
    Owner ! {kamock_test_member, self(), {sync_group, Role}},
    loop(StateData).

loop(StateData = #state{connection = Connection}) ->
    receive
        stop ->
            terminate(StateData);
        {'EXIT', Connection, _} ->
            terminate(StateData#state{connection = undefined})
    after ?DEFAULT_HEARTBEAT_INTERVAL_MS ->
        heartbeat(StateData)
    end.

terminate(
    _StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId
    }
) when is_pid(Connection) ->
    ?LOG_DEBUG("leaving group"),
    LeaveGroupRequest = #{
        group_id => GroupId,
        members => [
            #{
                member_id => MemberId,
                group_instance_id => null
            }
        ]
    },
    kafcod_connection:call(
        Connection,
        fun leave_group_request:encode_leave_group_request_4/1,
        LeaveGroupRequest,
        fun leave_group_response:decode_leave_group_response_4/1
    ),
    kafcod_connection:stop(Connection),
    ok;
terminate(_StateData) ->
    ?LOG_DEBUG("NOT leaving group"),
    ok.

heartbeat(
    StateData = #state{
        connection = Connection,
        group_id = GroupId,
        member_id = MemberId,
        generation_id = GenerationId
    }
) ->
    HeartbeatRequest = #{
        group_id => GroupId,
        generation_id => GenerationId,
        member_id => MemberId,
        group_instance_id => null
    },
    handle_heartbeat_response(
        kafcod_connection:call(
            Connection,
            fun heartbeat_request:encode_heartbeat_request_4/1,
            HeartbeatRequest,
            fun heartbeat_response:decode_heartbeat_response_4/1
        ),
        StateData
    ).

handle_heartbeat_response({ok, #{error_code := ?NONE}}, StateData) ->
    loop(StateData);
handle_heartbeat_response({ok, #{error_code := ?REBALANCE_IN_PROGRESS}}, StateData) ->
    ?LOG_DEBUG("~s: rebalance in progress", [StateData#state.member_id]),
    join_group(StateData).

make_join_group_request(
    GroupId,
    Topics,
    MemberId,
    _Options = #{rebalance_timeout_ms := RebalanceTimeoutMs, session_timeout_ms := SessionTimeoutMs}
) ->
    #{
        group_id => GroupId,
        member_id => MemberId,
        group_instance_id => null,
        protocol_type => <<"consumer">>,
        protocols => [
            #{
                name => <<"range">>,
                metadata => kafcod_consumer_protocol:encode_metadata(#{
                    topics => Topics, user_data => <<>>
                })
            }
        ],
        rebalance_timeout_ms => RebalanceTimeoutMs,
        session_timeout_ms => SessionTimeoutMs
    }.

assign(Topics, Members) ->
    % Naively; give the first member everything; give everyone else nothing.
    [#{member_id := MemberId} | _] = Members,
    AssignedPartitions = lists:foldl(
        fun(Topic, Acc) ->
            PartitionIndexes = [0, 1, 2, 3],
            Acc#{Topic => PartitionIndexes}
        end,
        #{},
        Topics
    ),
    #{MemberId => #{assigned_partitions => AssignedPartitions, user_data => <<>>}}.
