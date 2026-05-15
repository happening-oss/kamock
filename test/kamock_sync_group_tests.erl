-module(kamock_sync_group_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun return_error/0,
        fun wait_for_members/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

return_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    MemberId = <<"some_member_id">>,

    meck:expect(
        kamock_sync_group, handle_sync_group_request, kamock_sync_group:return_error(12345)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    SyncGroupRequest = make_request(GroupId, MemberId, []),
    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0,
            assignment => <<>>,
            protocol_type => null,
            protocol_name => null
        }},
        kafcod_connection:call(
            Connection,
            fun sync_group_request:encode_sync_group_request_5/1,
            SyncGroupRequest,
            fun sync_group_response:decode_sync_group_response_5/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

make_request(GroupId, MemberId, Assignments) ->
    #{
        group_id => GroupId,
        member_id => MemberId,
        group_instance_id => null,
        generation_id => 0,
        protocol_type => <<"consumer">>,
        protocol_name => <<"range">>,
        assignments => Assignments
    }.

wait_for_members() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    GroupId = <<"group">>,
    LeaderId = <<"leader">>,
    FollowerId1 = <<"follower1">>,
    FollowerId2 = <<"follower2">>,

    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        kamock_sync_group:wait_for_members(LeaderId, [FollowerId1, FollowerId2])
    ),

    Test = self(),
    Follower = fun(FollowerId) ->
        fun() ->
            {ok, F} = kafcod_connection:start_link(Broker),

            % Follower sends SyncGroup; it should be blocked until the leader sends the assignments.
            {ok, SyncGroupResponse} = kafcod_connection:call(
                F,
                fun sync_group_request:encode_sync_group_request_5/1,
                make_request(GroupId, FollowerId, []),
                fun sync_group_response:decode_sync_group_response_5/1
            ),

            #{assignment := Assignment} = SyncGroupResponse,
            Test ! {assignment, FollowerId, kafcod_consumer_protocol:decode_assignment(Assignment)},
            ok
        end
    end,

    % Two followers; just to prove that we _can_.
    {FPid1, FRef1} = spawn_monitor(Follower(FollowerId1)),
    {FPid2, FRef2} = spawn_monitor(Follower(FollowerId2)),

    % Leader (that's us) sends SyncGroup; this should unblock the follower.
    {ok, L} = kafcod_connection:start_link(Broker),

    Assignments = #{
        LeaderId => #{assigned_partitions => #{<<"cars">> => [0, 1]}, user_data => <<>>},
        FollowerId1 => #{assigned_partitions => #{<<"cars">> => [2]}, user_data => <<>>},
        FollowerId2 => #{assigned_partitions => #{<<"cars">> => [3]}, user_data => <<>>}
    },

    {ok, SyncGroupResponse} = kafcod_connection:call(
        L,
        fun sync_group_request:encode_sync_group_request_5/1,
        make_request(GroupId, LeaderId, kafcod_consumer_protocol:encode_assignments(Assignments)),
        fun sync_group_response:decode_sync_group_response_5/1
    ),

    % Assert that the leader (us) got the correct assignment.
    #{assignment := Assignment} = SyncGroupResponse,
    ?assertMatch(
        #{assigned_partitions := #{<<"cars">> := [0, 1]}, user_data := <<>>},
        kafcod_consumer_protocol:decode_assignment(Assignment)
    ),

    % Assert the followers got the correct assignments.
    receive
        {assignment, FollowerId1, Assignment1} ->
            ?assertMatch(
                #{assigned_partitions := #{<<"cars">> := [2]}, user_data := <<>>}, Assignment1
            )
    end,
    receive
        {assignment, FollowerId2, Assignment2} ->
            ?assertMatch(
                #{assigned_partitions := #{<<"cars">> := [3]}, user_data := <<>>}, Assignment2
            )
    end,

    % Wait for the followers to exit.
    receive
        {'DOWN', FRef1, process, FPid1, normal} -> ok
    end,
    receive
        {'DOWN', FRef2, process, FPid2, normal} -> ok
    end,

    kamock_broker:stop(Broker),
    ok.
