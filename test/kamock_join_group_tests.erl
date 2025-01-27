-module(kamock_join_group_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_2", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun as_leader/0,
        fun as_follower/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

as_leader() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    TopicName1 = ?TOPIC_NAME,
    TopicName2 = ?TOPIC_NAME_2,

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader()
    ),

    % Now for the fun part: we need an *entire* JoinGroup flow.
    {ok, Connection} = kafcod_connection:start_link(Broker),

    JoinGroupRequest1 = #{
        session_timeout_ms => 45_000,
        rebalance_timeout_ms => 90_000,
        protocol_type => <<"consumer">>,
        protocols => [
            #{
                name => <<"range">>,
                metadata => kafcod_consumer_protocol:encode_metadata(#{
                    topics => [TopicName1, TopicName2], user_data => <<>>
                })
            }
        ],
        member_id => <<>>,
        group_instance_id => null,
        group_id => GroupId
    },
    {ok, #{error_code := ?MEMBER_ID_REQUIRED, member_id := MemberId}} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest1,
            fun join_group_response:decode_join_group_response_7/1
        ),
    ?assertNotEqual(<<>>, MemberId),

    JoinGroupRequest2 = JoinGroupRequest1#{member_id => MemberId},
    {ok, #{leader := MemberId, protocol_name := <<"range">>, members := Members}} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest2,
            fun join_group_response:decode_join_group_response_7/1
        ),
    [#{member_id := MemberId, metadata := Metadata}] = Members,
    ?assertMatch(
        #{topics := [TopicName1, TopicName2], user_data := <<>>},
        kafcod_consumer_protocol:decode_metadata(Metadata)
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

as_follower() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    TopicName1 = ?TOPIC_NAME,
    TopicName2 = ?TOPIC_NAME_2,

    meck:expect(kamock_join_group, handle_join_group_request, kamock_join_group:as_follower()),

    % Now for the fun part: we need an *entire* JoinGroup flow.
    {ok, Connection} = kafcod_connection:start_link(Broker),

    JoinGroupRequest1 = #{
        session_timeout_ms => 45_000,
        rebalance_timeout_ms => 90_000,
        protocol_type => <<"consumer">>,
        protocols => [
            #{
                name => <<"range">>,
                metadata => kafcod_consumer_protocol:encode_metadata(#{
                    topics => [TopicName1, TopicName2], user_data => <<>>
                })
            }
        ],
        member_id => <<>>,
        group_instance_id => null,
        group_id => GroupId
    },
    {ok, #{error_code := ?MEMBER_ID_REQUIRED, member_id := MemberId}} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest1,
            fun join_group_response:decode_join_group_response_7/1
        ),
    ?assertNotEqual(<<>>, MemberId),

    JoinGroupRequest2 = JoinGroupRequest1#{member_id => MemberId},
    {ok, #{leader := LeaderId, protocol_name := <<"range">>, members := []}} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest2,
            fun join_group_response:decode_join_group_response_7/1
        ),

    ?assertNotEqual(MemberId, LeaderId),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.
