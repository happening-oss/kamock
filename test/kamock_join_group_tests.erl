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
        fun select_protocol/0,
        fun as_follower/0,
        fun return_error/0
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

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % As the leader, we're expecting the broker to choose a protocol and to give us the list of members (we're the only
    % member).
    JoinGroupResponse = do_join_group(Connection, GroupId, [TopicName1, TopicName2]),

    #{leader := LeaderId, member_id := MemberId, protocol_name := <<"range">>, members := Members} =
        JoinGroupResponse,
    ?assertEqual(LeaderId, MemberId),
    [#{member_id := MemberId, metadata := Metadata}] = Members,
    ?assertMatch(
        #{topics := [TopicName1, TopicName2], user_data := <<>>},
        kafcod_consumer_protocol:decode_metadata(Metadata)
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

select_protocol() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    TopicName1 = ?TOPIC_NAME,
    TopicName2 = ?TOPIC_NAME_2,

    % As the broker, choose the second protocol we're given.
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(0, fun(_ProtocolType, Protocols) ->
            [_, Protocol] = Protocols,
            Protocol
        end)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % Pass multiple protocols; the broker (see above) will choose the second one.
    Protocols = [
        #{
            name => <<"range">>,
            metadata => kafcod_consumer_protocol:encode_metadata(#{
                topics => [TopicName1, TopicName2], user_data => <<>>
            })
        },
        #{
            name => <<"roundrobin">>,
            metadata => kafcod_consumer_protocol:encode_metadata(#{
                topics => [TopicName1, TopicName2], user_data => <<>>
            })
        }
    ],

    % We're the leader; the broker will give us the chosen protocol and the list of members.
    JoinGroupResponse = do_join_group_with_protocols(Connection, GroupId, Protocols),
    #{
        leader := LeaderId,
        member_id := MemberId,
        protocol_name := <<"roundrobin">>,
        members := Members
    } =
        JoinGroupResponse,
    ?assertEqual(LeaderId, MemberId),
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

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We're the follower; the broker will give us an empty list of members. It also gives us the chosen protocol, even
    % though that's kinda pointless.
    JoinGroupResponse = do_join_group(Connection, GroupId, [TopicName1, TopicName2]),
    #{leader := LeaderId, member_id := MemberId, protocol_name := <<"range">>, members := []} =
        JoinGroupResponse,
    ?assertNotEqual(MemberId, LeaderId),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

return_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    TopicName1 = ?TOPIC_NAME,
    TopicName2 = ?TOPIC_NAME_2,

    meck:expect(
        kamock_join_group, handle_join_group_request, kamock_join_group:return_error(12345)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    MemberId = <<"some_member_id">>,
    Request = make_request(GroupId, MemberId, make_protocols([TopicName1, TopicName2])),
    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0,
            member_id => MemberId,
            generation_id => -1,
            members => [],
            protocol_name => <<>>,
            protocol_type => <<>>,
            leader => <<>>
        }},
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            Request,
            fun join_group_response:decode_join_group_response_7/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

do_join_group(Connection, GroupId, Topics) ->
    do_join_group_with_protocols(Connection, GroupId, make_protocols(Topics)).

make_protocols(Topics) ->
    [
        #{
            name => <<"range">>,
            metadata => kafcod_consumer_protocol:encode_metadata(#{
                topics => Topics, user_data => <<>>
            })
        }
    ].

do_join_group_with_protocols(Connection, GroupId, Protocols) ->
    JoinGroupRequest1 = make_request(GroupId, <<>>, Protocols),
    {ok, #{error_code := ?MEMBER_ID_REQUIRED, member_id := MemberId}} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest1,
            fun join_group_response:decode_join_group_response_7/1
        ),
    ?assertNotEqual(<<>>, MemberId),

    JoinGroupRequest2 = JoinGroupRequest1#{member_id => MemberId},
    {ok, JoinGroupResponse} =
        kafcod_connection:call(
            Connection,
            fun join_group_request:encode_join_group_request_7/1,
            JoinGroupRequest2,
            fun join_group_response:decode_join_group_response_7/1
        ),
    JoinGroupResponse.

make_request(GroupId, MemberId, Protocols) ->
    #{
        session_timeout_ms => 45_000,
        rebalance_timeout_ms => 90_000,
        protocol_type => <<"consumer">>,
        protocols => Protocols,
        member_id => MemberId,
        group_instance_id => null,
        group_id => GroupId
    }.
