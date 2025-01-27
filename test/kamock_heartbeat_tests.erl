-module(kamock_heartbeat_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun new_generation_id/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

new_generation_id() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    GroupId = ?GROUP_ID,
    MemberId = <<"member-id">>,
    GenerationId = 0,

    % The mock broker, by default, doesn't actually care whether you've joined a group or not, so we can do this:
    {ok, #{error_code := ?NONE}} = kafcod_connection:call(
        Connection,
        fun heartbeat_request:encode_heartbeat_request_4/1,
        #{
            group_id => GroupId,
            generation_id => GenerationId,
            member_id => MemberId,
            group_instance_id => null
        },
        fun heartbeat_response:decode_heartbeat_response_4/1
    ),

    % Return a new generation ID from JoinGroup. You need to set this expectation first, to prevent a race.
    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_follower(<<"leader">>, 1)
    ),

    % Reset the generation ID.
    meck:expect(
        kamock_heartbeat, handle_heartbeat_request, kamock_heartbeat:expect_generation_id(1)
    ),

    {ok, #{error_code := ?REBALANCE_IN_PROGRESS}} = kafcod_connection:call(
        Connection,
        fun heartbeat_request:encode_heartbeat_request_4/1,
        #{
            group_id => GroupId,
            generation_id => GenerationId,
            member_id => MemberId,
            group_instance_id => null
        },
        fun heartbeat_response:decode_heartbeat_response_4/1
    ),

    % JoinGroup should get the new generation ID.
    JoinGroupRequest = #{
        session_timeout_ms => 45_000,
        rebalance_timeout_ms => 90_000,
        protocol_type => <<"consumer">>,
        protocols => [
            #{
                name => <<"range">>,
                metadata => kafcod_consumer_protocol:encode_metadata(#{
                    topics => [], user_data => <<>>
                })
            }
        ],
        member_id => MemberId,
        group_instance_id => null,
        group_id => GroupId
    },

    {ok, #{error_code := ?NONE, generation_id := 1}} = kafcod_connection:call(
        Connection,
        fun join_group_request:encode_join_group_request_7/1,
        JoinGroupRequest,
        fun join_group_response:decode_join_group_response_7/1
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.
