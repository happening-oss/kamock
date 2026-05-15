-module(kamock_offset_fetch_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun offset_fetch/0,
        fun return_error/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

offset_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    GroupId = ?GROUP_ID,
    TopicName = ?TOPIC_NAME,

    Request = make_request(GroupId, TopicName, [0, 1]),

    ?assertMatch(
        {ok, #{
            error_code := ?NONE,
            topics := [
                #{
                    name := TopicName,
                    partitions := [
                        #{partition_index := 0, error_code := ?NONE, committed_offset := -1},
                        #{partition_index := 1, error_code := ?NONE, committed_offset := -1}
                    ]
                }
            ]
        }},
        kafcod_connection:call(
            Connection,
            fun offset_fetch_request:encode_offset_fetch_request_4/1,
            Request,
            fun offset_fetch_response:decode_offset_fetch_response_4/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

return_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,

    meck:expect(
        kamock_offset_fetch, handle_offset_fetch_request, kamock_offset_fetch:return_error(12345)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    Request = make_request(GroupId, ?TOPIC_NAME, [0, 1]),

    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0,
            topics => []
        }},
        kafcod_connection:call(
            Connection,
            fun offset_fetch_request:encode_offset_fetch_request_4/1,
            Request,
            fun offset_fetch_response:decode_offset_fetch_response_4/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

make_request(GroupId, TopicName, PartitionIndexes) ->
    #{
        group_id => GroupId,
        topics => [
            #{
                name => TopicName,
                partition_indexes => PartitionIndexes
            }
        ]
    }.
