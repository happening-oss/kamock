-module(kamock_offset_ets_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).

-define(TABLE, ?MODULE).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun offset_fetch/0,
        fun offset_commit/0
    ]}.

setup() ->
    ets:new(?TABLE, [named_table, public]),
    meck:expect(
        kamock_offset_commit,
        handle_offset_commit_request,
        kamock_offset_commit:to_ets(?TABLE)
    ),
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        kamock_offset_fetch:from_ets(?TABLE)
    ),
    ok.

cleanup(_) ->
    ets:delete(?TABLE),
    meck:unload().

offset_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    kamock_offset_commit:insert(?TABLE, ?GROUP_ID, ?TOPIC_NAME, 0, 123),
    kamock_offset_commit:insert(?TABLE, ?GROUP_ID, ?TOPIC_NAME, 1, 456),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    GroupId = ?GROUP_ID,
    TopicName = ?TOPIC_NAME,

    OffsetFetchRequest = #{
        group_id => GroupId,
        topics => [
            #{
                name => TopicName,
                partition_indexes => [0, 1]
            }
        ]
    },

    {ok, OffsetFetchResponse} = kafcod_connection:call(
        Connection,
        fun offset_fetch_request:encode_offset_fetch_request_4/1,
        OffsetFetchRequest,
        fun offset_fetch_response:decode_offset_fetch_response_4/1
    ),

    ?assertMatch(
        #{
            error_code := ?NONE,
            topics := [
                #{
                    name := TopicName,
                    partitions := [
                        #{partition_index := 0, error_code := ?NONE, committed_offset := 123},
                        #{partition_index := 1, error_code := ?NONE, committed_offset := 456}
                    ]
                }
            ]
        },
        OffsetFetchResponse
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

offset_commit() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    MemberId = kamock_join_group:generate_member_id(<<"client-id">>),
    TopicName = ?TOPIC_NAME,

    {ok, Connection} = kafcod_connection:start_link(Broker),

    OffsetCommitRequest = #{
        group_id => GroupId,
        topics => [
            #{
                name => TopicName,
                partitions => [
                    #{
                        partition_index => Partition,
                        committed_offset => Offset,
                        % Yes, committed_metadata is nullable, but when you OffsetFetch a brand-new group,
                        % you get an empty binary, so we'll do that.
                        committed_metadata => <<>>
                    }
                 || {Partition, Offset} <- [{0, 123}, {1, 456}]
                ]
            }
        ],
        % ignored
        member_id => MemberId,
        generation_id_or_member_epoch => -1,
        retention_time_ms => -1
    },

    {ok, OffsetCommitResponse} = kafcod_connection:call(
        Connection,
        fun offset_commit_request:encode_offset_commit_request_3/1,
        OffsetCommitRequest,
        fun offset_commit_response:decode_offset_commit_response_3/1
    ),

    ?assertMatch(
        #{
            topics := [
                #{
                    name := TopicName,
                    partitions := [
                        #{partition_index := 0, error_code := ?NONE},
                        #{partition_index := 1, error_code := ?NONE}
                    ]
                }
            ],
            throttle_time_ms := 0
        },
        OffsetCommitResponse
    ),

    ?assertMatch({123, _}, kamock_offset_commit:lookup(?TABLE, ?GROUP_ID, ?TOPIC_NAME, 0)),
    ?assertMatch({456, _}, kamock_offset_commit:lookup(?TABLE, ?GROUP_ID, ?TOPIC_NAME, 1)),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.
