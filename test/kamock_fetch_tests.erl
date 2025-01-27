-module(kamock_fetch_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

% These tests show that the mock broker and cluster are working correctly. They also demonstrate, in isolation, some of
% the techniques used by the "proper" unit tests.

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

setup() ->
    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_partition_data, [passthrough]),
    meck:new(kamock_metadata_response_partition, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun empty_topic/0,
        fun single_message_on_single_partition/0,
        fun not_leader_or_follower/0
    ]}.

empty_topic() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % Explicitly empty.
    meck:expect(kamock_partition_data, make_partition_data, kamock_partition_data:empty()),

    Topic = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    {ok, C} = kafcod_connection:start_link(Broker),

    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, Partitions),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := PartitionResponses, topic := Topic}]
    }} =
        kafcod_connection:call(
            C,
            fun fetch_request:encode_fetch_request_11/1,
            FetchRequest,
            fun fetch_response:decode_fetch_response_11/1
        ),

    ?assertMatch(
        [
            #{partition_index := 0, error_code := ?NONE, records := []},
            #{partition_index := 1, error_code := ?NONE, records := []},
            #{partition_index := 2, error_code := ?NONE, records := []},
            #{partition_index := 3, error_code := ?NONE, records := []}
        ],
        PartitionResponses
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

single_message_on_single_partition() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % After writing all of that complicated meck stuff, I realised that because the client tracks the offset, it could
    % be replaced with the following:
    MessageBuilder = fun(_Topic, Partition, Offset) ->
        Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
        Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
        #{key => Key, value => Value}
    end,
    meck:expect(kamock_partition_data, make_partition_data, [
        {
            ['_', meck:is(fun(#{partition := P}) -> P == 0 end), '_'],
            kamock_partition_data:range(0, 1, MessageBuilder)
        },
        {['_', '_', '_'], kamock_partition_data:empty()}
    ]),

    {ok, C} = kafcod_connection:start_link(Broker),

    assert_single_message(C),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

assert_single_message(Connection) ->
    Topic = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],

    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, Partitions),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := PartitionResponses, topic := Topic}]
    }} =
        kafcod_connection:call(
            Connection,
            fun fetch_request:encode_fetch_request_11/1,
            FetchRequest,
            fun fetch_response:decode_fetch_response_11/1
        ),
    ?assertMatch(
        [
            #{
                partition_index := 0,
                error_code := ?NONE,
                records := [
                    #{
                        records := [
                            #{value := <<"value-0-0">>, key := <<"key-0-0">>, offset_delta := 0}
                        ],
                        base_offset := 0,
                        last_offset_delta := 0
                    }
                ]
            },
            #{partition_index := 1, error_code := ?NONE, records := []},
            #{partition_index := 2, error_code := ?NONE, records := []},
            #{partition_index := 3, error_code := ?NONE, records := []}
        ],
        PartitionResponses
    ),
    ok.

not_leader_or_follower() ->
    % We're starting a mock cluster, even though we only need two of the brokers, and none of the clustering.
    {ok, Cluster, Brokers} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    % Any requests to node 101 fail with ?NOT_LEADER_OR_FOLLOWER; other requests succeed.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (_, #{partition := P}, #{node_id := 101}) ->
                kamock_partition_data:make_error(
                    P, ?NOT_LEADER_OR_FOLLOWER
                );
            (_, #{partition := P}, #{node_id := _}) ->
                kamock_partition_data:make_empty(P)
        end
    ),

    assert_fetch_error_code(get_node_by_id(Brokers, 101), ?NOT_LEADER_OR_FOLLOWER),
    assert_fetch_error_code(get_node_by_id(Brokers, 102), ?NONE),

    kamock_cluster:stop(Cluster),
    ok.

assert_fetch_error_code(Broker, ExpectedErrorCode) ->
    Topic = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    {ok, C} = kafcod_connection:start_link(Broker),

    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, Partitions),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := PartitionResponses, topic := Topic}]
    }} =
        kafcod_connection:call(
            C,
            fun fetch_request:encode_fetch_request_11/1,
            FetchRequest,
            fun fetch_response:decode_fetch_response_11/1
        ),
    ?assert(
        lists:all(
            fun(#{error_code := ErrorCode}) ->
                ErrorCode == ExpectedErrorCode
            end,
            PartitionResponses
        )
    ),

    kafcod_connection:stop(C),
    ok.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.
