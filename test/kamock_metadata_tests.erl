-module(kamock_metadata_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun broker_includes_self/0,
        fun cluster_metadata/0,
        fun default_no_topics/0,
        fun named_topic_appears/0,
        fun with_topics/0,
        fun number_of_partitions/0,
        fun unknown_topic_or_partition/0,
        fun partitions_move/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

broker_includes_self() ->
    % If there's only one broker, it should include itself in the response.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = <<"cats">>,

    {ok, C} = kafcod_connection:start_link(Broker),
    ?assertMatch(
        {ok, #{
            topics := [#{name := TopicName, error_code := ?NONE, partitions := _}], brokers := [_]
        }},
        kafcod_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false,
                topics => [#{name => TopicName}]
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

cluster_metadata() ->
    % In this test, we'll override the cluster ID. In the later tests, we'll leave it as the default.
    ClusterId = <<"koEzdSygYpmF">>,
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102, 103], #{cluster_id => ClusterId}
    ),
    {ok, Connection} = kafcod_connection:start_link(Bootstrap),

    ControllerId = maps:get(node_id, Bootstrap),
    ExpectedBrokers = [maps:with([host, port, node_id, rack], B) || B <- Brokers],
    ?assertMatch(
        {ok, #{
            cluster_id := ClusterId,
            controller_id := ControllerId,
            brokers := ExpectedBrokers
        }},
        kafcod_connection:call(
            Connection,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => null,
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_cluster:stop(Cluster),
    ok.

default_no_topics() ->
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102, 103]),
    {ok, Connection} = kafcod_connection:start_link(Bootstrap),

    ControllerId = maps:get(node_id, Bootstrap),
    ExpectedBrokers = [maps:with([host, port, node_id, rack], B) || B <- Brokers],
    ?assertMatch(
        {ok, #{
            topics := [],
            cluster_id := <<"kamock_cluster">>,
            controller_id := ControllerId,
            brokers := ExpectedBrokers
        }},
        kafcod_connection:call(
            Connection,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => null,
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_cluster:stop(Cluster),
    ok.

named_topic_appears() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    {ok, #{topics := [#{name := <<"cats">>, partitions := Partitions}]}} =
        kafcod_connection:call(
            Connection,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => [#{name => <<"cats">>}],
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        ),
    ?assertEqual(4, length(Partitions)),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

with_topics() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([<<"cats">>, <<"dogs">>])
    ),

    {ok, #{
        topics := [
            #{name := <<"cats">>, partitions := _},
            #{name := <<"dogs">>, partitions := _}
        ]
    }} =
        kafcod_connection:call(
            Connection,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => null,
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

number_of_partitions() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([<<"cats">>, <<"dogs">>])
    ),

    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions(lists:seq(0, 63))
    ),

    {ok, #{
        topics := [
            #{name := <<"cats">>, partitions := PCat},
            #{name := <<"dogs">>, partitions := PDog}
        ]
    }} =
        kafcod_connection:call(
            Connection,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => null,
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        ),

    ?assertEqual(64, length(PCat)),
    ?assertEqual(64, length(PDog)),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

unknown_topic_or_partition() ->
    % Demonstrate what Metadata looks like for a missing topic or partition.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        fun(#{name := Topic}, _Env) ->
            #{
                error_code => ?UNKNOWN_TOPIC_OR_PARTITION,
                name => Topic,
                is_internal => false,
                partitions => [],
                topic_authorized_operations => 0
            }
        end
    ),

    TopicName = ?TOPIC_NAME,
    {ok, C} = kafcod_connection:start_link(Broker),
    ?assertMatch(
        {ok, #{
            topics := [
                #{name := TopicName, error_code := ?UNKNOWN_TOPIC_OR_PARTITION, partitions := []}
            ],
            brokers := [_]
        }},
        kafcod_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false,
                topics => [#{name => TopicName}]
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

-define(LEADER_IS(NodeId), #{
    topics := [
        #{
            partitions := [
                #{partition_index := 0, leader_id := NodeId},
                #{partition_index := 1, leader_id := NodeId},
                #{partition_index := 2, leader_id := NodeId},
                #{partition_index := 3, leader_id := NodeId}
            ]
        }
    ]
}).

partitions_move() ->
    % This test demonstrates how to mock subsequent Metadata requests.
    %
    % In particular, it shows how to use meck:expect from _inside_ meck:expect/meck:seq, which is how we change the
    % kamock_metadata_response_partition:make_metadata_response_partition() mock on each Metadata request.
    NodeIds = [101, 102, 103],
    {ok, Cluster, [Bootstrap | _Brokers]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    % When a partition moves, it's because the metadata initially told us about the wrong leader. Subsequent metadata
    % should be correct. We'll do this by having the partitions all move to the next broker.
    TopicName = ?TOPIC_NAME,

    % Note that there's a potential race condition here: if there are two Metdata requests to separate brokers, in quick
    % succession, the first expect can be replaced by the second expect, _before_ it has a chance to be called.
    %
    % This isn't a problem in this particular test, since we don't send two Metadata requests at the same time to
    % separate brokers. But it's worth bearing in mind, in particular for testing overlapping fetch requests.
    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        ['_', '_'],
        meck:seq([
            fun(Req, Env) ->
                % On the first call, we'll have all of the partitions on node 101.
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        kamock_metadata_response_partition:make_metadata_response_partition(
                            PartitionIndex, 101, NodeIds
                        )
                    end
                ),
                meck:passthrough([Req, Env])
            end,
            fun(Req, Env) ->
                % On subsequent calls, we'll have all of the partitions on node 102.
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        kamock_metadata_response_partition:make_metadata_response_partition(
                            PartitionIndex, 102, NodeIds
                        )
                    end
                ),
                meck:passthrough([Req, Env])
            end
        ])
    ),

    {ok, C} = kafcod_connection:start_link(Bootstrap),
    ?assertMatch(
        {ok, ?LEADER_IS(101)},
        kafcod_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false,
                topics => [#{name => TopicName}]
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),
    ?assertMatch(
        {ok, ?LEADER_IS(102)},
        kafcod_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false,
                topics => [#{name => TopicName}]
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    kafcod_connection:stop(C),
    kamock_cluster:stop(Cluster),
    ok.
