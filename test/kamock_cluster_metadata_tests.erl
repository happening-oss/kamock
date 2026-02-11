-module(kamock_cluster_metadata_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun cluster_metadata/0,
        fun override_controller_id/0,
        fun default_no_topics/0,
        fun three_brokers_with_topics/0,
        fun six_brokers_with_topics/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

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

override_controller_id() ->
    ControllerId = 103,
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102, 103], #{controller_id => ControllerId}
    ),
    {ok, Connection} = kafcod_connection:start_link(Bootstrap),

    ExpectedBrokers = [maps:with([host, port, node_id, rack], B) || B <- Brokers],
    ?assertMatch(
        {ok, #{
            cluster_id := _,
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

three_brokers_with_topics() ->
    {ok, Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102, 103]),

    {ok, Connection} = kafcod_connection:start_link(Bootstrap),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([<<"cats">>, <<"dogs">>])
    ),

    {ok, MetadataResponse} =
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

    ?assertMatch(
        #{
            topics := [
                #{
                    name := <<"cats">>,
                    partitions := [
                        % By default, the partitions are round-robined through the brokers. 4 partitions and 3 brokers
                        % means we wrap around.
                        #{
                            partition_index := 0,
                            leader_id := 101,
                            replica_nodes := [101, 102, 103],
                            isr_nodes := [101, 102, 103]
                        },
                        #{
                            partition_index := 1,
                            leader_id := 102,
                            % replica_nodes and isr_nodes have the leader first, and then the other nodes in order.
                            replica_nodes := [102, 101, 103],
                            isr_nodes := [102, 101, 103]
                        },
                        #{
                            partition_index := 2,
                            leader_id := 103,
                            replica_nodes := [103, 101, 102],
                            isr_nodes := [103, 101, 102]
                        },
                        #{
                            partition_index := 3,
                            leader_id := 101,
                            replica_nodes := [101, 102, 103],
                            isr_nodes := [101, 102, 103]
                        }
                    ]
                },
                % Too tedious to assert 'dogs' as well. It's the same as 'cats'.
                #{name := <<"dogs">>, partitions := _}
            ]
        },
        MetadataResponse
    ),

    kafcod_connection:stop(Connection),
    kamock_cluster:stop(Cluster),
    ok.

six_brokers_with_topics() ->
    {ok, Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [
        101, 102, 103, 104, 105, 106
    ]),

    {ok, Connection} = kafcod_connection:start_link(Bootstrap),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([<<"cats">>, <<"dogs">>])
    ),

    {ok, MetadataResponse} =
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

    ?assertMatch(
        #{
            topics := [
                #{
                    name := <<"cats">>,
                    partitions := [
                        % By default, the partitions are round-robined through the brokers. 4 partitions and 3 brokers
                        % means we wrap around.
                        #{
                            partition_index := 0,
                            leader_id := 101,
                            % we don't provide a good way to control the replica count; every partition is on every broker.
                            replica_nodes := [101, 102, 103, 104, 105, 106],
                            isr_nodes := [101, 102, 103, 104, 105, 106]
                        },
                        #{
                            partition_index := 1,
                            leader_id := 102,
                            % replica_nodes and isr_nodes have the leader first, and then the other nodes in order.
                            replica_nodes := [102, 101, 103, 104, 105, 106],
                            isr_nodes := [102, 101, 103, 104, 105, 106]
                        },
                        #{
                            partition_index := 2,
                            leader_id := 103,
                            replica_nodes := [103, 101, 102, 104, 105, 106],
                            isr_nodes := [103, 101, 102, 104, 105, 106]
                        },
                        #{
                            partition_index := 3,
                            leader_id := 104,
                            replica_nodes := [104, 101, 102, 103, 105, 106],
                            isr_nodes := [104, 101, 102, 103, 105, 106]
                        }
                    ]
                },
                % Too tedious to assert 'dogs' as well. It's the same as 'cats'.
                #{name := <<"dogs">>, partitions := _}
            ]
        },
        MetadataResponse
    ),

    kafcod_connection:stop(Connection),
    kamock_cluster:stop(Cluster),
    ok.
