-module(kamock_describe_cluster_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun describe_cluster/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

describe_cluster() ->
    ClusterId = <<"koEzdSygYpmF">>,
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(
        ?CLUSTER_REF, [101, 102, 103], #{cluster_id => ClusterId}
    ),

    % You can send DescribeCluster to any broker in the cluster, so we just use the first one.
    {ok, Connection} = kafcod_connection:start_link(Bootstrap),
    {ok, DescribeClusterResponse} = kafcod_connection:call(
        Connection,
        fun describe_cluster_request:encode_describe_cluster_request_0/1,
        #{include_cluster_authorized_operations => false},
        fun describe_cluster_response:decode_describe_cluster_response_0/1
    ),

    % For some reason, the brokers in here use 'broker_id', rather than 'node_id'.
    ExpectedBrokers = lists:map(
        fun(Broker = #{node_id := NodeId}) ->
            maps:with([host, port, broker_id, rack], maps:put(broker_id, NodeId, Broker))
        end,
        Brokers
    ),

    #{node_id := ControllerId} = Bootstrap,
    ?assertMatch(
        #{
            error_code := ?NONE,
            throttle_time_ms := 0,
            brokers := ExpectedBrokers,
            controller_id := ControllerId,
            cluster_id := ClusterId
        },
        DescribeClusterResponse
    ),

    kafcod_connection:stop(Connection),
    kamock_cluster:stop(Cluster),
    ok.
