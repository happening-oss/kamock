-module(kamock_describe_cluster).
-export([handle_describe_cluster_request/2]).
-include_lib("kafcod/include/error_code.hrl").

-define(AUTHORIZED_OPERATIONS, -2147483648).
-define(CLUSTER_AUTHORIZED_OPERATIONS, ?AUTHORIZED_OPERATIONS).

handle_describe_cluster_request(
    _DescribeClusterRequest = #{correlation_id := CorrelationId},
    Env = #{cluster_id := ClusterId, node_ids := [ControllerId | _]}
) ->
    % For some reason, the brokers in here use 'broker_id', rather than 'node_id'.
    Brokers = lists:map(
        fun(Broker = #{node_id := NodeId}) ->
            maps:with([host, port, broker_id, rack], maps:put(broker_id, NodeId, Broker))
        end,
        get_brokers(Env)
    ),

    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE,
        error_message => null,
        cluster_id => ClusterId,
        brokers => Brokers,
        controller_id => ControllerId,
        cluster_authorized_operations => ?CLUSTER_AUTHORIZED_OPERATIONS
    }.

get_brokers(_Env = #{cluster := Cluster}) when Cluster /= undefined ->
    kamock_cluster:get_brokers(Cluster);
get_brokers(_Env = #{node_id := NodeId, host := Host, port := Port}) ->
    [#{host => Host, port => Port, node_id => NodeId, rack => null}].
