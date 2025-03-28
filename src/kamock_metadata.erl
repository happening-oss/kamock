-module(kamock_metadata).
-export([handle_metadata_request/2]).
-export([with_topics/1]).

-export([make_metadata_response_topics/2]).

-define(AUTHORIZED_OPERATIONS, -2147483648).
-define(CLUSTER_AUTHORIZED_OPERATIONS, ?AUTHORIZED_OPERATIONS).

handle_metadata_request(
    _MetadataRequest = #{correlation_id := CorrelationId, topics := Topics},
    Env = #{cluster_id := ClusterId, node_ids := [ControllerId | _]}
) ->
    Brokers = get_brokers(Env),
    #{
        correlation_id => CorrelationId,
        cluster_id => ClusterId,
        brokers => Brokers,
        controller_id => ControllerId,
        topics => make_metadata_response_topics(Topics, Env),
        cluster_authorized_operations => ?CLUSTER_AUTHORIZED_OPERATIONS,
        throttle_time_ms => 0
    }.

with_topics(TopicNames) ->
    fun
        (Req = #{topics := null}, Env) ->
            Req2 = Req#{topics := [#{name => TopicName} || TopicName <- TopicNames]},
            meck:passthrough([Req2, Env]);
        (Req, Env) ->
            meck:passthrough([Req, Env])
    end.

make_metadata_response_topics(_Topics = null, _Env) ->
    % null means "all topics", but we're not smart enough to know what topics are available.
    [];
make_metadata_response_topics(Topics, Env) ->
    lists:map(
        fun(Topic) ->
            kamock_metadata_response_topic:make_metadata_response_topic(Topic, Env)
        end,
        Topics
    ).

get_brokers(_Env = #{cluster := Cluster}) when Cluster /= undefined ->
    kamock_cluster:get_brokers(Cluster);
get_brokers(_Env = #{node_id := NodeId, host := Host, port := Port}) ->
    [#{host => Host, port => Port, node_id => NodeId, rack => null}].
