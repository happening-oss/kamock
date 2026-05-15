-module(kamock_broker_restart_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).

broker_restart_rejoins_cluster_test() ->
    {ok, Cluster, Brokers} = kamock_cluster:start(?CLUSTER_REF, [101, 102, 103]),

    ?assertEqual(Brokers, kamock_cluster:get_brokers(Cluster)),

    [Broker | _] = Brokers,
    kamock_broker:stop(Broker),

    % There should be two brokers.
    ?assertMatch(
        [#{node_id := 102}, #{node_id := 103}],
        kamock_cluster:get_brokers(Cluster)
    ),

    % Restart the previous broker.
    kamock_broker:restart(Broker),

    ?assertEqual(Brokers, kamock_cluster:get_brokers(Cluster)),

    kamock_cluster:stop(Cluster),
    ok.
