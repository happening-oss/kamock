-module(kamock_cluster_start_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).

setup() ->
    ok.

cleanup(_) ->
    ok.

kamock_cluster_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun start_stop/0
    ]}.

start_stop() ->
    % There should be no listeners.
    ?assertEqual(#{}, kamock_broker:info()),

    {ok, Cluster, Brokers} = kamock_cluster:start(?CLUSTER_REF, [101, 102, 103]),

    % Several tests expect node 101 to be the bootstrap broker, so we assert that it appears first.
    % Ideally, they'd appear in the order given above, but that's not a particularly strong requirement.
    ?assertMatch([#{node_id := 101} | _], Brokers),

    % There should be three listeners.
    ?assertEqual(3, map_size(kamock_broker:info())),

    kamock_cluster:stop(Cluster),

    % There should be no listeners.

    % Note: If it fails here, it's probably because one of the other tests failed and/or forgot to stop its broker.
    % Since the unit tests pass their {module, function} as the broker ref, it should be easy to figure out which one.
    ?assertEqual(#{}, kamock_broker:info()),
    ok.
