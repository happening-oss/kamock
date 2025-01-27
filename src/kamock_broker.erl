-module(kamock_broker).
-export([
    start/0,
    start/1,
    start/2,
    start/3,

    stop/1,

    info/0,
    info/1
]).

-define(DEFAULT_CLUSTER_ID, <<"kamock_cluster">>).
-define(DEFAULT_HANDLER, kamock_broker_handler).

start() ->
    start(make_ref(), #{}, undefined).

start(Ref) ->
    start(Ref, #{}, undefined).

start(Ref, Options) ->
    start(Ref, Options, undefined).

ensure_default_options(Options) ->
    DefaultOptions = #{
        node_id => 101,
        port => 0,
        node_ids => [101],
        cluster_id => ?DEFAULT_CLUSTER_ID,
        env => #{},
        handler => ?DEFAULT_HANDLER
    },
    maps:merge(DefaultOptions, Options).

start(
    Ref0,
    _Options = #{
        node_id := NodeId,
        port := Port,
        node_ids := NodeIds,
        cluster_id := ClusterId,
        env := Env0,
        handler := Handler
    },
    Cluster
) ->
    Ref = {?MODULE, Ref0},

    % Env is passed to the various handlers; some tests use the node_id to enforce node affinity. You can put arbitrary
    % items in here.
    Env = Env0#{
        ref => Ref0,
        node_id => NodeId,
        node_ids => NodeIds,
        cluster => Cluster,
        cluster_id => ClusterId
        % Note: kamock_broker_protocol will add 'host' and 'port' to Env after calling ranch:handshake.
    },
    ProtocolOpts = #{env => Env, handler => Handler},

    {ok, AssignedPort} = start_listeners(Ref, Port, ProtocolOpts),
    Broker = #{
        host => <<"localhost">>, port => AssignedPort, node_id => NodeId, rack => null, ref => Ref
    },
    kamock_cluster:register_broker(Cluster, Broker),
    {ok, Broker};
start(Ref, Options, Cluster) ->
    start(Ref, ensure_default_options(Options), Cluster).

% Start listeners on IPv4 and IPv6.
start_listeners(Ref, Port, ProtocolOpts) ->
    {ok, _LSup} = ranch:start_listener(
        Ref,
        ranch_tcp,
        #{socket_opts => [inet, {port, Port}]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    % To keep kcat happy -- we occasionally want to be able to validate the mock broker behaviour with a "real" client
    % -- we also listen on IPv6. We deliberately *don't* return these in info/0, below -- it would result in
    % double-counting.
    AssignedPort = ranch:get_port(Ref),
    {ok, _LSup6} = ranch:start_listener(
        {Ref, inet6},
        ranch_tcp,
        #{socket_opts => [inet6, {ipv6_v6only, true}, {port, AssignedPort}]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    {ok, AssignedPort}.

stop(_Broker = #{ref := Ref}) ->
    ranch:stop_listener(Ref),
    ranch:stop_listener({Ref, inet6}).

info() ->
    maps:filtermap(
        fun
            ({kamock_broker, _Ref}, Info) -> {true, Info};
            (_, _) -> false
        end,
        ranch:info()
    ).

info(_Broker = #{ref := Ref}) ->
    info(Ref);
info(Ref = {kamock_broker, _}) ->
    ranch:info(Ref);
info(Ref) ->
    ranch:info({kamock_broker, Ref}).
