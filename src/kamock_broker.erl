-module(kamock_broker).
-export([
    start/0,
    start/1,
    start/2,
    start/3,
    start/4,

    start_tls/3,
    start_tls/4,

    stop/1,
    drop/1,

    info/0,
    info/1,

    connections/1
]).

-define(DEFAULT_CLUSTER_ID, <<"kamock_cluster">>).
-define(DEFAULT_HANDLER, kamock_broker_handler).

start() ->
    start(make_ref(), #{}, [], undefined).

start(Ref) ->
    start(Ref, #{}, [], undefined).

start(Ref, Options) ->
    start(Ref, Options, [], undefined).

start(Ref, Options, Cluster) ->
    start(Ref, Options, [], Cluster).

start_tls(Ref, Options, SocketOpts) ->
    start(Ref, Options#{transport => ranch_ssl}, SocketOpts, undefined).

start_tls(Ref, Options, SocketOpts, Cluster) ->
    start(Ref, Options#{transport => ranch_ssl}, SocketOpts, Cluster).

start(
    Ref0,
    _Options = #{
        node_id := NodeId,
        port := Port,
        node_ids := NodeIds,
        cluster_id := ClusterId,
        env := Env0,
        handler := Handler,
        transport := Transport
    },
    SocketOpts,
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

    {ok, AssignedPort} = start_listeners(Ref, Port, Transport, SocketOpts, ProtocolOpts),
    Broker = #{
        host => <<"localhost">>, port => AssignedPort, node_id => NodeId, rack => null, ref => Ref
    },
    kamock_cluster:register_broker(Cluster, Broker),
    {ok, Broker};
start(Ref, Options, SocketOpts, Cluster) ->
    start(Ref, ensure_default_options(Options), SocketOpts, Cluster).

ensure_default_options(Options) ->
    DefaultOptions = #{
        node_id => 101,
        port => 0,
        node_ids => [101],
        cluster_id => ?DEFAULT_CLUSTER_ID,
        env => #{},
        transport => ranch_tcp,
        transport_opts => [],
        handler => ?DEFAULT_HANDLER
    },
    maps:merge(DefaultOptions, Options).

% Start listeners on IPv4 and IPv6.
start_listeners(Ref, Port, Transport, SocketOpts, ProtocolOpts) ->
    {ok, _LSup} = ranch:start_listener(
        Ref,
        Transport,
        #{socket_opts => [inet, {port, Port} | SocketOpts]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    % To keep kcat happy -- we occasionally want to be able to validate the mock broker behaviour with a "real" client
    % -- we also listen on IPv6. We deliberately *don't* return these in info/0, below -- it would result in
    % double-counting.
    AssignedPort = ranch:get_port(Ref),
    {ok, _LSup6} = ranch:start_listener(
        {Ref, inet6},
        Transport,
        #{socket_opts => [inet6, {ipv6_v6only, true}, {port, AssignedPort} | SocketOpts]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    {ok, AssignedPort}.

stop(_Broker = #{ref := Ref}) ->
    stop(Ref);
stop(Ref = {kamock_broker, _}) ->
    ranch:stop_listener(Ref),
    ranch:stop_listener({Ref, inet6}).

drop(_Broker = #{ref := Ref}) ->
    Connections = [
        Pid
     || {SupRef, _N, Sup} <- ranch_server:get_connections_sups(),
        SupRef =:= Ref,
        {kamock_broker_protocol, Pid, _, _} <- supervisor:which_children(Sup),
        is_pid(Pid)
    ],
    lists:foreach(
        fun(Pid) when is_pid(Pid) ->
            exit(Pid, kill)
        end,
        Connections
    ).

info() ->
    % Filter out the IPv6 listeners.
    maps:filtermap(
        fun
            ({kamock_broker, _Ref}, Info) -> {true, Info};
            ({{kamock_broker, _Ref}, inet6}, _Info) -> false
        end,
        ranch:info()
    ).

info(_Broker = #{ref := Ref}) ->
    info(Ref);
info(Ref = {kamock_broker, _}) ->
    ranch:info(Ref);
info(Ref) ->
    ranch:info({kamock_broker, Ref}).

connections(_Broker = #{ref := Ref}) ->
    Connections = [
        Pid
     || {SupRef, _N, Sup} <- ranch_server:get_connections_sups(),
        SupRef =:= Ref,
        {kamock_broker_protocol, Pid, _, _} <- supervisor:which_children(Sup),
        is_pid(Pid)
    ],
    [kamock_broker_protocol:connection_info(C) || C <- Connections].
