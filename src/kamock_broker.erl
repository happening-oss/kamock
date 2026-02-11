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

    get_port/1,

    connections/1
]).

-define(DEFAULT_CLUSTER_ID, <<"kamock_cluster">>).
-define(DEFAULT_HANDLER, kamock_broker_handler).

-define(LISTENER_REF(Ref), {?MODULE, Ref}).
-define(LISTENER_REF_6(Ref), {?MODULE, Ref, inet6}).

-type start_ret() :: {ok, kamock:broker()}.

-spec start() -> start_ret().
start() ->
    start(make_ref(), #{}, [], undefined).

-spec start(Ref :: kamock:ref()) -> start_ret().
start(Ref) ->
    start(Ref, #{}, [], undefined).

-spec start(Ref :: kamock:ref(), Options :: map()) -> start_ret().
start(Ref, Options) ->
    start(Ref, Options, [], undefined).

-spec start(Ref :: kamock:ref(), Options :: map(), Cluster :: pid()) -> start_ret().
start(Ref, Options, Cluster) ->
    start(Ref, Options, [], Cluster).

-spec start_tls(Ref :: kamock:ref(), Options :: map(), SocketOpts :: [ssl:tls_server_option()]) ->
    start_ret().
start_tls(Ref, Options, SocketOpts) ->
    start(Ref, Options#{transport => ranch_ssl}, SocketOpts, undefined).

-spec start_tls(
    Ref :: kamock:ref(),
    Options :: map(),
    SocketOpts :: [ssl:tls_server_option()],
    Cluster :: pid()
) -> start_ret().
start_tls(Ref, Options, SocketOpts, Cluster) ->
    start(Ref, Options#{transport => ranch_ssl}, SocketOpts, Cluster).

start(
    Ref,
    Options = #{
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
    Host = <<"localhost">>,

    % Env is passed to the various handlers; some tests use the node_id to enforce node affinity. You can put arbitrary
    % items in here.
    ControllerId = maps:get(controller_id, Options, hd(NodeIds)),

    % We copy a bunch of stuff into Env so that it's available to mocks.
    Env = Env0#{
        ref => Ref,
        node_id => NodeId,
        node_ids => NodeIds,
        cluster => Cluster,
        cluster_id => ClusterId,
        controller_id => ControllerId
        % Note: kamock_broker_protocol will add 'host' and 'port' to Env after calling ranch:handshake.
    },

    ProtocolOpts = #{env => Env, handler => Handler},

    {ok, AssignedPort} = start_listeners(Ref, Port, Transport, SocketOpts, ProtocolOpts),
    Broker = #{
        host => Host,
        port => AssignedPort,
        addr => format_addr(Host, AssignedPort),
        node_id => NodeId,
        rack => null,
        ref => Ref
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
        ?LISTENER_REF(Ref),
        Transport,
        #{socket_opts => [inet, {port, Port} | SocketOpts]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    % To keep kcat happy -- we occasionally want to be able to validate the mock broker behaviour with a "real" client
    % -- we also listen on IPv6. We deliberately *don't* return these in info/0, below -- it would result in
    % double-counting.
    AssignedPort = ranch:get_port(?LISTENER_REF(Ref)),
    {ok, _LSup6} = ranch:start_listener(
        ?LISTENER_REF_6(Ref),
        Transport,
        #{socket_opts => [inet6, {ipv6_v6only, true}, {port, AssignedPort} | SocketOpts]},
        kamock_broker_protocol,
        ProtocolOpts
    ),
    {ok, AssignedPort}.

format_addr(Host, Port) when is_binary(Host), is_integer(Port) ->
    lists:flatten(io_lib:format("~s:~B", [Host, Port])).

stop(_Broker = #{ref := Ref}) ->
    stop(Ref);
stop(Ref) ->
    ranch:stop_listener(?LISTENER_REF(Ref)),
    ranch:stop_listener(?LISTENER_REF_6(Ref)).

drop(_Broker = #{ref := Ref}) ->
    Connections = which_connections(Ref),
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
            (?LISTENER_REF(_Ref), Info) -> {true, Info};
            (_, _Info) -> false
        end,
        ranch:info()
    ).

info(_Broker = #{ref := Ref}) ->
    info(Ref);
info(Ref) ->
    ranch:info(?LISTENER_REF(Ref)).

get_port(_Broker = #{port := Port}) ->
    Port;
get_port(Ref) ->
    ranch:get_port(?LISTENER_REF(Ref)).

connections(_Broker = #{ref := Ref}) ->
    Connections = which_connections(Ref),
    [kamock_broker_protocol:connection_info(C) || C <- Connections].

which_connections(Ref) ->
    [
        Pid
     || {SupRef, _N, Sup} <- ranch_server:get_connections_sups(),
        SupRef =:= ?LISTENER_REF(Ref),
        {kamock_broker_protocol, Pid, _, _} <- supervisor:which_children(Sup),
        is_pid(Pid)
    ].
