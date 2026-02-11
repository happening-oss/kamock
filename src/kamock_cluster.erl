-module(kamock_cluster).
-export([
    start/0,
    start/1,
    start/2,
    start/3,

    start_tls/4,

    stop/1
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3
]).
% For internal use; adding/removing brokers dynamically is not supported.
-export([
    get_brokers/1,
    register_broker/2
]).

-type start_ret() :: {ok, Cluster :: pid(), Brokers :: [kamock:broker()]}.

-spec start() -> start_ret().
start() ->
    start(make_ref()).

-spec start(Ref :: kamock:ref()) -> start_ret().
start(Ref) ->
    NodeIds = [101, 102, 103],
    start(Ref, NodeIds).

-spec start(Ref :: kamock:ref(), NodeIds :: [kamock:node_id()]) -> start_ret().
start(Ref, NodeIds) when is_list(NodeIds) ->
    start(Ref, NodeIds, #{}).

-spec start(Ref :: kamock:ref(), NodeIds :: [kamock:node_id()], Options :: map()) -> start_ret().
start(Ref, NodeIds, Options) when is_list(NodeIds), is_map(Options) ->
    start(Ref, NodeIds, Options, []).

start(Ref, NodeIds, Options, SocketOpts) ->
    % We need a place to store the port numbers for the brokers in the "cluster", otherwise we can't respond to a
    % Metadata request properly. That information lives in 'Cluster'.
    {ok, Cluster} = gen_statem:start(?MODULE, [], []),
    Brokers = start_brokers(Ref, Cluster, NodeIds, Options#{node_ids => NodeIds}, SocketOpts, []),
    {ok, Cluster, Brokers}.

start_tls(Ref, NodeIds, Options, SocketOpts) ->
    start(Ref, NodeIds, Options#{transport => ranch_ssl}, SocketOpts).

start_brokers(Ref, Cluster, [NodeId | NodeIds], Options, SocketOpts, Brokers) ->
    {ok, Broker} = kamock_broker:start(
        {Ref, NodeId},
        Options#{node_id => NodeId},
        SocketOpts,
        Cluster
    ),
    start_brokers(Ref, Cluster, NodeIds, maps:without([port], Options), SocketOpts, [
        Broker | Brokers
    ]);
start_brokers(_Ref, _Cluster, [], _Options, _SocketOpts, Brokers) ->
    lists:reverse(Brokers).

stop(Cluster) when is_pid(Cluster) ->
    % We don't support stopping a cluster by ref, because that would require some kind of process registry, and we don't
    % want to take a dependency on, say, gproc, or whatever. We can't use 'local', because we allow `any()` as the ref.
    gen_statem:stop(Cluster).

-record(state, {
    brokers
}).

init([]) ->
    process_flag(trap_exit, true),

    StateData = #state{
        brokers = []
    },
    {ok, ready, StateData}.

callback_mode() ->
    [handle_event_function].

handle_event(
    {call, From}, get_brokers, _State, _StateData = #state{brokers = Brokers}
) ->
    {keep_state_and_data, [{reply, From, lists:sort(fun order_brokers_by_node_id/2, Brokers)}]};
handle_event(
    {call, From}, {register_broker, Broker}, _State, StateData = #state{brokers = Brokers}
) ->
    StateData2 = StateData#state{brokers = [Broker | Brokers]},
    {keep_state, StateData2, [{reply, From, ok}]}.

terminate(_Reason, _State, _StateData = #state{brokers = Brokers}) ->
    lists:foreach(fun kamock_broker:stop/1, Brokers),
    ok.

order_brokers_by_node_id(#{node_id := A}, #{node_id := B}) ->
    A =< B.

get_brokers(Cluster) ->
    call(Cluster, get_brokers, []).

register_broker(Cluster, Broker) ->
    call(Cluster, {register_broker, Broker}, ok).

call(Cluster, Request, _Default) when Cluster /= undefined ->
    gen_statem:call(Cluster, Request);
call(_Cluster, _Request, Default) ->
    Default.
