-module(kamock_broker_protocol).
-behaviour(ranch_protocol).
-export([
    start_link/3,
    start_link/4
]).
-behaviour(gen_statem).
-export([
    init/1,
    callback_mode/0,
    handle_event/4
]).

-include_lib("kernel/include/logger.hrl").

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

start_link(Ref, Transport, Opts) ->
    gen_statem:start_link(?MODULE, [Ref, Transport, Opts], []).

% start_link/4 is for backward compatibility with Ranch 1.8.x
start_link(Ref, _Socket, Transport, Opts) ->
    start_link(Ref, Transport, Opts).

callback_mode() ->
    [state_enter, handle_event_function].

-type transport_messages() :: {OK :: atom(), Closed :: atom(), Error :: atom(), Passive :: atom()}.

-record(state, {
    ref,
    transport,
    messages :: transport_messages(),
    socket :: ranch_transport:socket(),
    env,
    metadata :: telemetry:event_metadata(),
    handler :: module()
}).

init([Ref, Transport, _Opts = #{handler := Handler, env := Env}]) ->
    #{node_id := NodeId} = Env,
    StateData = #state{
        ref = Ref,
        transport = Transport,
        messages = init_messages(Transport),
        env = Env,
        metadata = #{ref => Ref, node_id => NodeId},
        handler = Handler
    },
    {ok, connected, StateData}.

init_messages(Transport = ranch_tcp) ->
    % Backwards compatibility: Ranch 2.x adds the passive message for {active, N}.
    case Transport:messages() of
        {Tcp, Closed, Error} ->
            {Tcp, Closed, Error, tcp_passive};
        Messages ->
            Messages
    end.

handle_event(
    enter,
    connected,
    _State,
    StateData = #state{ref = Ref, transport = Transport, env = Env, metadata = Metadata}
) ->
    % Per the ranch documentation, we call handshake from the enter clause of our initial state.
    % See https://ninenines.eu/docs/en/ranch/2.1/guide/protocols/#_using_gen_statem_and_gen_server
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{mode, binary}, {packet, 4}, {active, true}]),
    {ok, {_, Port}} = Transport:sockname(Socket),
    Env2 = Env#{host => <<"localhost">>, port => Port},
    telemetry:execute([kamock, protocol, connected], #{}, Metadata),
    {keep_state, StateData#state{socket = Socket, env = Env2}};
handle_event(
    info,
    {Tcp, Socket, Packet},
    State,
    StateData = #state{
        env = Env, handler = Handler, messages = {Tcp, _Closed, _Error, _Passive}, socket = Socket
    }
) ->
    % We need to peek at the API Key and API Version before we know what it is, and therefore which decoder to use.
    <<ApiKey:16/big, ApiVersion:16/big, _/binary>> = Packet,
    ?LOG_DEBUG("Got request with API key ~B", [ApiKey]),
    handle_request_result(
        Handler:handle_request(ApiKey, ApiVersion, Packet, Env), State, StateData
    );
handle_event(
    info,
    {Closed, Socket},
    _State,
    _StateData = #state{
        messages = {_Tcp, Closed, _Error, _Passive}, socket = Socket
    }
) ->
    stop;
handle_event(
    info,
    {Error, Socket},
    _State,
    _StateData = #state{
        messages = {_Tcp, _Closed, Error, _Passive}, socket = Socket
    }
) ->
    stop.

handle_request_result(
    {reply, Reply},
    _State,
    _StateData = #state{transport = Transport, socket = Socket}
) ->
    Transport:send(Socket, Reply),
    keep_state_and_data;
handle_request_result(
    noreply,
    _State,
    _StateData
) ->
    keep_state_and_data;
handle_request_result(
    {stop, Reason},
    _State,
    _StateData
) ->
    {stop, Reason}.
