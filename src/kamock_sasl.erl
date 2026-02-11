-module(kamock_sasl).
-export([
    require_sasl/0
]).

-export([
    handshake/0,
    handshake/1,

    authenticate/0,
    authenticate/3
]).
-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").

require_sasl() ->
    fun
        (ApiKey, ApiVersion, Packet, Env) when
            ApiKey =:= ?API_VERSIONS; ApiKey =:= ?SASL_HANDSHAKE; ApiKey =:= ?SASL_AUTHENTICATE
        ->
            % ApiVersions can be sent before SASL auth, so we allow that.
            meck:passthrough([ApiKey, ApiVersion, Packet, Env]);
        (ApiKey, ApiVersion, Packet, Env = #{principal := _}) ->
            % If you've already authenticated (principal is set), then carry on.
            meck:passthrough([ApiKey, ApiVersion, Packet, Env]);
        (_ApiKey, _ApiVersion, _Packet, _Env) ->
            % Otherwise stop.
            stop
    end.

handshake() ->
    % Even if the client gives a specific mechanism in the SaslHandshake request, the broker returns all of the enabled
    % mechanisms. We'll claim to support PLAIN and SCRAM-SHA-256.
    handshake([<<"PLAIN">>, <<"SCRAM-SHA-256">>]).

handshake(Mechanisms) ->
    fun(
        _Request = #{correlation_id := CorrelationId}, Env
    ) ->
        {reply,
            #{
                correlation_id => CorrelationId,
                error_code => ?NONE,
                mechanisms => Mechanisms
            },
            Env#{sasl_cache => expect_client_first}}
    end.

authenticate() ->
    Username = <<"admin">>,
    Password = <<"secret">>,
    authenticate(Username, Password, sha256).

authenticate(Username, Password, Algorithm) ->
    IterationCount = 4096,

    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, #{
        algorithm => Algorithm, iteration_count => IterationCount
    }),

    % Look up the username.
    RetrieveFun = fun
        (U) when U =:= Username ->
            {ok, #{
                stored_key => StoredKey,
                server_key => ServerKey,
                salt => Salt
            }};
        (_) ->
            {error, unknown_user}
    end,

    % SASL authentication requires multiple round-trips. In the case of SCRAM, two trips. So we use 'Env' to cache some
    % state between the two trips, and we need two function clauses here, one for each round-trip.
    fun
        (
            _Request = #{
                correlation_id := CorrelationId,
                auth_bytes := ClientFirstMessage
            },
            Env = #{sasl_cache := expect_client_first}
        ) ->
            case
                sasl_auth_scram:check_client_first_message(ClientFirstMessage, #{
                    iteration_count => IterationCount, retrieve => RetrieveFun
                })
            of
                {continue, ServerFirstMessage, ServerCache} ->
                    Response = #{
                        error_code => ?NONE,
                        error_message => <<>>,
                        correlation_id => CorrelationId,
                        auth_bytes => ServerFirstMessage,
                        session_lifetime_ms => 0
                    },

                    {reply, Response, Env#{sasl_cache => ServerCache}};
                ignore ->
                    Response = #{
                        error_code => ?SASL_AUTHENTICATION_FAILED,
                        error_message => <<"Computer says no">>,
                        correlation_id => CorrelationId,
                        auth_bytes => <<>>,
                        session_lifetime_ms => 0
                    },

                    {reply, Response, maps:remove(sasl_cache, Env)}
            end;
        (
            _Request = #{correlation_id := CorrelationId, auth_bytes := ClientFinalMessage},
            Env = #{sasl_cache := ServerCache}
        ) ->
            case
                sasl_auth_scram:check_client_final_message(ClientFinalMessage, ServerCache#{
                    algorithm => Algorithm
                })
            of
                {ok, ServerFinalMessage} ->
                    Response = #{
                        error_code => ?NONE,
                        error_message => <<>>,
                        correlation_id => CorrelationId,
                        auth_bytes => ServerFinalMessage,
                        session_lifetime_ms => 24 * 60 * 60 * 1000
                    },
                    {reply, Response, maps:remove(sasl_cache, Env#{principal => Username})};

                {error, Error} ->
                    Response = #{
                        error_code => ?SASL_AUTHENTICATION_FAILED,
                        error_message => iolist_to_binary(io_lib:format("~p", [Error])),
                        correlation_id => CorrelationId,
                        auth_bytes => <<>>,
                        session_lifetime_ms => 0
                    },
                    {reply, Response, maps:remove(sasl_cache, Env)}
            end
    end.
