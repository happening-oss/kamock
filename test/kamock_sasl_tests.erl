-module(kamock_sasl_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun require_sasl/0,
        fun scram_sha256/0,
        fun scram_sha512/0,
        % fun wrong_mechanism/0,
        fun invalid_username/0,
        fun invalid_password/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

require_sasl() ->
    % Quick demonstration of dropping the connection for anything except SASL calls.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(kamock_broker_handler, handle_request, kamock_sasl:require_sasl()),

    ?assertException(
        exit,
        _,
        kafcod_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => null,
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    ),

    % Fails with 'noproc', as expected:
    % kafcod_connection:stop(C),

    kamock_broker:stop(Broker),
    ok.

scram_sha256() ->
    scram_with_mechanism(<<"SCRAM-SHA-256">>, sha256).

scram_sha512() ->
    scram_with_mechanism(<<"SCRAM-SHA-512">>, sha512).

scram_with_mechanism(Mechanism, Algorithm) ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    Username = <<"admin">>,
    Password = <<"secret">>,

    meck:expect(kamock_broker_handler, handle_request, kamock_sasl:require_sasl()),
    meck:expect(kamock_sasl_handshake, handle_sasl_handshake_request, kamock_sasl:handshake([Mechanism])),
    meck:expect(
        kamock_sasl_authenticate, handle_sasl_authenticate_request, kamock_sasl:authenticate(Username, Password, Algorithm)
    ),

    {ok, #{error_code := 0, mechanisms := _Mechanisms}} = kafcod_connection:call(
        C,
        fun sasl_handshake_request:encode_sasl_handshake_request_1/1,
        #{mechanism => Mechanism},
        fun sasl_handshake_response:decode_sasl_handshake_response_1/1
    ),

    ClientFirst = sasl_auth_scram:client_first_message(Username),

    {ok, #{error_code := ?NONE, auth_bytes := ServerFirst}} = kafcod_connection:call(
        C,
        fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
        #{auth_bytes => ClientFirst},
        fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1
    ),

    {continue, ClientFinal, ClientCache} =
        sasl_auth_scram:check_server_first_message(
            ServerFirst,
            #{
                client_first_message => ClientFirst,
                password => Password,
                algorithm => Algorithm
            }
        ),

    {ok, #{error_code := ?NONE, auth_bytes := ServerFinal}} = kafcod_connection:call(
        C,
        fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
        #{auth_bytes => ClientFinal},
        fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1
    ),

    ok = sasl_auth_scram:check_server_final_message(ServerFinal, ClientCache#{algorithm => Algorithm}),

    [#{env := #{principal := <<"admin">>}}] = kamock_broker:connections(Broker),

    % Get Metadata (this works):
    {ok, _} = kafcod_connection:call(
        C,
        fun metadata_request:encode_metadata_request_9/1,
        #{
            topics => null,
            allow_auto_topic_creation => false,
            include_cluster_authorized_operations => false,
            include_topic_authorized_operations => false
        },
        fun metadata_response:decode_metadata_response_9/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

invalid_username() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(kamock_broker_handler, handle_request, kamock_sasl:require_sasl()),
    meck:expect(kamock_sasl_handshake, handle_sasl_handshake_request, kamock_sasl:handshake()),
    meck:expect(
        kamock_sasl_authenticate, handle_sasl_authenticate_request, kamock_sasl:authenticate()
    ),

    {ok, #{error_code := 0, mechanisms := _Mechanisms}} = kafcod_connection:call(
        C,
        fun sasl_handshake_request:encode_sasl_handshake_request_1/1,
        #{mechanism => <<"SCRAM-SHA-256">>},
        fun sasl_handshake_response:decode_sasl_handshake_response_1/1
    ),

    Username = <<"mallory">>,
    _Password = <<"wild-guess">>,

    ClientFirst = sasl_auth_scram:client_first_message(Username),

    {ok, #{error_code := ?SASL_AUTHENTICATION_FAILED}} = kafcod_connection:call(
        C,
        fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
        #{auth_bytes => ClientFirst},
        fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

invalid_password() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(kamock_broker_handler, handle_request, kamock_sasl:require_sasl()),
    meck:expect(kamock_sasl_handshake, handle_sasl_handshake_request, kamock_sasl:handshake()),
    meck:expect(
        kamock_sasl_authenticate, handle_sasl_authenticate_request, kamock_sasl:authenticate()
    ),

    {ok, #{error_code := 0, mechanisms := _Mechanisms}} = kafcod_connection:call(
        C,
        fun sasl_handshake_request:encode_sasl_handshake_request_1/1,
        #{mechanism => <<"SCRAM-SHA-256">>},
        fun sasl_handshake_response:decode_sasl_handshake_response_1/1
    ),

    Username = <<"admin">>,
    Password = <<"wild-guess">>,

    ClientFirst = sasl_auth_scram:client_first_message(Username),

    {ok, #{error_code := ?NONE, auth_bytes := ServerFirst}} = kafcod_connection:call(
        C,
        fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
        #{auth_bytes => ClientFirst},
        fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1
    ),

    {continue, ClientFinal, _ClientCache} =
        sasl_auth_scram:check_server_first_message(
            ServerFirst,
            #{
                client_first_message => ClientFirst,
                password => Password,
                algorithm => sha256
            }
        ),

    {ok, #{error_code := ?SASL_AUTHENTICATION_FAILED}} = kafcod_connection:call(
        C,
        fun sasl_authenticate_request:encode_sasl_authenticate_request_1/1,
        #{auth_bytes => ClientFinal},
        fun sasl_authenticate_response:decode_sasl_authenticate_response_1/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.
