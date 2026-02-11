-module(kamock_sasl_authenticate).
-export([handle_sasl_authenticate_request/2]).
-include_lib("kafcod/include/error_code.hrl").

handle_sasl_authenticate_request(
    _SaslAuthenticateRequest = #{correlation_id := CorrelationId},
    _Env
) ->
    % These are reasonable defaults for a single-round trip of PLAIN. They'll do.
    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        error_message => <<>>,
        auth_bytes => <<>>,
        session_lifetime_ms => 0
    }.
