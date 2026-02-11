-module(kamock_sasl_handshake).
-export([handle_sasl_handshake_request/2]).
-include_lib("kafcod/include/error_code.hrl").

handle_sasl_handshake_request(
    _SaslHandshakeRequest = #{correlation_id := CorrelationId},
    _Env
) ->
    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        mechanisms => []
    }.
