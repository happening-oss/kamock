# Example: SASL authentication

By default, the mock broker doesn't require any authentication.

If, however, you want to test your client's implementation of SASL authentication, you can require SCRAM-SHA256, as
follows:

```erlang
kamock_broker:start(make_ref(), #{port => 9990}).

meck:expect(kamock_broker_handler, handle_request, kamock_sasl:require_sasl()).
meck:expect(kamock_sasl_handshake, handle_sasl_handshake_request, kamock_sasl:handshake()).
meck:expect(kamock_sasl_authenticate, handle_sasl_authenticate_request, kamock_sasl:authenticate(<<"admin">>, <<"secret">>)).
```

Then you can test with `kcat`:

```sh
kcat -b localhost:9990 \
    -X security.protocol=sasl_plaintext \
    -X sasl.mechanism=SCRAM-SHA-256 \
    -X sasl.username=admin \
    -X sasl.password=secret \
    -L
```
