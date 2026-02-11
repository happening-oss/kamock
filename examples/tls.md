# TLS Listener

If you want to test your client against a broker with TLS enabled, `kamock` can do that as well:

```erlang
Ref = make_ref().
SocketOpts = [
        {certfile, "server.crt"},
        {keyfile, "server.key"},
        % Client certs aren't used.
        {verify, verify_none}
    ].
{ok, Broker} = kamock_broker:start_tls(Ref, #{port => 9993}, SocketOpts).
```

However, you'll need to mess around with `openssl` to generate certificates, and all the usual shenanigans apply.

You'll have more luck in your unit and integration tests if you use `erl509` to generate throwaway certificates and
keys. See the `kamock_broker_tls_tests.erl` and `kamock_cluster_tls_tests.erl` files for examples.
