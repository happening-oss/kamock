-module(kamock_broker_tls_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

client_cert_test() ->
    CAKey = erl509_private_key:create_rsa(4096),
    CACert = erl509_certificate:create_self_signed(
        CAKey, <<"CN=ca">>, erl509_certificate_template:root_ca()
    ),

    ServerKey = erl509_private_key:create_rsa(2048),
    ServerPub = erl509_public_key:derive_public_key(ServerKey),
    ServerCert = erl509_certificate:create(
        ServerPub, <<"CN=localhost">>, CACert, CAKey, erl509_certificate_template:server()
    ),

    Ref = make_ref(),
    {ok, Broker} = kamock_broker:start_tls(Ref, #{}, [
        {cert, erl509_certificate:to_der(ServerCert)},
        {key, {'RSAPrivateKey', erl509_private_key:to_der(ServerKey)}},
        % Client certs are optional.
        {verify, verify_peer},
        {fail_if_no_peer_cert, false},
        {cacerts, [erl509_certificate:to_der(CACert)]}
    ]),

    #{host := Host, port := Port} = Broker,

    ClientKey = erl509_private_key:create_rsa(2048),
    ClientPub = erl509_public_key:derive_public_key(ClientKey),
    ClientCert = erl509_certificate:create(
        ClientPub, <<"CN=client">>, CACert, CAKey, erl509_certificate_template:client()
    ),

    % Connect to the broker. We'll use 'ssl', because kafcod_connection doesn't support TLS.
    {ok, S} = ssl:connect(binary_to_list(Host), Port, [
        {cert, erl509_certificate:to_der(ClientCert)},
        {key, {'RSAPrivateKey', erl509_private_key:to_der(ClientKey)}},
        {verify, verify_peer},
        {cacerts, [erl509_certificate:to_der(CACert)]},
        {verify, verify_none}
    ]),

    [#{transport := ranch_ssl, socket := SslSocket}] = kamock_broker:connections(Broker),
    {ok, PeerCertDer} = ssl:peercert(SslSocket),
    #'OTPCertificate'{
        tbsCertificate = #'OTPTBSCertificate'{
            subject = PeerSubject
        }
    } = public_key:pkix_decode_cert(PeerCertDer, otp),
    ?assertEqual(
        {rdnSequence, [
            [{'AttributeTypeAndValue', ?'id-at-commonName', {printableString, "client"}}]
        ]},
        PeerSubject
    ),

    ok = ssl:close(S),
    kamock_broker:stop(Broker),
    ok.
