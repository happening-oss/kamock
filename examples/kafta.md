# Use with `kafta`

See https://github.com/electric-saw/kafta

> Kafta is a command line for managing Kafka clusters

## Configuration

In `~/.kafta/config`:

```
contexts:
    kamock:
        schema-registry: ""
        schemaregistryauth:
            key: ""
            secret: ""
        ksql: ""
        bootstrap-servers:
            - localhost:9990
        kafka-version: 3.3.0
        usesasl: false
        usetls: false
        tls:
            clientcertfile: ""
            clientkeyfile: ""
            cacertfile: ""
        sasl:
            algorithm: ""
            username: ""
            password: ""
```

Though -- if you were feeling particularly fancy -- you _could_ start kamock with TLS and SASL support.

## Describe Cluster

```sh
kafta --context kamock cluster describe
```

## Group Consumer

```sh
kafta --context kamock console consumer example
```
