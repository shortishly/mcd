# Erlang Memcached Server (and Client)

[mcd][mcd-github] is a [memcached][memcached-org] compatible API
server written in [Erlang/OTP 25][erlang-org] using
[ETS][ets-erlang-org] to maintain the cache of data.

It has a TCP [memcached][memcached-org] compatible API on port 11211
offering:

- the original text based [standard
  protocol][memcached-protocol-text];
- the [binary protocol][memcached-protocol-binary];
- the text based [meta protocol][memcached-protocol-meta] (which
  also deprecates the binary protocol).
  
![main](https://github.com/shortishly/mcd/actions/workflows/main.yml/badge.svg)
  
## Build

[mcd][mcd-github] uses [erlang.mk][erlang-mk] with [Erlang/OTP
25][erlang-org] to build:

```shell
make
```

## Run

To run [mcd][mcd-github] with an [Erlang Shell][erlang-shell]
listening on port 11211:

```shell
make shell
```

## Docker

To run [mcd][mcd-github] as a docker container:

```shell
docker run \
    --pull always \
    --detach \
    --publish 11211:11211 \
    --rm \
    ghcr.io/shortishly/mcd:0.1.0
```


## API Example

The text based API is the easiest to try out from the command line
with a simple set and get:

```shell
telnet localhost 11211
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.

set foo 0 0 6 
fooval
STORED
get foo
VALUE foo 0 6
fooval
END
```


The implementation uses some recently introduced features of
[Erlang/OTP][erlang-org]:

- [socket][erlang-org-socket] for all communication
- [send_request][erlang-org-send-request-4] for asynchronous request
  and response.
- the [timeout][erlang-org-statem-timeout] feature of
  [statem][erlang-org-statem] to expire items from the cache.



[erlang-shell]: https://www.erlang.org/doc/man/shell.html
[erlang-mk]: https://erlang.mk
[erlang-org-statem]: https://www.erlang.org/doc/man/gen_statem.html
[erlang-org-statem-timeout]: https://www.erlang.org/doc/man/gen_statem.html#type-timeout_event_type
[erlang-org-send-request-4]: https://www.erlang.org/doc/man/gen_statem.html#send_request-4
[erlang-org-socket]: https://www.erlang.org/doc/man/socket.html
[erlang-org]: https://www.erlang.org
[ets-erlang-org]: https://www.erlang.org/doc/man/ets.html
[mcd-github]: https://github.com/shortishly/mcd
[memcached-github]: https://github.com/memcached/memcached
[memcached-org]: https://memcached.org/
[memcached-protocol-binary]: https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped
[memcached-protocol-meta]: https://github.com/memcached/memcached/wiki/MetaCommands
[memcached-protocol-text]: https://github.com/memcached/memcached/wiki/Commands
[memcached-wikipedia]: https://en.wikipedia.org/wiki/Memcached
