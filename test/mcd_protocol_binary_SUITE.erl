%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(mcd_protocol_binary_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(mcd),
    application:set_env(mcd, protocol_callback, mcd_emulator),
    {ok, _} = mcd:start(),
    {ok, Client} = mcd_client:start(),
    [{client, Client} | Config].


end_per_suite(Config) ->
    Client = ?config(client, Config),
    ok = gen_statem:stop(Client),
    ok = application:stop(mcd).


flush_test(Config) ->
    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config, request(flush))).


no_op_test(Config) ->
    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config, request(no_op))).


set_test(Config) ->
    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config,
                 #{header => header(set),
                   extra => #{flags => uint(32), expiration => 19},
                   key => alpha(5)})).


set_get_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = uint(32),
    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config,
                 #{header => header(set),
                   extra => #{flags => Flags, expiration => 19},
                   key => Key,
                   value => Value})),
    ?assertMatch(
       #{header := #{status := no_error},
         extra := #{flags := Flags},
         value := Value},
       send_sync(Config,
                 #{header => header(get),
                   key => Key})).

set_get_flush_get_key_not_found_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = uint(32),

    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config,
                 #{header => header(set),
                   extra => #{flags => Flags, expiration => 19},
                   key => Key,
                   value => Value})),

    ?assertMatch(
       #{header := #{status := no_error},
         extra := #{flags := Flags},
         value := Value},
       send_sync(Config,
                 #{header => header(get),
                   key => Key})),

    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config, request(flush))),

    ?assertMatch(
       #{header := #{status := key_not_found}},
       send_sync(Config,
                 #{header => header(get),
                   key => Key})).

set_get_delete_get_key_not_found_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = uint(32),

    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config,
                 #{header => header(set),
                   extra => #{flags => Flags, expiration => 19},
                   key => Key,
                   value => Value})),

    ?assertMatch(
       #{header := #{status := no_error},
         extra := #{flags := Flags},
         value := Value},
       send_sync(Config,
                 #{header => header(get),
                   key => Key})),

    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config, request(delete, Key))),

    ?assertMatch(
       #{header := #{status := key_not_found}},
       send_sync(Config,
                 #{header => header(get),
                   key => Key})).


delete_test(Config) ->
    ?assertMatch(
       #{header := #{status := no_error}},
       send_sync(Config, request(delete, alpha(5)))).


key_not_found_get_test(Config) ->
    ?assertMatch(
       #{header := #{status := key_not_found}},
       send_sync(Config, request(get, alpha(5)))).


%% TODO
%% key_not_found_replace_test(Config) ->
%%     ?assertMatch(
%%        #{header := #{status := key_not_found}},
%%        send_sync(Config,
%%                  #{header => header(replace),
%%                    extra => #{flags => 0, expiration => 0},
%%                    key => alpha(5)})).


request(Header) ->
    #{header => header(Header)}.


request(Header, Key) ->
    #{header => header(Header), key => Key}.


header(#{} = Arg) ->
    maps:merge(
      Arg,
      #{magic => request,
        data_type => raw,
        vbucket_id => 0,
        opaque => 0,
        cas => 0});

header(Opcode) when is_atom(Opcode) ->
    ?FUNCTION_NAME(#{opcode => Opcode}).


send_sync(Config, Data) ->
    Client = ?config(client, Config),

    {reply, Reply} = gen_statem:receive_response(
                       mcd_client:send(
                         #{server_ref => Client,
                           data => Data})),

    Reply.


alpha(N) ->
    list_to_binary(pick(N, lists:seq($a, $z))).


pick(N, Pool) ->
    ?FUNCTION_NAME(N, Pool, []).


pick(0, _, A) ->
    A;

pick(N, Pool, A) ->
    ?FUNCTION_NAME(N - 1,
                   Pool,
                   [lists:nth(rand:uniform(length(Pool)), Pool) | A]).


uint(32) ->
    random(mcd_util:max(uint32)).


random(High) ->
    rand:uniform(High + 1) - 1.
