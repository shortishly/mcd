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


-module(mcd_protocol_text_incrdecr_SUITE).


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


bug21_test(Config) ->
    K = alpha(5),
    Flags = 0,

    MaxInt64 = integer_to_binary(mcd_util:max(int64)),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => MaxInt64})),

    [#{command := value,
       key := K,
       cas := _,
       flags := Flags,
       data := MaxInt64},
     #{command := 'end'}] = send_sync(
                              Config,
                              #{command => gets,
                                keys => [K]}),

    MaxInt64Plus1 = mcd_util:max(int64) + 1,

    ?assertMatch(
       #{command := incrdecr,
         value := MaxInt64Plus1},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})),

    MaxInt64Plus2 = mcd_util:max(int64) + 2,

    ?assertMatch(
       #{command := incrdecr,
         value := MaxInt64Plus2},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})),

    ?assertMatch(
       #{command := incrdecr,
         value := MaxInt64Plus1},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 1})).


incr_decr_test(Config) ->
    K = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => "1"})),

    ?assertMatch(
       #{command := incrdecr,
         value := 2},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})),

    ?assertMatch(
       #{command := incrdecr,
         value := 10},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 8})),

    ?assertMatch(
       #{command := incrdecr,
         value := 9},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 1})),

    ?assertMatch(
       #{command := incrdecr,
         value := 0},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 9})),

    ?assertMatch(
       #{command := incrdecr,
         value := 0},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 5})).

incr_uint32_test(Config) ->
    K = alpha(5),
    Flags = 0,

    Max32 = integer_to_binary(mcd_util:max(uint32)),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => Max32})),

    Max32Plus1 = mcd_util:max(uint32) + 1,

    ?assertMatch(
       #{command := incrdecr,
         value := Max32Plus1},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})),

    Max32Plus2 = mcd_util:max(uint32) + 2,

    ?assertMatch(
       #{command := incrdecr,
         value := Max32Plus2},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})).


wrap_incr_max_uint64_test(Config) ->
    K = alpha(5),
    Flags = 0,

    Max64 = integer_to_binary(mcd_util:max(uint64)),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => Max64})),

    ?assertMatch(
       #{command := incrdecr,
         value := 0},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})).


incr_not_found_test(Config) ->
    K = alpha(5),

    ?assertMatch(
       #{command := not_found},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})).


decr_not_found_test(Config) ->
    K = alpha(5),

    ?assertMatch(
       #{command := not_found},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 1})).


non_numeric_incr_client_error_test(Config) ->
    K = alpha(5),
    V = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => V})),

    ?assertMatch(
       #{command := client_error},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})).


big_incr_test(Config) ->
    K = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => "1"})),

    ?assertMatch(
       #{command := incrdecr,
         value := 0},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => mcd_util:max(uint64)})).


non_numeric_decr_client_error_test(Config) ->
    K = alpha(5),
    V = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => V})),

    ?assertMatch(
       #{command := client_error},
       send_sync(
         Config,
         #{command => decr,
           key => K,
           noreply => false,
           value => 1})).


send_sync(Config, Data) ->
    Client = ?config(client, Config),

    {reply, Reply} = gen_statem:receive_response(
                       mcd_client:send(
                         #{server_ref => Client,
                           data => Data})),

    Reply.


request(Opcode) ->
    #{meta => Opcode}.


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
