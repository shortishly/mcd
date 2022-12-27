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


-module(mcd_protocol_text_noreply_SUITE).


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


add_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := Value},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => add,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => Value},

          #{command => get,
            keys => [Key]}])).


set_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := Value},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => Value},

          #{command => get,
            keys => [Key]}])).


replace_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    V1 = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := V1},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V0},

          #{command => replace,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V1},

          #{command => get,
            keys => [Key]}])).


append_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    V1 = alpha(5),
    Combined = iolist_to_binary([V0, V1]),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := Combined},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V0},

          #{command => append,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V1},

          #{command => get,
            keys => [Key]}])).


prepend_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    V1 = alpha(5),
    Combined = iolist_to_binary([V1, V0]),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := Combined},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V0},

          #{command => prepend,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => V1},

          #{command => get,
            keys => [Key]}])).


cas_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    V1 = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => false,
            data => V0})),

    [#{command := value,
       key := Key,
       cas := CAS,
       flags := Flags,
       data := V0},
     #{command := 'end'}] = send_sync(
                              Config,
                              #{command => gets,
                                keys => [Key]}),

    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := V1},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => cas,
            key => Key,
            flags => Flags,
            expiry => 0,
            unique => CAS,
            noreply => true,
            data => V1},

          #{command => get,
            keys => [Key]}])).


incr_test(Config) ->
    Key = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          data := <<"123">>,
          flags := Flags,
          key := Key},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => <<"122">>},

          #{command => incr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


decr_test(Config) ->
    Key = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          data := <<"121">>,
          flags := Flags,
          key := Key},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => <<"122">>},

          #{command => decr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


decr_non_numeric_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          data := Value,
          flags := Flags,
          key := Key},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => Value},

          #{command => decr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


incr_non_numeric_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       [#{command := value,
          data := Value,
          flags := Flags,
          key := Key},
        #{command := 'end'}],
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => Value},

          #{command => incr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


decr_not_found_test(Config) ->
    Key = alpha(5),

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         [#{command => decr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


incr_not_found_test(Config) ->
    Key = alpha(5),

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         [#{command => incr,
            key => Key,
            noreply => true,
            value => 1},

          #{command => get,
            keys => [Key]}])).


delete_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         [#{command => set,
            key => Key,
            flags => Flags,
            expiry => 0,
            noreply => true,
            data => Value},

          #{command => delete,
            key => Key,
            noreply => true},

          #{command => get,
            keys => [Key]}])).


delete_not_found_test(Config) ->
    Key = alpha(5),

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         [#{command => delete,
            key => Key,
            noreply => true},

          #{command => get,
            keys => [Key]}])).


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
