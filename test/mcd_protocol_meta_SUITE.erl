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


-module(mcd_protocol_meta_SUITE).


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


no_op_test(Config) ->
    ?assertMatch(
       #{meta := no_op_reply},
       send_sync(Config, #{meta => no_op})).

ms_mg_test(Config) ->
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => "abc",
                   data => "pqr"})).

delete_with_cas_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),

    %% set test key
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => Key,
                   data => Value})),

    %% got a CAS value back
    #{flags := [{cas, Expected}],
      meta := head} =
        send_sync(Config,
                  #{meta => get,
                    flags => [cas],
                    key => Key}),

    %% delete fails for wrong CAS
    ?assertMatch(
       #{meta := exists},
       send_sync(Config,
                 #{meta => delete,
                   flags => [{cas_expected, Expected + 10}],
                   key => Key,
                   data => alpha(5)})),

    %% success on correct cas
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => delete,
                   flags => [{cas_expected, Expected}],
                   key => Key})).


basic_mset_cas_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),

    %% set test key
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => Key,
                   data => Value})),

    %% got a CAS value back
    #{flags := [{cas, Expected}],
      meta := head} =
        send_sync(Config,
                  #{meta => get,
                    flags => [cas],
                    key => Key}),

    %% zeroed out cas on return
    ?assertMatch(
       #{meta := exists,
        flags := [{cas, 0}]},
       send_sync(Config,
                 #{meta => set,
                   flags => [cas, {cas_expected, Expected + 10}],
                   key => Key,
                   data => alpha(5)})),

    %% success on correct cas
    ?assertMatch(
       #{meta := head,
        flags := [{cas, CAS}]} when CAS > Expected,
       send_sync(Config,
                 #{meta => set,
                   flags => [cas, {cas_expected, Expected}],
                   key => Key,
                   data => alpha(5)})).


arithmetic_incr_miss_test(Config) ->
    ?assertMatch(
       #{meta := not_found},
       send_sync(Config,
                 #{meta => arithmetic,
                   flags => [],
                   key => alpha(5)})).


arithmetic_incr_miss_with_delta_flag_test(Config) ->
    ?assertMatch(
       #{meta := not_found},
       send_sync(Config,
                 #{meta => arithmetic,
                   flags => [{delta, 1}],
                   key => alpha(5)})).


arithmetic_set_value_increment_with_value_flag_test(Config) ->
    Key = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => Key,
                   data => "1"})),
    ?assertMatch(
       #{meta := value,
         data := <<"2">>},
       send_sync(Config,
                 #{meta => arithmetic,
                   flags => [value],
                   key => Key})).


arithmetic_set_value_increment_test(Config) ->
    Key = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => Key,
                   data => "1"})),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => arithmetic,
                   flags => [],
                   key => Key})).


arithmetic_set_non_numeric_value_increment_test(Config) ->
    Key = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   flags => [],
                   key => Key,
                   data => alpha(5)})),
    ?assertMatch(
       #{command := client_error},
       send_sync(Config,
                 #{meta => arithmetic,
                   flags => [],
                   key => Key})).

arithmetic_increment_with_seed_test(Config) ->
    Key = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => arithmetic,
                   %% N90
                   flags => [{vivify, 90}],
                   key => Key})),

    %% TODO: key not being returned in a flag here
    ?assertMatch(
       #{flags := [{size, 1},
                   {ttl, TTL},
                   {opaque, <<"foo">>}],
         data := <<"0">>,
         meta := value}
       when TTL > 10 andalso TTL < 91,
            send_sync(Config,
                      #{meta => get,
                        %% s t v Ofoo k
                        flags => [size,
                                  ttl,
                                  value,
                                  {opaque, "foo"},
                                  key],
                        key => Key})),

    ?assertMatch(
       #{flags := [{ttl, TTL}],
         data := <<"1">>,
         meta := value}
       when TTL > 150 andalso TTL < 301,
            send_sync(Config,
                      #{meta => arithmetic,
                        %% T300 v t
                        flags => [{set_ttl, 300},
                                  value,
                                  ttl],
                        key => Key})).


arithmetic_seeded_on_missed_test(Config) ->
    Key = alpha(5),
    ?assertMatch(
       #{meta := value,
         data := <<"13">>,
         flags := [{ttl, 0}]},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 J13 v t
                        flags => [{vivify, 0},
                                  {initial, 13},
                                  value,
                                  ttl],
                        key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<"14">>,
         flags := [{ttl, 0}]},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 J13 v t
                        flags => [{vivify, 0},
                                  {initial, 13},
                                  value,
                                  ttl],
                        key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<"44">>,
         flags := [{ttl, 0}]},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 J13 v t D30
                        flags => [{vivify, 0},
                                  {initial, 13},
                                  value,
                                  ttl,
                                  {delta, 30}],
                        key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<"22">>,
         flags := [{ttl, 0}]},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 J13 v t MD D22
                        flags => [{vivify, 0},
                                  {initial, 13},
                                  value,
                                  ttl,
                                  {mode, "D"},
                                  {delta, 22}],
                        key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<"0">>,
         flags := [{ttl, 0}]},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 J13 v t MD D9000
                        flags => [{vivify, 0},
                                  {initial, 13},
                                  value,
                                  ttl,
                                  {mode, "D"},
                                  {delta, 9000}],
                        key => Key})),

    ?assertMatch(
       #{meta := no_op_reply},
            send_sync(Config,
                      [#{meta => arithmetic,
                         %% flags: q
                         flags => [noreply],
                         key => Key},

                       #{meta => no_op}])).


arithmetic_cas_test(Config) ->
    Key = alpha(5),
    #{meta := value,
      data := <<"0">>,
      flags := [{cas, CAS}]} =
        send_sync(Config,
                  #{meta => arithmetic,
                    %% flags: N0 c v
                    flags => [{vivify, 0},
                              cas,
                              value],
                    key => Key}),

    ?assertMatch(
       #{meta := exists},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 C99999 v
                        flags => [{vivify, 0},
                                  {cas_expected, 99_999},
                                  value],
                        key => Key})),

    ?assertMatch(
       #{meta := value, data := <<"1">>},
            send_sync(Config,
                      #{meta => arithmetic,
                        %% flags: N0 C<CAS> c v
                        flags => [{vivify, 0},
                                  {cas_expected, CAS},
                                  cas,
                                  value],
                        key => Key})).


add_followed_by_add_is_not_stored_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120
                   flags => [{set_ttl, 120}],
                   data => Value,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := Value,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})),

    ?assertMatch(
       #{meta := not_stored},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 ME
                   flags => [{set_ttl, 120},
                             {mode, "E"}],
                   data => alpha(5),
                   key => Key})).

set_append_prepend_test(Config) ->
    Size = 5,
    Key = alpha(Size),
    V0 = alpha(Size),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120
                   flags => [{set_ttl, 120}],
                   data => V0,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := V0,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})),

    V1 = alpha(Size),

    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 MA
                   flags => [{set_ttl, 120},
                             {mode, "A"}],
                   data => V1,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<V0:Size/bytes, V1:Size/bytes>>,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})),

    V2 = alpha(Size),

    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 MA
                   flags => [{set_ttl, 120},
                             {mode, "P"}],
                   data => V2,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := <<V2:Size/bytes, V0:Size/bytes, V1:Size/bytes>>,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})).


replace_not_stored_test(Config) ->
    ?assertMatch(
       #{meta := not_stored},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 MR
                   flags => [{set_ttl, 120},
                             {mode, "R"}],
                   data => alpha(5),
                   key => alpha(5)})).


add_followed_by_replace_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120
                   flags => [{set_ttl, 120}],
                   data => V0,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := V0,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})),

    V1 = alpha(5),

    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 ME
                   flags => [{set_ttl, 120},
                             {mode, "R"}],
                   data => V1,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := V1,
        flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})).


explicit_forced_set_mode_test(Config) ->
    Key = alpha(5),
    V0 = alpha(5),
    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120
                   flags => [{set_ttl, 120}],
                   data => V0,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := V0,
         flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})),

    V1 = alpha(5),

    ?assertMatch(
       #{meta := head},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 ME
                   flags => [{set_ttl, 120},
                             {mode, "S"}],
                   data => V1,
                   key => Key})),

    ?assertMatch(
       #{meta := value,
         data := V1,
         flags := [{size, _}]},
       send_sync(Config,
                 #{meta => get,
                   %% flags: s v
                   flags => [size, value],
                   key => Key})).


set_invalid_mode_test(Config) ->
    ?assertMatch(
       #{command := client_error},
       send_sync(Config,
                 #{meta => set,
                   %% flags: T120 ME
                   flags => [{set_ttl, 120},
                             {mode, "Z"}],
                   data => alpha(5),
                   key => alpha(5)})).


%% lease_test(Config) ->
%%     ?assertMatch(
%%        #{command := client_error},
%%        send_sync(Config,
%%                  #{meta => get,
%%                    %% flags: s c v N30 t
%%                    flags => [size,
%%                              cas,
%%                              value,
%%                              {vivify, 30},
%%                              ttl],
%%                    data => alpha(5),
%%                    key => alpha(5)})).


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
