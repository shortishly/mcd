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


-module(mcd_emulator_meta).


-export([recv/1]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("mcd_emulator.hrl").

recv(#{message := #{meta := no_op}}) ->
    {continue, {encode, #{meta => no_op_reply}}};

recv(#{message := #{meta := debug,
                    key := Key},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(#{arg => Arg}),
    case ets:lookup(Table, Key) of
        [#entry{cas = Expected}] ->
            {continue,
             {encode,
              #{meta => debug_reply,
                key => Key,
                internal => [{"cas", integer_to_list(Expected)}]}}};

        [] ->
            {continue, {encode, #{meta => miss}}}
    end;

recv(#{message := #{meta := delete, key := Key},
       flags := #{invalidate := _},
       data := #{table := Table}} = Arg) ->
    case ets:select_replace(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate, cas = CAS} = Entry) when Key == Candidate ->
                     Entry#entry{cas = CAS + 1, stale = true}
             end)) of

        1 ->
            case ets:lookup(Table, Key) of
                [#entry{} = Entry] ->
                    {continue, encode(head, Entry, Arg)};
                [] ->
                    {continue, {encode, #{meta => not_found}}}
            end;

        0 ->
            {continue, {encode, #{meta => not_found}}}
    end;

recv(#{message := #{meta := delete, key := Key},
       flags := #{cas_expected := Expected},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(#{arg => Arg}),
    case ets:lookup(Table, Key) of
        [#entry{cas = Expected}] ->
            case ets:select_delete(
                   Table,
                   ets:fun2ms(
                     fun
                         (#entry{key = Candidate, cas = Actual}) ->
                             Candidate == Key andalso Expected == Actual
                     end)) of

                1 ->
                    {continue, {encode, #{meta => head}}};

                0 ->
                    {continue, {encode, #{meta => exists}}}
            end;

        [#entry{}] ->
                    {continue, {encode, #{meta => exists}}};

        [] ->
            {continue, {encode, #{meta => not_found}}}
    end;

recv(#{message := #{meta := arithmetic, key := Key} = Message,
       flags := #{cas_expected := ExistingCAS},
       data := #{table := Table}} = Arg) ->
    case ets:lookup(Table, Key) of
        [#entry{data = ExistingValue, cas = ExistingCAS} = Entry] ->
            try
                NewValue = update_op(ExistingValue, Arg),

                case ets:select_replace(
                       Table,
                       ets:fun2ms(
                         fun
                             (#entry{key = FoundKey,
                                     cas = FoundCAS,
                                     data = FoundData} = Found)
                               when FoundKey == Key,
                                    FoundCAS == ExistingCAS,
                                    FoundData == ExistingValue ->
                                 Found#entry{data = NewValue, cas = FoundCAS + 1}
                         end)) of

                    1 ->
                        ?FUNCTION_NAME(Arg#{message := Message#{meta := get}});

                    0 ->
                        {continue, encode(exists, Entry#entry{cas = 0}, Arg)}
                end
            catch
                error:badarg ->
                    {continue,
                     {encode,
                      #{command => client_error,
                        reason =>  "cannot increment or decrement "
                        "non-numeric value"}}}
            end;

        [#entry{} = Entry] ->
            {continue, encode(exists, Entry#entry{cas = 0}, Arg)};

        [] ->
            {continue, encode(not_found, #entry{}, Arg)}
    end;

recv(#{message := #{meta := arithmetic, key := Key} = Message,
       flags := Flags,
       data := #{table := Table}} = Arg) ->
    try ets:update_counter(Table, Key, [delta(Arg)]) of
        [_] when is_map_key(noreply, Flags) ->
            continue;

        [_] ->
            ?FUNCTION_NAME(Arg#{message := Message#{meta := get}})

    catch
        error:badarg ->
            case ets:lookup(Table, Key) of
                [#entry{data = ExistingText}] when is_binary(ExistingText) ->
                    try
                        ExistingTextAsInteger = binary_to_integer(ExistingText),

                        ets:select_replace(
                          Table,
                          ets:fun2ms(
                            fun
                                (#entry{key = FoundKey,
                                        data = FoundData} = Entry)
                                  when FoundKey =:= Key,
                                       FoundData == ExistingText ->
                                    Entry#entry{data = ExistingTextAsInteger}
                            end)),

                        ?FUNCTION_NAME(Arg)

                    catch
                        error:badarg ->
                            {continue,
                             {encode,
                              #{command => client_error,
                                reason =>  "cannot increment or decrement "
                                "non-numeric value"}}}
                    end;

                [] when is_map_key(vivify, Flags) ->
                    case ets:insert_new(Table, entry_from(Arg)) of
                        true when is_map_key(noreply, Flags) ->
                            continue;

                        true ->
                            ?FUNCTION_NAME(
                               Arg#{message := Message#{meta := get}});

                        false ->
                            ?FUNCTION_NAME(Arg)
                    end;

                [] when is_map_key(noreply, Flags) ->
                    continue;

                [] ->
                    {continue, {encode, #{meta => not_found}}}
            end
    end;


recv(#{message := #{meta := set,
                    data := _Data,
                    key := Key},
       flags := #{mode := add},
       data := #{table := Table}} = Arg) ->
        ?LOG_DEBUG(#{arg => Arg}),

    Entry = entry_from(Arg),

    case ets:insert_new(Table, entry_from(Arg)) of
        true ->
            {continue, encode(head, Entry, Arg)};

        false ->
            lru_bump_entry(Arg),
            case ets:lookup(Table, Key) of
                [#entry{} = Existing] ->
                    {continue, encode(not_stored, Existing, Arg)};

                [] ->
                    ?FUNCTION_NAME(Arg)
            end
    end;

recv(#{message := #{meta := set,
                    data := Data,
                    key := Key},
       flags := #{mode := Mode},
       data := #{table := Table}} = Arg) when Mode == append;
                                              Mode == prepend;
                                              Mode == replace ->

    case ets:select_replace(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate,
                         cas = CAS,
                         data = D0} = Existing)
                   when Candidate == Key, Mode == append ->
                     Existing#entry{cas = CAS + 1, data = [D0, Data]};

                 (#entry{key = Candidate,
                         cas = CAS,
                         data = D0} = Existing)
                   when Candidate == Key, Mode == prepend ->
                     Existing#entry{cas = CAS + 1, data = [Data, D0]};

                 (#entry{key = Candidate,
                         cas = CAS,
                         data = D0} = Existing)
                   when Candidate == Key, Mode == replace ->
                     Existing#entry{cas = CAS + 1, data = Data}
             end)) of
        1 ->
            case ets:lookup(Table, Key) of
                [#entry{} = Entry] ->
                    {continue, encode(Entry, Arg)};

                [] when Mode == replace ->
                    {continue, {encode, #{meta => not_stored}}};

                [] ->
                    {continue, {encode, #{meta => not_found}}}
            end;

        0 when Mode == replace ->
            {continue, {encode, #{meta => not_stored}}};

        0 ->
            {continue, {encode, #{meta => not_found}}}
    end;

recv(#{message := #{meta := set,
                    data := Data,
                    key := Key},
       flags := #{cas_expected := Expected,
                  mode := set},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(#{arg => Arg}),

    case ets:lookup(Table, Key) of
        [#entry{cas = Expected} = Entry] ->
            case ets:select_replace(
                   Table,
                   ets:fun2ms(
                     fun
                         (#entry{key = Candidate, cas = Actual} = Existing)
                           when Candidate == Key, Expected == Actual ->
                             Existing#entry{cas = Actual + 1, data = Data}
                     end)) of

                1 ->
                    {continue, encode(head, Entry#entry{cas = Expected + 1}, Arg)};

                0 ->
                    {continue, encode(exists, Entry#entry{cas = 0}, Arg)}
            end;

        [#entry{} = Entry] ->
            {continue, encode(exists, Entry#entry{cas = 0}, Arg)};

        [] ->
            {continue, encode(not_found, #entry{}, Arg)}
    end;

recv(#{message := #{meta := set},
       flags := #{mode := set},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(#{arg => Arg}),
    ets:insert(Table, entry_from(Arg)),
    {continue, {encode, #{meta => head}}};


recv(#{message := #{meta := get, key := Key},
       flags := Flags,
       data := #{table := Table}} = Arg)
  when is_map_key(value, Flags);
       is_map_key(ttl, Flags);
       is_map_key(cas, Flags) ->
    ?LOG_DEBUG(#{arg => Arg}),

    case update_entry(Arg) of
        true ->
            case ets:lookup(Table, Key) of
                [#entry{} = Entry] ->
                    _ = update_entry(Arg),
                    {continue, encode(Entry, Arg)};

                [] ->
                    {continue, {encode, #{meta => miss}}}
            end;

        false when is_map_key(vivify, Flags) ->
            Entry = entry_from(Arg),
            case ets:insert_new(Table, Entry) of
                true ->
                    {continue, encode(Entry, Arg)};

                false ->
                    ?FUNCTION_NAME(Arg)
            end;

        false ->
            {continue, {encode, #{meta => miss}}}
    end;

recv(#{message := #{meta := get},
       flags := _} = Arg) ->
    ?LOG_DEBUG(#{arg => Arg}),
    {continue,
     {encode,
      #{meta => case update_entry(Arg) of
                    true ->
                        head;

                    false ->
                        miss
                end}}};

recv(#{message := #{meta := Meta, key := Key, flags := Flags} = Message} = Arg)
  when not(is_map_key(flags, Arg)) ->
    ?LOG_DEBUG(#{arg => Arg}),

    case lists:member(base64, Flags) of
        true ->
            ?FUNCTION_NAME(
               Arg#{message := Message#{key := base64:decode(Key)},
                    flags => mcd_meta_flags:as_map(Meta, Flags)});

        false ->
            ?FUNCTION_NAME(Arg#{flags => mcd_meta_flags:as_map(Meta, Flags)})
    end.


encode(Entry, #{flags := #{value := true}} = Arg) ->
    ?FUNCTION_NAME(value, Entry, Arg);
encode(Entry, Arg) ->
    ?FUNCTION_NAME(head, Entry, Arg).

encode(Meta, #entry{data = Data} = Entry, #{flags := #{value := true}} = Arg) ->
    ?FUNCTION_NAME(Meta, Entry, Arg, #{data => Data});
encode(Meta, Entry, Arg) ->
    ?FUNCTION_NAME(Meta, Entry, Arg, #{}).

encode(Meta, Entry, Arg, Encoded) ->
    ?LOG_DEBUG(#{meta => Meta,
                 entry => Entry,
                 arg => Arg,
                 encoded => Encoded}),
    [{encode,
      maps:merge(
        Encoded,
        #{meta => Meta, flags => reply_flags(Entry, Arg)})} |
     expire(Meta, Entry, Arg)].


expire(head, _Entry, #{key := Key, flags := #{set_ttl := TTL}}) ->
    [{expire, #{key => Key, seconds => TTL}}];

expire(value, _Entry, #{key := Key, flags := #{set_ttl := TTL}}) ->
    [{expire, #{key => Key, seconds => TTL}}];

expire(_, _, _) ->
    [].


update_entry(#{message := #{key := Key, flags := Flags},
               data := #{table := Table}}) ->
    ets:update_element(
      Table,
      Key,
      lists:foldl(
        fun
            ({set_ttl, TTL}, A) ->
                [{#entry.expiry, TTL},
                 {#entry.touched, erlang:monotonic_time()} | A];

            (_, A) ->
                A
        end,
        [{#entry.accessed, erlang:monotonic_time()}],
        Flags)).

lru_bump_entry(#{message := #{key := Key}, data := #{table := Table}}) ->
    ets:update_element(
      Table,
      Key,
      [{#entry.accessed, erlang:monotonic_time()},
       {#entry.touched, erlang:monotonic_time()}]).


entry_from(#{message := #{meta := arithmetic} = Message,
             flags := #{initial := Initial}} = Arg)
  when not(is_map_key(data, Message)) ->
    ?FUNCTION_NAME(Arg#{message := Message#{data => Initial}});

entry_from(#{message := #{key := Key, flags := Flags} = Message}) ->
    lists:foldl(
      fun
          ({K, TTL}, A) when K == set_ttl; K == vivify ->
              A#entry{expiry = TTL};

          (_, A) ->
              A
      end,
      case maps:find(data, Message) of
          {ok, Data} ->
              #entry{key = Key, data = Data};
          error ->
              #entry{key = Key}
      end,
      Flags).


reply_flags(#entry{key = Key,
                   data = Data,
                   expiry = TTL,
                   flags = Flags,
                   accessed = Accessed,
                   touched = Touched,
                   cas = CAS} = Entry,
            #{message := #{flags := MetaFlags}}) ->
    ?LOG_DEBUG(#{entry => Entry, flags => Flags}),
    lists:foldr(
      fun
          (key, A) when Key /= undefined ->
              [{key,
                case lists:member(base64, MetaFlags) of
                    true ->
                        base64:encode(Key);

                    false ->
                        Key
                end} | A];

          (cas, A) when CAS /= undefined ->
              [{cas, CAS} | A];

          (size, A) when is_integer(Data) ->
              [{size, iolist_size(integer_to_binary(Data))} | A];

          (size, A) when Data /= undefined ->
              [{size, iolist_size(Data)} | A];

          (flags, A) when Flags /= undefined ->
              [{flags, Flags} | A];

          (accessed, A) ->
              [{accessed,
                mcd_time:monotonic_to_system(
                  Accessed,
                  native,
                  second)} | A];

          (ttl, A) when TTL /= undefined ->
              [{ttl,
                max(0,
                    TTL - erlang:convert_time_unit(
                            erlang:monotonic_time() - Touched,
                            native,
                            second))} | A];

          ({opaque, _} = Opaque, A) ->
              [Opaque | A];

          (_, A) ->
              A
      end,
      [],
      MetaFlags).


delta(#{message := #{meta := arithmetic},
        flags := #{delta := Delta, mode := increment}}) ->
    {#entry.data, Delta, mcd_util:max(uint64), 0};

delta(#{message := #{meta := arithmetic},
        flags := #{delta := Delta, mode := decrement}}) ->
    {#entry.data, -Delta, mcd_util:min(uint64), 0}.


update_op(Existing, Arg) when is_binary(Existing) ->
    ?FUNCTION_NAME(binary_to_integer(Existing), Arg);

update_op(Existing,
          #{message := #{meta := arithmetic},
            flags := #{delta := Delta, mode := increment}}) ->
    ?FUNCTION_NAME(Existing, Delta, mcd_util:max(uint64), 0);

update_op(Existing,
          #{message := #{meta := arithmetic},
            flags := #{delta := Delta, mode := decrement}}) ->
    ?FUNCTION_NAME(Existing, -Delta, mcd_util:min(uint64), 0).


update_op(Current,
          Delta,
          Threshold,
          SetValue) when Delta >= 0,
                         Current + Delta > Threshold ->
    SetValue;

update_op(Current,
          Delta,
          Threshold,
          SetValue) when Delta < 0,
                         Current + Delta < Threshold ->
    SetValue;

update_op(Current, Delta, _, _) ->
    Current + Delta.
