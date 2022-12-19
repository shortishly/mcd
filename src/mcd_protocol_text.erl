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


-module(mcd_protocol_text).


-export([decode/2]).
-export([encode/1]).
-export([expected_reply_count/1]).
-import(mcd_util, [split/1]).
-include("mcd.hrl").


decode(Command, Remainder)
  when Command == set;
       Command == cas;
       Command == add;
       Command == replace;
       Command == append;
       Command == prepend ->
    decode_storage_command(Command, Remainder);

decode(Command, Remainder)
  when Command == get;
       Command == gets;
       Command == gat;
       Command == gats ->
    decode_retrieval_command(Command, Remainder);

decode(delete, R0) ->
    [Parameters, Encoded] = split(R0),

    {ok, MP} = re:compile("(?<key>[^\\s]+)( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => delete},
               lists:zip(NL, Matches)),
             Encoded}
    end;

decode(value = Command, R0) ->
    [Key, R1] = string:split(R0, <<" ">>),
    [Flags, R2] = string:split(R1, <<" ">>),
    case string:split(R2, <<" ">>) of
        [R3] ->
            [DataLength, R4] = string:split(R3, ?RN),
            Length = binary_to_integer(DataLength),
            <<Data:Length/bytes, ?RN, Encoded/bytes>> = R4,
            {#{command => Command,
               key => Key,
               flags => binary_to_integer(Flags),
               data => Data},
             Encoded};

        [DataLength, R3] ->
            Length = binary_to_integer(DataLength),
            [CAS, <<Data:Length/bytes, ?RN, Encoded/bytes>>] = string:split(R3, ?RN),
            {#{command => Command,
               key => Key,
               flags => binary_to_integer(Flags),
               cas => binary_to_integer(CAS),
               data => Data},
            Encoded}
    end;

decode(Command, Remainder)
  when Command == client_error;
       Command == server_error ->
    [Reason, Encoded] = split(Remainder),
    {#{command => Command, reason => Reason}, Encoded};

decode(touch = Command, Remainder) ->
    [Parameters, Encoded] = split(Remainder),

    {ok, MP} = re:compile(
                 "(?<key>[^\\s]+) (?<expiry>[0-9]+)"
                 "( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({<<"expiry">> = Name, Value}, A) ->
                       A#{binary_to_atom(Name) => binary_to_integer(Value)};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => Command},
               lists:zip(NL, Matches)),
             Encoded}
    end;

decode(stats = Command, Remainder) ->
    [CommandLine, Encoded] = split(Remainder),
    {maps:filter(
       fun
           (arg, <<>>) ->
               false;

           (_, _) ->
               true
       end,
       re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(?<arg>\\w+)?"})),
     Encoded};

decode(stat = Command, Remainder) ->
    [CommandLine, Encoded] = split(Remainder),
    {re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(?<key>[^\\s]+)\\s+(?<value>.+)"}),
     Encoded};

decode(flush_all = Command, Remainder) ->
    [CommandLine, Encoded] = split(Remainder),
    {re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(\\s+(?<expiry>\\d+))?(\\s+(?<noreply>noreply))?",
         mapping => #{expiry => optional_binary_to_integer(0),
                      noreply => fun optional/1}}),
     Encoded};

decode(quit = Command, Remainder) ->
    [<<>>, Encoded] = split(Remainder),
    {#{command => Command}, Encoded};

decode(verbosity = Command, Remainder) ->
    [CommandLine, Encoded] = split(Remainder),
    {re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(?<level>\\d+)( (?<noreply>noreply))?",
         mapping => #{level => fun erlang:binary_to_integer/1,
                      noreply => fun optional/1}}),
     Encoded};

decode(Command, Remainder)
  when Command == incr;
       Command == decr ->
    [Parameters, Encoded] = split(Remainder),

    {ok, MP} = re:compile(
                 "(?<key>[^\\s]+) (?<value>[0-9]+)"
                 "( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({<<"value">> = Name, Value}, A) ->
                       A#{binary_to_atom(Name) => binary_to_integer(Value)};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => Command},
               lists:zip(NL, Matches)),
             Encoded}
    end.


decode_storage_command(Command, Remainder)
  when Command == set;
       Command == add;
       Command == replace;
       Command == append;
       Command == prepend ->
    [CommandLine, DataBlock] = split(Remainder),
    #{bytes := Size} =
        Decoded =
        re_run(
          #{command => Command,
            subject => CommandLine,
            re => "(?<key>[^\\s]+) (?<flags>\\d+) (?<expiry>\\d+) (?<bytes>\\d+)( (?<noreply>noreply))?\\s*",
            mapping => #{flags => fun erlang:binary_to_integer/1,
                         expiry => fun erlang:binary_to_integer/1,
                         noreply => fun optional/1,
                         bytes => fun erlang:binary_to_integer/1}}),
    case DataBlock of
        <<Data:Size/bytes, ?RN, Encoded/bytes>> ->
            {maps:without([bytes], Decoded#{data => Data}), Encoded};

        _Otherwise ->
            partial
    end;

decode_storage_command(cas = Command, Remainder) ->
    [CommandLine, DataBlock] = split(Remainder),
    #{bytes := Size} = Decoded = re_run(
                                   #{command => Command,
                                     subject => CommandLine,
                                     re => "(?<key>[^\\s]+) (?<flags>\\d+) (?<expiry>\\d+) (?<bytes>\\d+) (?<unique>\\d+)( (?<noreply>noreply))?",
                                     mapping => #{flags => fun erlang:binary_to_integer/1,
                                                  expiry => fun erlang:binary_to_integer/1,
                                                  unique => fun erlang:binary_to_integer/1,
                                                  noreply => fun optional/1,
                                                  bytes => fun erlang:binary_to_integer/1}}),
    <<Data:Size/bytes, ?RN, Encoded/bytes>> = DataBlock,
    {maps:without([bytes], Decoded#{data => Data}), Encoded}.


optional(Optional) ->
    Optional /= <<>>.


optional_binary_to_integer(Default) when is_integer(Default) ->
    fun
        (<<>>) ->
            Default;

        (Otherwise) ->
            erlang:binary_to_integer(Otherwise)
    end.


decode_retrieval_command(Command, Remainder)
  when Command == get; Command == gets ->
    case split(Remainder) of
        [Keys, Encoded] ->
            {#{command => Command,
               keys => string:split(string:trim(Keys), <<" ">>, all)},
             Encoded};

        [_] ->
            partial
    end;

decode_retrieval_command(Command, Remainder)
  when Command == gat; Command == gats ->
    [CommandLine, Encoded] = split(Remainder),
    {re_run(#{command => Command,
              subject => CommandLine,
              re => "(?<expiry>[0-9]+)\\s(?<keys>([^\\s]+\\s*)+)",
              mapping => #{expiry => fun erlang:binary_to_integer/1,
                           keys => fun
                                       (Keys) ->
                                           string:split(Keys, " ", all)
                                  end}}),
     Encoded}.


re_run(#{subject := Subject, re := RE} = Arg) ->
    {ok, MP} = re:compile(RE),
    {namelist, NL} = re:inspect(MP, namelist),
    case re:run(
           Subject,
           MP,
           maps:get(
             options,
             Arg,
             [{newline, crlf},
              {capture, all_names, binary}])) of

        {match, Matches} ->
            lists:foldl(
              fun
                  ({K, V}, A) ->
                      Key = binary_to_existing_atom(K),
                      A#{Key => case maps:find(
                                       Key,
                                       maps:get(mapping, Arg, #{})) of
                                    {ok, Mapper} ->
                                        Mapper(V);

                                    error ->
                                        V
                                end}
              end,
              maps:get(acc0, Arg, maps:with([command], Arg)),
              lists:zip(NL, Matches));

        nomatch ->
            error(client_error)
    end.


encode(#{command := ok = Command}) ->
    [string:uppercase(atom_to_list(Command)), ?RN];

encode(#{command := Command}) when Command == stats;
                                   Command == quit ->
    [atom_to_list(Command), ?RN];

encode(#{command := stat = Command,
         key := Key,
         value := Value}) ->
    [string:uppercase(atom_to_list(Command)), " ", Key, " ", Value, ?RN];

encode(#{command := incrdecr, value := Value}) ->
    [integer_to_list(Value), ?RN];

encode(#{command := flush_all = Command,
         expiry := Expiry,
         noreply := Noreply}) ->
    [atom_to_list(Command),
     [[" ", integer_to_list(Expiry)] || Expiry > 0],
     [" noreply" || Noreply],
     ?RN];

encode(#{command := verbosity = Command,
         level := Level,
         noreply := Noreply}) ->
    [atom_to_list(Command),
     " ",
     integer_to_list(Level),
     [" noreply" || Noreply],
     ?RN];

encode(#{command := Command,
         keys := Keys}) when Command == get; Command == gets ->
    [atom_to_list(Command),
     " ",
     lists:join(" ", Keys),
     ?RN];

encode(#{command := Command,
         key := Key,
         flags := Flags,
         expiry := Expiry,
         noreply := Noreply,
         data := Data}) when Command == set;
                             Command == add;
                             Command == replace;
                             Command == append;
                             Command == prepend ->
    [io_lib:format("~p ~s ~p ~p ~p",
                   [Command, Key, Flags, Expiry, iolist_size(Data)]),
     [" noreply" || Noreply],
     ?RN,
     Data,
     ?RN];

encode(#{command := cas = Command,
         key := Key,
         flags := Flags,
         expiry := Expiry,
         unique := Unique,
         noreply := Noreply,
         data := Data}) ->
    [io_lib:format("~p ~s ~p ~p ~p ~p",
                   [Command, Key, Flags, Expiry, iolist_size(Data), Unique]),
     [" noreply" || Noreply],
     ?RN,
     Data,
     ?RN];

encode(#{command := Command,
         expiry := Expiry,
         keys := Keys}) when Command == gat; Command == gats ->
    [lists:join(
       " ",
       [atom_to_list(Command), integer_to_list(Expiry) | Keys]),
     ?RN];

encode(#{command := delete, key := Key, noreply := Noreply}) ->
    ["delete ", Key, [" noreply" || Noreply], ?RN];

encode(#{command := Command, key := Key, value := Value, noreply := Noreply})
  when Command == incr; Command == decr ->
    [io_lib:format("~p ~s ~p", [Command, Key, Value]),
     [" noreply" || Noreply],
     ?RN];

encode(#{command := touch = Command,
         key := Key,
         expiry := Expiry,
         noreply := Noreply}) ->
    [io_lib:format("~p ~s ~p", [Command, Key, Expiry]),
     [" noreply" || Noreply],
     ?RN];


encode(#{command := value, data := Data} = Arg) when is_integer(Data) ->
    ?FUNCTION_NAME(Arg#{data := integer_to_binary(Data)});

encode(#{command := value,
         key := Key,
         cas := CAS,
         flags := Flags,
         data := Data}) ->
    [io_lib:format(
       "VALUE ~s ~p ~p ~p",
       [Key,
        Flags,
        iolist_size(Data),
        CAS]),
     ?RN,
     Data,
     ?RN];


encode(#{command := value, data := Data} = Arg) when is_integer(Data) ->
    ?FUNCTION_NAME(Arg#{data := integer_to_binary(Data)});

encode(#{command := value,
         key := Key,
         flags := Flags,
         data := Data}) ->
    [io_lib:format(
       "VALUE ~s ~p ~p",
       [Key,
        Flags,
        iolist_size(Data)]),
     ?RN,
     Data,
     ?RN];

encode(#{command := 'end'}) ->
    ["END", ?RN];

encode(#{command := error}) ->
    ["ERROR", ?RN];

encode(#{command := touched}) ->
    ["TOUCHED", ?RN];

encode(#{command := client_error, reason := Reason}) ->
    ["CLIENT_ERROR ", Reason, ?RN];

encode(#{command := server_error, reason := Reason}) ->
    ["SERVER_ERROR ", Reason, ?RN];

encode(#{command := stored}) ->
    ["STORED", ?RN];

encode(#{command := not_stored}) ->
    ["NOT_STORED", ?RN];

encode(#{command := exists}) ->
    ["EXISTS", ?RN];

encode(#{command := not_found}) ->
    ["NOT_FOUND", ?RN];

encode(#{command := deleted}) ->
    ["DELETED", ?RN].


expected_reply_count(#{noreply := true}) ->
    0;

expected_reply_count(#{command := Command,
                   keys := Keys}) when Command == get;
                                       Command == gets ->
    1 + length(Keys);

expected_reply_count(#{command := _}) ->
    1.
