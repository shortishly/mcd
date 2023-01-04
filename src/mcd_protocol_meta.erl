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


-module(mcd_protocol_meta).


-export([decode/2]).
-export([encode/1]).
-include("mcd.hrl").


decode(debug_reply = Command, Remainder) ->
    [CommandLine, Encoded] = string:split(Remainder, ?RN),
    {mcd_re:run(
       #{meta => Command,
         subject => CommandLine,
         re => "(?<key>[^\\s]+) (?<internal>(\\w+=\\w+ ?)+)",
         mapping => #{internal => fun decode_meta_debug_internal/1}}),
     Encoded};

decode(Command, Remainder) when Command == head;
                                Command == not_stored;
                                Command == exists;
                                Command == not_found ->
    [Flags, Encoded] = string:split(Remainder, ?RN),
    {#{meta => Command, flags => mcd_meta_flags:decode(Flags)}, Encoded};

decode(Command, Remainder) when Command == debug;
                                Command == get;
                                Command == arithmetic;
                                Command == delete ->
    [CommandLine, Encoded] = split(Remainder),
    {re_run(
       #{meta => Command,
         subject => CommandLine,
         re => [key, flags],
         mapping => #{flags => fun mcd_meta_flags:decode/1}}),
     Encoded};

decode(set = Command, Remainder) ->
    [CommandLine, DataLine] = split(Remainder),
    data_line(
      re_run(
        #{meta => Command,
          subject => CommandLine,
          re => [key, datalen, flags],
          mapping => #{datalen => fun erlang:binary_to_integer/1,
                       flags => fun mcd_meta_flags:decode/1}}),
      DataLine);

decode(Command, Remainder) when Command == no_op; Command == no_op_reply ->
    [<<>>, Encoded] = split(Remainder),
    {#{meta => Command}, Encoded};

decode(value = Command, Remainder) ->
    [CommandLine, DataLine] = split(Remainder),
    data_line(
      re_run(
        #{meta => Command,
          subject => CommandLine,
          re => [datalen, flags],
          mapping => #{datalen => fun erlang:binary_to_integer/1,
                       flags => fun mcd_meta_flags:decode/1}}),
      DataLine).


decode_meta_debug_internal(Encoded) ->
    {match, Matches} = re:run(Encoded,
                              "(?<k>\\w+)=(?<v>\\w+)\s?",
                              [global,
                               {capture, all_names, binary}]),
    [list_to_tuple(Match) || Match <- Matches].


encode(#{meta := set = Meta,
         flags := Flags,
         key := Key,
         data := Data}) ->
    [command(Meta),
     " ",
     Key,
     " ",
     integer_to_binary(iolist_size(Data)),
     encode_flags(Flags),
     ?RN,
     Data,
     ?RN];

encode(#{meta := value, data := Data} = Arg) when is_integer(Data) ->
    ?FUNCTION_NAME(Arg#{data := integer_to_binary(Data)});

encode(#{meta := value = Meta,
         flags := Flags,
         data := Data}) ->
    [command(Meta),
     " ",
     integer_to_binary(iolist_size(Data)),
     encode_flags(Flags),
     ?RN,
     Data,
     ?RN];

encode(#{meta := Meta, flags := Flags, key := Key})
  when Meta == debug;
       Meta == get;
       Meta == arithmetic;
       Meta == delete ->
    [command(Meta),
     " ",
     Key,
     encode_flags(Flags),
     ?RN];

encode(#{meta := Meta, key := Key})
  when Meta == debug; Meta == get; Meta == debug ->
    [command(Meta), " ", Key, ?RN];

encode(#{meta := debug_reply = Meta, key := Key, internal := KV}) ->
    [command(Meta), " ", Key, [[" ", K, "=", V] || {K, V} <- KV], ?RN];

encode(#{meta := Meta, flags := Flags}) ->
    [command(Meta), encode_flags(Flags), ?RN];

encode(#{meta := Meta}) ->
    [command(Meta), ?RN].


encode_flags([]) ->
    [];
encode_flags(Flags) ->
    [" ", mcd_meta_flags:encode(Flags)].


command(no_op) ->
    "mn";
command(no_op_reply) ->
    "MN";
command(miss) ->
    "EN";
command(value) ->
    "VA";
command(not_stored) ->
    "NS";
command(not_found) ->
    "NF";
command(head) ->
    "HD";
command(exists) ->
    "EX";
command(debug_reply) ->
    "ME";
command(debug) ->
    "me";
command(get) ->
    "mg";
command(arithmetic) ->
    "ma";
command(set) ->
    "ms";
command(delete) ->
    "md".


re_run(#{re := L} = Arg) when is_list(L) ->
    mcd_re:run(
      Arg#{re := lists:join(
                   "\\s*",
                   lists:map(
                     fun re/1,
                     L))}).

re(key = Arg) ->
    ?FUNCTION_NAME(Arg, "[^\\s]+");

re(flags = Arg) ->
    ?FUNCTION_NAME(Arg, "(\\w\\w*(\\s+\\w\\w*)*)?");

re(datalen = Arg) ->
    ?FUNCTION_NAME(Arg, "\\d+").

re(Name, Expression) ->
    io_lib:format("(?<~p>~s)", [Name, Expression]).


split(S) ->
  string:split(S, ?RN).


data_line(#{datalen := Length} = Decoded, DataLine) ->
    case DataLine of
        <<Data:Length/bytes, ?RN, Encoded/bytes>> ->
            {maps:without([datalen], Decoded#{data => Data}), Encoded};
        _ ->
            partial
    end.
