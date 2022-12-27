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


-module(mcd_re).


-export([run/1]).


run(#{subject := Subject, re := RE} = Arg) ->
    {ok, MP} = re:compile(RE),
    {namelist, NL} = re:inspect(MP, namelist),
    case re:run(
           Subject,
           MP,
           options(Arg)) of

        {match, Matches} ->
            maps:filtermap(
              mapper(Arg),
              initial(NL, Matches, Arg));

        nomatch ->
            error(client_error)
    end.


initial(NL, Matches, Arg) ->
    maps:merge(
      maps:get(acc0, Arg, maps:with([command, meta], Arg)),
      maps:from_list(
        lists:zip([binary_to_existing_atom(N) || N <- NL], Matches))).


mapper(#{mapping := Mapping}) ->
    fun
        (K, V) ->
            case maps:find(K, Mapping) of
                {ok, Mapper} ->
                    case erlang:fun_info(Mapper, arity) of
                        {arity, 1} ->
                            {true, Mapper(V)};

                        {arity, 2} ->
                            Mapper(K, V)
                    end;

                error ->
                    {true, V}
            end
    end;

mapper(_) ->
    fun
        (_, V) ->
            {true, V}
    end.

options(#{options := Options}) ->
    Options;

options(#{}) ->
    [{capture, all_names, binary}].
