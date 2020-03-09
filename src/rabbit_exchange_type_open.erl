%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_open).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").


-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2, info/1, info/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([init_db/0]).

-rabbit_boot_step({?MODULE, [ {description, "exchange type x-open : registry"}
  ,{mfa,      {rabbit_registry, register, [exchange, <<"x-open">>, ?MODULE]}}
  ,{cleanup,  {rabbit_registry, unregister, [exchange, <<"x-open">>]}}
  ,{requires, rabbit_registry}
  ,{enables,  kernel_ready}
  ]}).
-rabbit_boot_step({rabbit_exchange_type_open_mnesia, [ {description, "exchange type x-open : mnesia"}
  ,{mfa,      {?MODULE, init_db, []}}
  ,{requires, database}
  ,{enables,  external_infrastructure}
  ]}).

description() ->
    [{description, <<"Rabbit extension : x-open exchange, route like you need">>}].

-define(RECORD, open_bindings).
-define(RECKEY, exchange_name).
-define(RECVALUE, bindings).
-record(?RECORD, {?RECKEY, ?RECVALUE}).
-define(TABLE, rabbit_open_bindings).

init_db() ->
    mnesia:create_table(?TABLE, [{record_name, ?RECORD},
                                 {attributes, record_info(fields, ?RECORD)},
                                 {type, ordered_set}]),
    mnesia:add_table_copy(?TABLE, node(), ram_copies),
    mnesia:wait_for_tables([?TABLE], 30000),
    ok.


-define(ONE_CHAR_AT_LEAST, _/utf8, _/binary).
-define(BIN, _/binary).

info(_X) -> [].
info(_X, _) -> [].


-define(DEFAULT_DESTS_RE, {nil,nil,nil,nil,nil,nil,nil,nil}).

serialise_events() -> false.



validate_binding(_X, #binding{args = Args, key = << >>, destination = Dest}) ->
    Args2 = transform_x_del_dest(Args, Dest),

    case get_match_bt(Args2) of
        Err = {error, _} -> Err;
        0 -> validate_list_type_usage(any, Args2);
        _ -> validate_list_type_usage(all, Args2)
    end;
validate_binding(_X, _) ->
    {error, {binding_invalid, "Forbidden declaration of binding's routing key", []}}.


route(#exchange{name = #resource{virtual_host = VHost} = Name},
      #delivery{message = #basic_message{content = Content, routing_keys = [RK | _]}}) ->
    put(xopen_vhost, VHost),
    erase(xopen_dtl),
    erase(xopen_allqs),
    erase(xopen_msg_ds),

    MsgProperties = Content#content.properties#'P_basic'{headers =
        case Content#content.properties#'P_basic'.headers of
            undefined -> [];
            Table     -> lists:keysort(1, Table)
        end
    },

    [#?RECORD{?RECVALUE = [{0, Config} | CurrentOrderedBindings]}] = ets:lookup(?TABLE, Name),

    case Config of
        [] -> get_routes({RK, MsgProperties}, CurrentOrderedBindings, 1, ordsets:new());
        _ ->
            MsgContentSize = iolist_size(Content#content.payload_fragments_rev),
            case pre_check(Config, MsgContentSize, MsgProperties#'P_basic'.headers) of
                ok -> get_routes({RK, MsgProperties}, CurrentOrderedBindings, 1, ordsets:new());
                _ -> []
            end
    end.


pre_check([], _, _) -> ok;
pre_check([{1, {0, MinPlSize}} | _], PlSize, _) when PlSize < MinPlSize ->
    rabbit_misc:protocol_error(precondition_failed, "Min size payload hit.", []);
pre_check([{1, {1, MinPlSize}} | _], PlSize, _) when PlSize < MinPlSize -> ko;
pre_check([{2, {0, MaxPlSize}} | _], PlSize, _) when PlSize > MaxPlSize ->
    rabbit_misc:protocol_error(precondition_failed, "Max size payload hit.", []);
pre_check([{2, {1, MaxPlSize}} | _], PlSize, _) when PlSize > MaxPlSize -> ko;
pre_check([{3, {MaxHdsArrDepth, MaxHdsSize}} | _], _, Headers) ->
    check_headers_size(Headers, MaxHdsArrDepth, MaxHdsSize);
pre_check([_ | Tail], PlSize, Headers) ->
    pre_check(Tail, PlSize, Headers).


check_headers_size(Hs, MaxArrDepth, MaxSize) ->
    check_headers_size(Hs, MaxArrDepth, 0, MaxSize, 0).

check_headers_size(_, MaxArrDepth, ArrDepth, _, _) when ArrDepth > MaxArrDepth ->
    rabbit_misc:protocol_error(precondition_failed, "Max headers depth hit.", []);
check_headers_size(_, _, _, MaxSize, Size) when Size > MaxSize ->
    rabbit_misc:protocol_error(precondition_failed, "Max headers size hit.", []);
check_headers_size([], _, _, _, _) -> ok;
% arbitrarily, size of an array is 10 : why not ?!
% array of first level (argument has a Key)
check_headers_size([{<< Key/binary >>, array, Arr} | Tail], MaxArrDepth, 0, MaxSize, Size) ->
    KeySize = byte_size(Key),
    NewTail = lists:append([Arr, Tail]),
    check_headers_size(NewTail, MaxArrDepth, 1, MaxSize, Size + KeySize + 10);
% count the key name size only
check_headers_size([{<< Key/binary >>, _, V} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) ->
    KeySize = byte_size(Key),
    check_headers_size([{nil, V} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size + KeySize);
check_headers_size([{array, Arr} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) ->
    NewTail = lists:append([Arr, Tail]),
    check_headers_size(NewTail, MaxArrDepth, ArrDepth + 1, MaxSize, Size + 10);
% arbitrarily, size of a number is 4
check_headers_size([{_, N} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) when is_number(N) ->
    check_headers_size(Tail, MaxArrDepth, ArrDepth, MaxSize, Size + 4);
% size of a binary can be computed
check_headers_size([{_, << Bin/binary >>} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) ->
    ValSize = byte_size(Bin),
    check_headers_size(Tail, MaxArrDepth, ArrDepth, MaxSize, Size + ValSize);
% size of a list can be computed
check_headers_size([{_, L} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) when is_list(L) ->
    ValSize = length(L),
    check_headers_size(Tail, MaxArrDepth, ArrDepth, MaxSize, Size + ValSize);
% should be only for bool : arbitrarily, size of a bool is 1
check_headers_size([{_, _} | Tail], MaxArrDepth, ArrDepth, MaxSize, Size) ->
    check_headers_size(Tail, MaxArrDepth, ArrDepth, MaxSize, Size + 1).



%% Get routes
%% -----------------------------------------------------------------------------
get_routes(_, [], _, ResDests) ->
    ordsets:to_list(ResDests);

get_routes(MsgData, [ {_, BT, Dest, {HKRules, RKRules, DTRules, ATRules, DERules}, _} | T ], _, ResDests) ->
    case is_match(BT, MsgData, ResDests, HKRules, RKRules, DTRules, ATRules, DERules) of
        true -> get_routes(MsgData, T, 1, ordsets:add_element(Dest, ResDests));
           _ -> get_routes(MsgData, T, 1, ResDests)
    end;

% Jump to the next binding satisfying the last goto operator
get_routes(MsgData, [ {Order, _, _, _, _, _} | T ], GotoOrder, ResDests) when GotoOrder > Order ->
    get_routes(MsgData, T, GotoOrder, ResDests);

get_routes(MsgData, [ {_, BT, Dest, {HKRules, RKRules, DTRules, ATRules, DERules}, {GOT, GOF, StopOperators}, _} | T ], _, ResDests) ->
    case {is_match(BT, MsgData, ResDests, HKRules, RKRules, DTRules, ATRules, DERules), StopOperators} of
        {true,{1,_}}  -> ordsets:add_element(Dest, ResDests);
        {false,{_,1}} -> ResDests;
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:add_element(Dest, ResDests));
        {false,_}     -> get_routes(MsgData, T, GOF, ResDests)
    end;

get_routes(MsgData, [ {_, BT, Dest, {HKRules, RKRules, DTRules, ATRules, DERules}, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, ?DEFAULT_DESTS_RE, <<0, 0>>}, _} | T ], _, ResDests) ->
    case {is_match(BT, MsgData, ResDests, HKRules, RKRules, DTRules, ATRules, DERules), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT);
        {false,{_,1}} -> ordsets:subtract(ordsets:union(DAF, ResDests), DDF);
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT));
        {false,_}     -> get_routes(MsgData, T, GOF, ordsets:subtract(ordsets:union(DAF, ResDests), DDF))
    end;

get_routes(MsgData={_, MsgProps}, [ {_, BT, Dest, {HKRules, RKRules, DTRules, ATRules, DERules}, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, ?DEFAULT_DESTS_RE, <<BindDest:8, 0>>}, _} | T ], _, ResDests) ->
    {{MAQT, MAQF, MAET, MAEF, MDQT, MDQF, MDET, MDEF}, _} = case get(xopen_msg_ds) of
        undefined -> save_msg_dops(MsgProps#'P_basic'.headers, get(xopen_vhost));
        V -> V
    end,
    MsgAQT = case (BindDest band 1) of
        0 -> ordsets:from_list([]);
        _ -> MAQT
    end,
    MsgAQF = case (BindDest band 2) of
        0 -> ordsets:from_list([]);
        _ -> MAQF
    end,
    MsgAET = case (BindDest band 4) of
        0 -> ordsets:from_list([]);
        _ -> MAET
    end,
    MsgAEF = case (BindDest band 8) of
        0 -> ordsets:from_list([]);
        _ -> MAEF
    end,
    MsgDQT = case (BindDest band 16) of
        0 -> ordsets:from_list([]);
        _ -> MDQT
    end,
    MsgDQF = case (BindDest band 32) of
        0 -> ordsets:from_list([]);
        _ -> MDQF
    end,
    MsgDET = case (BindDest band 64) of
        0 -> ordsets:from_list([]);
        _ -> MDET
    end,
    MsgDEF = case (BindDest band 128) of
        0 -> ordsets:from_list([]);
        _ -> MDEF
    end,
    case {is_match(BT, MsgData, ResDests, HKRules, RKRules, DTRules, ATRules, DERules), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union([DAT, MsgAQT, MsgAET, ordsets:add_element(Dest, ResDests)]), ordsets:union([DDT, MsgDQT, MsgDET]));
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF, MsgAQF, MsgAEF, ResDests]), ordsets:union([DDF, MsgDQF, MsgDEF]));
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:subtract(ordsets:union([DAT, MsgAQT, MsgAET, ordsets:add_element(Dest, ResDests)]), ordsets:union([DDT, MsgDQT, MsgDET])));
        {false,_}     -> get_routes(MsgData, T, GOF, ordsets:subtract(ordsets:union([DAF, MsgAQF, MsgAEF, ResDests]), ordsets:union([DDF, MsgDQF, MsgDEF])))
    end;

get_routes(MsgData={_, MsgProps}, [ {_, BT, Dest, {HKRules, RKRules, DTRules, ATRules, DERules}, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, {DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE}, <<BindDest:8, BindREDest:8>>}, _} | T ], _, ResDests) ->
    AllVHQueues = case get(xopen_allqs) of
        undefined -> save_queues();
        Res1 -> Res1
    end,
    {{MAQT, MAQF, MAET, MAEF, MDQT, MDQF, MDET, MDEF}, {MAQRT, MAQRF, MDQRT, MDQRF, MAQNRT, MAQNRF, MDQNRT, MDQNRF}} = case get(xopen_msg_ds) of
        undefined -> save_msg_dops(MsgProps#'P_basic'.headers, get(xopen_vhost));
        V -> V
    end,

    MsgAQT = case (BindDest band 1) of
        0 -> ordsets:from_list([]);
        _ -> MAQT
    end,
    MsgAQF = case (BindDest band 2) of
        0 -> ordsets:from_list([]);
        _ -> MAQF
    end,
    MsgAET = case (BindDest band 4) of
        0 -> ordsets:from_list([]);
        _ -> MAET
    end,
    MsgAEF = case (BindDest band 8) of
        0 -> ordsets:from_list([]);
        _ -> MAEF
    end,
    MsgDQT = case (BindDest band 16) of
        0 -> ordsets:from_list([]);
        _ -> MDQT
    end,
    MsgDQF = case (BindDest band 32) of
        0 -> ordsets:from_list([]);
        _ -> MDQF
    end,
    MsgDET = case (BindDest band 64) of
        0 -> ordsets:from_list([]);
        _ -> MDET
    end,
    MsgDEF = case (BindDest band 128) of
        0 -> ordsets:from_list([]);
        _ -> MDEF
    end,

    MsgAQRT = case (BindREDest band 1 /= 0 andalso MAQRT /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MAQRT, [ report_errors, {capture, none} ]) == match])
    end,
    MsgAQRF = case (BindREDest band 2 /= 0 andalso MAQRF /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MAQRF, [ report_errors, {capture, none} ]) == match])
    end,
    MsgDQRT = case (BindREDest band 16 /= 0 andalso MDQRT /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MDQRT, [ report_errors, {capture, none} ]) == match])
    end,
    MsgDQRF = case (BindREDest band 32 /= 0 andalso MDQRF /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MDQRF, [ report_errors, {capture, none} ]) == match])
    end,
    MsgAQNRT = case (BindREDest band 1 /= 0 andalso MAQNRT /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MAQNRT, [ report_errors, {capture, none} ]) /= match])
    end,
    MsgAQNRF = case (BindREDest band 2 /= 0 andalso MAQNRF /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MAQNRF, [ report_errors, {capture, none} ]) /= match])
    end,
    MsgDQNRT = case (BindREDest band 16 /= 0 andalso MDQNRT /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MDQNRT, [ report_errors, {capture, none} ]) /= match])
    end,
    MsgDQNRF = case (BindREDest band 32 /= 0 andalso MDQNRF /= nil) of
        false -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MDQNRF, [ report_errors, {capture, none} ]) /= match])
    end,

    DATREsult = case DATRE of
        nil -> ordsets:from_list([]);
        {DATRE1} -> ordsets:from_list(pickoneof([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATRE1, [ {capture, none} ]) == match]));
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATRE, [ {capture, none} ]) == match])
    end,
    DAFREsult = case DAFRE of
        nil -> ordsets:from_list([]);
        {DAFRE1} -> ordsets:from_list(pickoneof([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFRE1, [ {capture, none} ]) == match]));
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFRE, [ {capture, none} ]) == match])
    end,
    DDTREsult = case DDTRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDTRE, [ {capture, none} ]) == match])
    end,
    DDFREsult = case DDFRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDFRE, [ {capture, none} ]) == match])
    end,
    DATNREsult = case DATNRE of
        nil -> ordsets:from_list([]);
        {DATNRE1} -> ordsets:from_list(pickoneof([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATNRE1, [ {capture, none} ]) /= match]));
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATNRE, [ {capture, none} ]) /= match])
    end,
    DAFNREsult = case DAFNRE of
        nil -> ordsets:from_list([]);
        {DAFNRE1} -> ordsets:from_list(pickoneof([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFNRE1, [ {capture, none} ]) /= match]));
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFNRE, [ {capture, none} ]) /= match])
    end,
    DDTNREsult = case DDTNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDTNRE, [ {capture, none} ]) /= match])
    end,
    DDFNREsult = case DDFNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDFNRE, [ {capture, none} ]) /= match])
    end,

    case {is_match(BT, MsgData, ResDests, HKRules, RKRules, DTRules, ATRules, DERules), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,MsgAQT,MsgAET,MsgAQRT,MsgAQNRT,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult,MsgDQT,MsgDET,MsgDQRT,MsgDQNRT]));
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,MsgAQF,MsgAEF,MsgAQRF,MsgAQNRF,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult,MsgDQF,MsgDEF,MsgDQRF,MsgDQNRF]));
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,MsgAQT,MsgAET,MsgAQRT,MsgAQNRT,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult,MsgDQT,MsgDET,MsgDQRT,MsgDQNRT])));
        {false,_}     -> get_routes(MsgData, T, GOF, ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,MsgAQF,MsgAEF,MsgAQRF,MsgAQNRF,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult,MsgDQF,MsgDEF,MsgDQRF,MsgDQNRF])))
    end.



%% -----------------------------------------------------------------------------
%% Save it for later (.. Don't run away and let me down :)
%% Called once if needed per message; result (process dictionary..) to be used
%%   multiple times in bindings parsing.
%% -----------------------------------------------------------------------------

%% Save current datetime
%% -----------------------------------------------------------------------------
% This one is too heavy; maybe we could implement it via NIF ?
save_datetimes() ->
    TS = erlang:timestamp(),
% Local date time
    {YMD={Y,Mo,D},{H,Mi,S}} = calendar:now_to_local_time(TS),
    {_,W} = calendar:iso_week_number(YMD),
    Dw = calendar:day_of_the_week(YMD),
    DateAsString = lists:flatten(io_lib:format("~B~2..0B~2..0B ~2..0B~2..0B~2..0B ~B ~2..0B", [Y,Mo, D, H, Mi, S, Dw, W])),
    put(xopen_dtl, DateAsString),
% Universal date time taken from before
    {UYMD={UY,UMo,UD},{UH,UMi,US}} = calendar:now_to_universal_time(TS),
    {_,UW} = calendar:iso_week_number(UYMD),
    UDw = calendar:day_of_the_week(UYMD),
    UDateAsString = lists:flatten(io_lib:format("~B~2..0B~2..0B ~2..0B~2..0B~2..0B ~B ~2..0B", [UY,UMo, UD, UH, UMi, US, UDw, UW])),
    put(xopen_dtu, UDateAsString).


%% Save current existing queues
%% -----------------------------------------------------------------------------
% Maybe there is a cleaner way to do that ?
save_queues() ->
    AllQueues = mnesia:dirty_all_keys(rabbit_queue),
    AllVHQueues = [Q || Q = #resource{virtual_host = QueueVHost, kind = queue} <- AllQueues, QueueVHost == get(xopen_vhost)],
    put(xopen_allqs, AllVHQueues),
    AllVHQueues.


%% Save operators from message
%% -----------------------------------------------------------------------------
save_msg_dops(Headers, VHost) ->
    FHeaders = flatten_table_msg(Headers, []),
    {Dests, DestsRE} = pack_msg_ops(FHeaders, VHost),
    put(xopen_msg_ds, {Dests, DestsRE}),
    {Dests, DestsRE}.

% --------------------------------------
flatten_table_msg([], Result) -> Result;
% Flatten arrays and filter values on longstr type
flatten_table_msg ([ {Op, array, Vs} | Tail ], Result) when
        Op==<<"x-addq-ontrue">>; Op==<<"x-addq-onfalse">>;
            Op==<<"x-adde-ontrue">>; Op==<<"x-adde-onfalse">>;
            Op==<<"x-delq-ontrue">>; Op==<<"x-delq-onfalse">>;
            Op==<<"x-dele-ontrue">>; Op==<<"x-dele-onfalse">> ->
    Res = [ { Op, T, V } || {T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} <- Vs ],
    flatten_table_msg (Tail, lists:append ([ Res , Result ]));
% Filter ops and on type
flatten_table_msg ([ {Op, longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result) when
        Op==<<"x-addq-ontrue">>; Op==<<"x-addq-onfalse">>;
            Op==<<"x-adde-ontrue">>; Op==<<"x-adde-onfalse">>;
            Op==<<"x-delq-ontrue">>; Op==<<"x-delq-onfalse">>;
            Op==<<"x-dele-ontrue">>; Op==<<"x-dele-onfalse">> ->
    flatten_table_msg (Tail, [ {Op, longstr, V} | Result ]);
flatten_table_msg ([ {Op, longstr, Regex} | Tail ], Result) when
        Op==<<"x-addqre-ontrue">>; Op==<<"x-addqre-onfalse">>;
            Op==<<"x-addq!re-ontrue">>; Op==<<"x-addq!re-onfalse">>;
            Op==<<"x-delqre-ontrue">>; Op==<<"x-delqre-onfalse">>;
            Op==<<"x-delq!re-ontrue">>; Op==<<"x-delq!re-onfalse">> ->
    case is_regex_valid(Regex) of
        true -> flatten_table_msg (Tail, [ {Op, longstr, Regex} | Result ]);
        _ -> rabbit_misc:protocol_error(precondition_failed, "Invalid regex '~ts'", [Regex])
    end;
% Go next
flatten_table_msg ([ _ | Tail ], Result) ->
        flatten_table_msg (Tail, Result).

% --------------------------------------
pack_msg_ops(Args, VHost) ->
    pack_msg_ops(Args, VHost, {[],[],[],[],[],[],[],[]}, ?DEFAULT_DESTS_RE).

pack_msg_ops([], _, Dests, DestsRE) ->
    {Dests, DestsRE};
pack_msg_ops([ {K, _, V} | Tail ], VHost, Dests, DestsRE) ->
    {AQT, AQF, AET, AEF, DQT, DQF, DET, DEF} = Dests,
    NewDests = case K of
        <<"x-addq-ontrue">> -> {[rabbit_misc:r(VHost, queue, V) | AQT], AQF, AET, AEF, DQT, DQF, DET, DEF};
        <<"x-addq-onfalse">> -> {AQT, [rabbit_misc:r(VHost, queue, V) | AQF], AET, AEF, DQT, DQF, DET, DEF};
        <<"x-adde-ontrue">> -> {AQT, AQF, [rabbit_misc:r(VHost, exchange, V) | AET], AEF, DQT, DQF, DET, DEF};
        <<"x-adde-onfalse">> -> {AQT, AQF, AET, [rabbit_misc:r(VHost, exchange, V) | AEF], DQT, DQF, DET, DEF};
        <<"x-delq-ontrue">> -> {AQT, AQF, AET, AEF, [rabbit_misc:r(VHost, queue, V) | DQT], DQF, DET, DEF};
        <<"x-delq-onfalse">> -> {AQT, AQF, AET, AEF, DQT, [rabbit_misc:r(VHost, queue, V) | DQF], DET, DEF};
        <<"x-dele-ontrue">> -> {AQT, AQF, AET, AEF, DQT, DQF, [rabbit_misc:r(VHost, exchange, V) | DET], DEF};
        <<"x-dele-onfalse">> -> {AQT, AQF, AET, AEF, DQT, DQF, DET, [rabbit_misc:r(VHost, exchange, V) | DEF]};
        _ -> Dests
    end,
    {AQRT, AQRF, DQRT, DQRF, AQNRT, AQNRF, DQNRT, DQNRF} = DestsRE,
    NewDestsRE = case K of
        <<"x-addqre-ontrue">> -> {V, AQRF, DQRT, DQRF, AQNRT, AQNRF, DQNRT, DQNRF};
        <<"x-addqre-onfalse">> -> {AQRT, V, DQRT, DQRF, AQNRT, AQNRF, DQNRT, DQNRF};
        <<"x-delqre-ontrue">> -> {AQRT, AQRF, V, DQRF, AQNRT, AQNRF, DQNRT, DQNRF};
        <<"x-delqre-onfalse">> -> {AQRT, AQRF, DQRT, V, AQNRT, AQNRF, DQNRT, DQNRF};
        <<"x-addq!re-ontrue">> -> {AQRT, AQRF, DQRT, DQRF, V, AQNRF, DQNRT, DQNRF};
        <<"x-addq!re-onfalse">> -> {AQRT, AQRF, DQRT, DQRF, AQNRT, V, DQNRT, DQNRF};
        <<"x-delq!re-ontrue">> -> {AQRT, AQRF, DQRT, DQRF, AQNRT, AQNRF, V, DQNRF};
        <<"x-delq!re-onfalse">> -> {AQRT, AQRF, DQRT, DQRF, AQNRT, AQNRF, DQNRT, V};
        _ -> DestsRE
    end,
    pack_msg_ops(Tail, VHost, NewDests, NewDestsRE).



%% -----------------------------------------------------------------------------
%% Matching
%% -----------------------------------------------------------------------------

% !!!! L'ordre dans la liste des operations est important
% !!!! SINON, utiliser une maps !!

is_match2(0, _, []) -> false;
is_match2(_, _, []) -> true;

% Binding type any and hkOps
is_match2(0, MsgData = {_, MsgProps}, [{hkOps, MatchHKOps} | OtherOps]) ->
    is_match_hk_any(MatchHKOps, MsgProps#'P_basic'.headers) orelse is_match2(0, MsgData, OtherOps);
% Binding type set and hkOps
is_match2({3, Nmin}, MsgData = {_, MsgProps}, [{hkOps, MatchHKOps} | OtherOps]) ->
    (is_match_hk_set(MatchHKOps, MsgProps#'P_basic'.headers) >= Nmin) andalso is_match2(42, MsgData, OtherOps);
% Binding type all or eq and hkOps
is_match2(_, MsgData = {_, MsgProps}, [{hkOps, MatchHKOps} | OtherOps]) ->
    is_match_hk_alloreq(MatchHKOps, MsgProps#'P_basic'.headers) andalso is_match2(42, MsgData, OtherOps);

% Binding type any and prOps
is_match2(0, MsgData = {_, MsgProps}, [{prOps, MatchPROps} | OtherOps]) ->
    is_match_pr_any(MatchRKOps, MsgProps) orelse is_match2(0, MsgData, OtherOps);

band



% Optimization for headers only..
is_match(0, {_, MsgProps}, _, HKRules, [], [], [], []) ->
    is_match_hk_any(HKRules, MsgProps#'P_basic'.headers);
is_match({3, Nmin}, {_, MsgProps}, _, HKRules, [], [], [], []) ->
    is_match_hk_set(HKRules, MsgProps#'P_basic'.headers, 0) >= Nmin;
is_match(BT, {_, MsgProps}, _, HKRules, [], [], [], []) ->
    is_match_hk_alloreq(BT, HKRules, MsgProps#'P_basic'.headers);

is_match(BT, {MsgRK, MsgProps}, _, HKRules, RKRules, [], [], []) ->
    case BT of
        0 -> is_match_rk_any(RKRules, MsgRK)
            orelse is_match_hk_any(HKRules, MsgProps#'P_basic'.headers);
        _ -> is_match_rk_all(RKRules, MsgRK)
            andalso is_match_hk(BT, HKRules, MsgProps#'P_basic'.headers)
    end;
is_match(BT, {MsgRK, MsgProps}, _, HKRules, RKRules, [], ATRules, []) ->
    case BT of
        0 -> is_match_rk_any(RKRules, MsgRK)
            orelse is_match_hk_any(HKRules, MsgProps#'P_basic'.headers)
            orelse is_match_pr_any(ATRules, MsgProps);
        _ -> is_match_rk_all(RKRules, MsgRK)
            andalso is_match_hk(BT, HKRules, MsgProps#'P_basic'.headers)
            andalso is_match_pr_all(ATRules, MsgProps)
    end;
is_match(BT, {MsgRK, MsgProps}, ResDests, HKRules, RKRules, DTRules, ATRules, DERules) ->
    case get(xopen_dtl) of
        undefined -> save_datetimes();
        _ -> ok
    end,
    case BT of
        0 -> is_match_rk_any(RKRules, MsgRK)
            orelse is_match_hk_any(HKRules, MsgProps#'P_basic'.headers)
            orelse is_match_de_any(DERules, ResDests)
            orelse is_match_pr_any(ATRules, MsgProps)
            orelse is_match_dt_any(DTRules);
        _ -> is_match_rk_all(RKRules, MsgRK)
            andalso is_match_hk(BT, HKRules, MsgProps#'P_basic'.headers)
            andalso is_match_de_all(DERules, ResDests)
            andalso is_match_pr_all(ATRules, MsgProps)
            andalso is_match_dt_all(DTRules)
    end.


%% Match on datetime
%% -----------------------------------------------------------------------------

% all
% --------------------------------------
is_match_dt_all([]) -> true;
is_match_dt_all([ {dture, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> is_match_dt_all(Tail);
        _ -> false
    end;
is_match_dt_all([ {dtunre, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_dt_all(Tail)
    end;
is_match_dt_all([ {dtlre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> is_match_dt_all(Tail);
        _ -> false
    end;
is_match_dt_all([ {dtlnre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_dt_all(Tail)
    end.

% any
% --------------------------------------
is_match_dt_any([]) -> false;
is_match_dt_any([ {dture, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtunre, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtlre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtlnre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end.


%% Match on properties
%% -----------------------------------------------------------------------------

% all
% --------------------------------------
is_match_pr_all([], _) ->
    true;
is_match_pr_all([ {PropOp, PropId, V} | Tail], MsgProps) ->
    MsgPropV = get_msg_prop_value(PropId, MsgProps),
    if
        PropOp == nx andalso MsgPropV == undefined ->
            is_match_pr_all(Tail, MsgProps);
        MsgPropV /= undefined ->
            case PropOp of
                ex -> is_match_pr_all(Tail, MsgProps);
                nx -> false;
                eq when MsgPropV == V ->
                    is_match_pr_all(Tail, MsgProps);
                eq -> false;
                ne when MsgPropV /= V ->
                    is_match_pr_all(Tail, MsgProps);
                ne -> false;
                re -> case re:run(MsgPropV, V, [ {capture, none} ]) == match of
                          true -> is_match_pr_all(Tail, MsgProps);
                          _ -> false
                      end;
                nre -> case re:run(MsgPropV, V, [ {capture, none} ]) == nomatch of
                           true -> is_match_pr_all(Tail, MsgProps);
                           _ -> false
                       end;
                lt when MsgPropV < V ->
                    is_match_pr_all(Tail, MsgProps);
                lt -> false;
                le when MsgPropV =< V ->
                    is_match_pr_all(Tail, MsgProps);
                le -> false;
                ge when MsgPropV >= V ->
                    is_match_pr_all(Tail, MsgProps);
                ge -> false;
                gt when MsgPropV > V ->
                    is_match_pr_all(Tail, MsgProps);
                gt -> false
            end;
        true ->
            false
    end.

% any
% --------------------------------------
is_match_pr_any([], _) -> false;
is_match_pr_any([ {PropOp, PropId, V} | Tail], MsgProps) ->
    MsgPropV = get_msg_prop_value(PropId, MsgProps),
    if
        PropOp == nx andalso MsgPropV == undefined ->
            true;
        MsgPropV /= undefined ->
            case PropOp of
                ex -> true;
                nx -> is_match_pr_any(Tail, MsgProps);
                eq when MsgPropV == V -> true;
                eq -> is_match_pr_any(Tail, MsgProps);
                ne when MsgPropV /= V -> true;
                ne -> is_match_pr_any(Tail, MsgProps);
                re -> case re:run(MsgPropV, V, [ {capture, none} ]) == match of
                          true -> true;
                          _ -> is_match_pr_any(Tail, MsgProps)
                      end;
                nre -> case re:run(MsgPropV, V, [ {capture, none} ]) == nomatch of
                           true -> true;
                           _ -> is_match_pr_any(Tail, MsgProps)
                       end;
                lt when MsgPropV < V -> true;
                lt -> is_match_pr_any(Tail, MsgProps);
                le when MsgPropV =< V -> true;
                le -> is_match_pr_any(Tail, MsgProps);
                ge when MsgPropV >= V -> true;
                ge -> is_match_pr_any(Tail, MsgProps);
                gt when MsgPropV > V -> true;
                gt -> is_match_pr_any(Tail, MsgProps)
            end;
        true ->
            is_match_pr_any(Tail, MsgProps)
    end.

% --------------------------------------
get_msg_prop_value(PropId, MsgProps) ->
    case PropId of
        ex -> try binary_to_integer(MsgProps#'P_basic'.expiration)
            catch _:_ -> MsgProps#'P_basic'.expiration
            end;
        ct -> MsgProps#'P_basic'.content_type;
        ce -> MsgProps#'P_basic'.content_encoding;
        co -> MsgProps#'P_basic'.correlation_id;
        re -> MsgProps#'P_basic'.reply_to;
        me -> MsgProps#'P_basic'.message_id;
        ty -> MsgProps#'P_basic'.type;
        us -> MsgProps#'P_basic'.user_id;
        ap -> MsgProps#'P_basic'.app_id;
        cl -> MsgProps#'P_basic'.cluster_id;
        de -> MsgProps#'P_basic'.delivery_mode;
        pr -> MsgProps#'P_basic'.priority;
        ti -> MsgProps#'P_basic'.timestamp
    end.


%% Match on destinations
%% -----------------------------------------------------------------------------

% all
% --------------------------------------
is_match_de_all([], _) -> true;

% If no dest match operator excludes others (validations must follow), then it could be true here
is_match_de_all([ no | T], []) ->
    is_match_de_all(T, []);
is_match_de_all([ no | _], _) -> false;

is_match_de_all([ {q, _} | _], []) -> false;
is_match_de_all([ {q, V} | T], Dests) ->
    QueueNames = [QueueName || #resource{name = QueueName, kind=queue} <- ResDests],
    (V in QueueNames) andalso is_match_de_all(T, Dests);
    Blah. 
is_match_de_all([ {qn, V} | T], Dests) ->
    QueueNames = [QueueName || #resource{name = QueueName, kind=queue} <- ResDests],
    (V NOT in QueueNames) andalso is_match_de_all(T, Dests);
    Blah. 



%% Match on routing key
%% -----------------------------------------------------------------------------

% all
% --------------------------------------
is_match_rk_all([], _) -> true;
is_match_rk_all([ {rkeq, V} | Tail], RK) when V == RK ->
    is_match_rk_all(Tail, RK);
is_match_rk_all([ {rkne, V} | Tail], RK) when V /= RK ->
    is_match_rk_all(Tail, RK);
is_match_rk_all([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_all(Tail, RK);
        _ -> false
    end;
is_match_rk_all([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_rk_all(Tail, RK)
    end;
is_match_rk_all(_, _) ->
    false.

% any
% --------------------------------------
is_match_rk_any([], _) -> false;
is_match_rk_any([ {rkeq, V} | _], RK) when V == RK -> true;
is_match_rk_any([ {rkne, V} | _], RK) when V /= RK -> true;
is_match_rk_any([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_rk_any(Tail, RK)
    end;
is_match_rk_any([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_any(Tail, RK);
        _ -> true
    end;
is_match_rk_any([ _ | Tail], RK) ->
    is_match_rk_any(Tail, RK).


%% Match on Headers Keys
%% -----------------------------------------------------------------------------
% set
is_match_hk({3, Nmin}, Args, Headers) ->
    is_match_hk_set(Args, Headers, 0) >= Nmin;
is_match_hk(BT, Args, Headers) ->
    is_match_hk_alloreq(BT, Args, Headers).

% all and eq
% --------------------------------------
% No more match operator to check with all; return true
is_match_hk_alloreq(1, [], _) -> true;
% With eq, return false if there are still some header keys
is_match_hk_alloreq(4, [], [_ | _]) -> false;
is_match_hk_alloreq(4, [], []) -> true;


% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
% MAYBE WE CAN TREAT NX AND HK? ops the same way like that : {0, nx} ?
% Purge nx op on no data as all these are true
is_match_hk_alloreq(BT, [{_, nx, _} | BNext], []) ->
    is_match_hk_alloreq(BT, BNext, []);
% This rule is true for key not required too
is_match_hk_alloreq(BT, [{_, {0, _}, _} | BNext], []) ->
    is_match_hk_alloreq(BT, BNext, []);

% No more message header but still match operator to check other than nx or hk?
is_match_hk_alloreq(_, _, []) -> false;


% Current header key not in match operators; go next header with current match operator
%  With eq return false
is_match_hk_alloreq(4, [{BK, _, _} | _], [{HK, _, _} | _]) when BK > HK -> false;
is_match_hk_alloreq(BT, BCur = [{BK, _, _} | _], [{HK, _, _} | HNext]) when BK > HK ->
    is_match_hk_alloreq(BT, BCur, HNext);

% Current binding key must not exist in data, go next binding
is_match_hk_alloreq(BT, [{BK, nx, _} | BNext], HCur = [{HK, _, _} | _]) when BK < HK ->
    is_match_hk_alloreq(BT, BNext, HCur);
% This rule is true for key not required too
is_match_hk_alloreq(BT, [{BK, {0, _}, _} | BNext], HCur = [{HK, _, _} | _]) when BK < HK ->
    is_match_hk_alloreq(BT, BNext, HCur);
% Current match operator does not exist in message
is_match_hk_alloreq(_, [{BK, _, _} | _], [{HK, _, _} | _]) when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
is_match_hk_alloreq(BT, [{_, {_, eq}, BV} | BNext], [{_, _, HV} | HNext]) when BV == HV ->
    is_match_hk_alloreq(BT, BNext, HNext);
% Current values must match but do not match; return false
is_match_hk_alloreq(_, [{_, {_, eq}, _} | _], _) -> false;
% Key must not exist, return false
is_match_hk_alloreq(_, [{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
is_match_hk_alloreq(BT, [{_, ex, _} | BNext], [ _ | HNext]) ->
    is_match_hk_alloreq(BT, BNext, HNext);

%  .. with type checking
is_match_hk_alloreq(BT, [{_, is, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        longstr -> is_match_hk_alloreq(BT, BNext, HNext);
        _ -> false
    end;
is_match_hk_alloreq(BT, [{_, ib, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        bool -> is_match_hk_alloreq(BT, BNext, HNext);
        _ -> false
    end;
is_match_hk_alloreq(BT, [{_, ii, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_integer(HV) of
        true -> is_match_hk_alloreq(BT, BNext, HNext);
        _ -> false
    end;
is_match_hk_alloreq(BT, [{_, in, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_number(HV) of
        true -> is_match_hk_alloreq(BT, BNext, HNext);
        _ -> false
    end;
is_match_hk_alloreq(BT, [{_, ia, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type == array of
        true -> is_match_hk_alloreq(BT, BNext, HNext);
        _ -> false
    end;

is_match_hk_alloreq(BT, [{_, nis, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        longstr -> false;
        _ -> is_match_hk_alloreq(BT, BNext, HNext)
    end;
is_match_hk_alloreq(BT, [{_, nib, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        bool -> false;
        _ -> is_match_hk_alloreq(BT, BNext, HNext)
    end;
is_match_hk_alloreq(BT, [{_, nii, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_integer(HV) of
        true -> false;
        _ -> is_match_hk_alloreq(BT, BNext, HNext)
    end;
is_match_hk_alloreq(BT, [{_, nin, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_number(HV) of
        true -> false;
        _ -> is_match_hk_alloreq(BT, BNext, HNext)
    end;
is_match_hk_alloreq(BT, [{_, nia, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type == array of
        true -> false;
        _ -> is_match_hk_alloreq(BT, BNext, HNext)
    end;

% <= < != > >=
is_match_hk_alloreq(BT, [{_, {_, ne}, BV} | BNext], HCur = [{_, _, HV} | _]) when BV /= HV ->
    is_match_hk_alloreq(BT, BNext, HCur);
is_match_hk_alloreq(_, [{_, {_, ne}, _} | _], _) -> false;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
is_match_hk_alloreq(BT, [{_, {_, gt}, BV} | BNext], HCur = [{_, _, HV} | _]) when is_number(HV), HV > BV ->
    is_match_hk_alloreq(BT, BNext, HCur);
is_match_hk_alloreq(_, [{_, {_, gt}, _} | _], _) -> false;
is_match_hk_alloreq(BT, [{_, {_, ge}, BV} | BNext], HCur = [{_, _, HV} | _]) when is_number(HV), HV >= BV ->
    is_match_hk_alloreq(BT, BNext, HCur);
is_match_hk_alloreq(_, [{_, {_, ge}, _} | _], _) -> false;
is_match_hk_alloreq(BT, [{_, {_, lt}, BV} | BNext], HCur = [{_, _, HV} | _]) when is_number(HV), HV < BV ->
    is_match_hk_alloreq(BT, BNext, HCur);
is_match_hk_alloreq(_, [{_, {_, lt}, _} | _], _) -> false;
is_match_hk_alloreq(BT, [{_, {_, le}, BV} | BNext], HCur = [{_, _, HV} | _]) when is_number(HV), HV =< BV ->
    is_match_hk_alloreq(BT, BNext, HCur);
is_match_hk_alloreq(_, [{_, {_, le}, _} | _], _) -> false;

% Regexes
is_match_hk_alloreq(BT, [{_, {_, re}, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hk_alloreq(BT, BNext, HCur);
        _ -> false
    end;
is_match_hk_alloreq(_, [{_, {_, re}, _} | _], _) -> false;
is_match_hk_alloreq(BT, [{_, {_, nre}, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> is_match_hk_alloreq(BT, BNext, HCur);
        _ -> false
    end;
is_match_hk_alloreq(_, [{_, {_, nre}, _} | _], _) -> false.


% set
% --------------------------------------
% No more match operator to check; return result
is_match_hk_set([], _, Res) -> Res;

% Purge nx op on no data as all these are true
is_match_hk_set([{_, nx, _} | BNext], [], Res) ->
    is_match_hk_set(BNext, [], Res + 1);
% In set, key is never required; just go next
is_match_hk_set([{_, {_, _}, _} | BNext], [], Res) ->
    is_match_hk_set(BNext, [], Res);

% No more message header but still match operator to check other than nx and hk?
is_match_hk_set(_, [], Res) -> Res;

% Current header key not in match operators; go next header with current match operator
is_match_hk_set(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext], Res) when BK > HK ->
    is_match_hk_set(BCur, HNext, Res);
% Current binding key must not exist in data, go next binding
is_match_hk_set([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _], Res) when BK < HK ->
    is_match_hk_set(BNext, HCur, Res + 1);
% Current match operator does not exist in message, go next
%  (the same apply for not required key)
is_match_hk_set([{BK, _, _} | BNext], HCur = [{HK, _, _} | _], Res) when BK < HK ->
    is_match_hk_set(BNext, HCur, Res);
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
is_match_hk_set([{_, {_, eq}, BV} | BNext], [{_, _, HV} | HNext], Res) when BV == HV ->
    is_match_hk_set(BNext, HNext, Res + 1);
% Current values must match but do not match; return false
is_match_hk_set([{_, {_, eq}, _} | _], _, _) -> -1;
% Key must not exist, return false
is_match_hk_set([{_, nx, _} | _], _, _) -> -1;
% Current header key must exist; ok go next
is_match_hk_set([{_, ex, _} | BNext], [ _ | HNext], Res) ->
    is_match_hk_set(BNext, HNext, Res + 1);

%  .. with type checking
is_match_hk_set([{_, is, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type of
        longstr -> is_match_hk_set(BNext, HNext, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, ib, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type of
        bool -> is_match_hk_set(BNext, HNext, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, ii, _} | BNext], [{_, _, HV} | HNext], Res) ->
    case is_integer(HV) of
        true -> is_match_hk_set(BNext, HNext, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, in, _} | BNext], [{_, _, HV} | HNext], Res) ->
    case is_number(HV) of
        true -> is_match_hk_set(BNext, HNext, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, ia, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type == array of
        true -> is_match_hk_set(BNext, HNext, Res + 1);
        _ -> -1
    end;

is_match_hk_set([{_, nis, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type of
        longstr -> -1;
        _ -> is_match_hk_set(BNext, HNext, Res + 1)
    end;
is_match_hk_set([{_, nib, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type of
        bool -> -1;
        _ -> is_match_hk_set(BNext, HNext, Res + 1)
    end;
is_match_hk_set([{_, nii, _} | BNext], [{_, _, HV} | HNext], Res) ->
    case is_integer(HV) of
        true -> -1;
        _ -> is_match_hk_set(BNext, HNext, Res + 1)
    end;
is_match_hk_set([{_, nin, _} | BNext], [{_, _, HV} | HNext], Res) ->
    case is_number(HV) of
        true -> -1;
        _ -> is_match_hk_set(BNext, HNext, Res + 1)
    end;
is_match_hk_set([{_, nia, _} | BNext], [{_, Type, _} | HNext], Res) ->
    case Type == array of
        true -> -1;
        _ -> is_match_hk_set(BNext, HNext, Res + 1)
    end;

% <= < != > >=
is_match_hk_set([{_, {_, ne}, BV} | BNext], HCur = [{_, _, HV} | _], Res) when BV /= HV ->
    is_match_hk_set(BNext, HCur, Res + 1);
is_match_hk_set([{_, {_, ne}, _} | _], _, _) -> -1;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
is_match_hk_set([{_, {_, gt}, BV} | BNext], HCur = [{_, _, HV} | _], Res) when is_number(HV), HV > BV ->
    is_match_hk_set(BNext, HCur, Res + 1);
is_match_hk_set([{_, {_, gt}, _} | _], _, _) -> -1;
is_match_hk_set([{_, {_, ge}, BV} | BNext], HCur = [{_, _, HV} | _], Res) when is_number(HV), HV >= BV ->
    is_match_hk_set(BNext, HCur, Res + 1);
is_match_hk_set([{_, {_, ge}, _} | _], _, _) -> -1;
is_match_hk_set([{_, {_, lt}, BV} | BNext], HCur = [{_, _, HV} | _], Res) when is_number(HV), HV < BV ->
    is_match_hk_set(BNext, HCur, Res + 1);
is_match_hk_set([{_, {_, lt}, _} | _], _, _) -> -1;
is_match_hk_set([{_, {_, le}, BV} | BNext], HCur = [{_, _, HV} | _], Res) when is_number(HV), HV =< BV ->
    is_match_hk_set(BNext, HCur, Res + 1);
is_match_hk_set([{_, {_, le}, _} | _], _, _) -> -1;

% Regexes
is_match_hk_set([{_, {_, re}, BV} | BNext], HCur = [{_, longstr, HV} | _], Res) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hk_set(BNext, HCur, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, {_, re}, _} | _], _, _) -> -1;
is_match_hk_set([{_, {_, nre}, BV} | BNext], HCur = [{_, longstr, HV} | _], Res) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> is_match_hk_set(BNext, HCur, Res + 1);
        _ -> -1
    end;
is_match_hk_set([{_, {_, nre}, _} | _], _, _) -> -1.


% any
% --------------------------------------
is_match_hk_any([], _) -> false;
% Yet some nx op without data
is_match_hk_any([{_, nx, _} | _], []) -> true;
% No more message header but still match operator to check; return false
is_match_hk_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
is_match_hk_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext]) when BK > HK ->
    is_match_hk_any(BCur, HNext);
% Current binding key must not exist in data
is_match_hk_any([{BK, nx, _} | _], [{HK, _, _} | _]) when BK < HK -> true;
% Current binding key does not exist in message; go next binding
is_match_hk_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _]) when BK < HK ->
    is_match_hk_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match
is_match_hk_any([{_, {_, eq}, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist
is_match_hk_any([{_, ex, _} | _], _) -> true;

% HK type checking
is_match_hk_any([{_, is, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type of
        longstr -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, ib, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type of
        bool -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, ii, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_integer(HV) of
        true -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, in, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_number(HV) of
        true -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, ia, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type == array of
        true -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;

is_match_hk_any([{_, nis, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type of
        longstr -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;
is_match_hk_any([{_, nib, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type of
        bool -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;
is_match_hk_any([{_, nii, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_integer(HV) of
        true -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;
is_match_hk_any([{_, nin, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_number(HV) of
        true -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;
is_match_hk_any([{_, nia, _} | BNext], HCur = [{_, Type, _} | _]) ->
    case Type == array of
        true -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;

is_match_hk_any([{_, {_, ne}, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;

is_match_hk_any([{_, {_, gt}, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV > BV -> true;
is_match_hk_any([{_, {_, ge}, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV >= BV -> true;
is_match_hk_any([{_, {_, lt}, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV < BV -> true;
is_match_hk_any([{_, {_, le}, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV =< BV -> true;

% Regexes
is_match_hk_any([{_, {_, re}, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, {_, nre}, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;
% No match yet; go next
is_match_hk_any([_ | BNext], HCur) ->
    is_match_hk_any(BNext, HCur).



%% -----------------------------------------------------------------------------
%% Binding validation
%% -----------------------------------------------------------------------------

%% Binding type
%% ---------------------------------------------------------

get_match_bt([]) -> 1;
get_match_bt([ {<<"x-match">>, longstr, <<"any">>} | _ ]) -> 0;
get_match_bt([ {<<"x-match">>, longstr, <<"all">>} | _ ]) -> 1;
get_match_bt([ {<<"x-match">>, longstr, <<"eq">>} | _ ]) -> 4;
get_match_bt([ {<<"x-match">>, longstr, <<"set">>} | _ ]) -> {3, 0};
get_match_bt([ {<<"x-match">>, longstr, <<"set ", NB/binary>>} | _ ]) ->
    try
        N = binary_to_integer(NB),
        true = (N >= 0),
        {3, N}
      catch _:_ -> {error, {binding_invalid, "Invalid binding type", []}}
    end;
get_match_bt([ {<<"x-match">>, _, _} | _ ]) ->
    {error, {binding_invalid, "Invalid binding type", []}};
get_match_bt([ _ | Tail]) ->
    get_match_bt(Tail).


%% Validate list type usage
%% -----------------------------------------------------------------------------
validate_list_type_usage(BT, Args) ->
    validate_list_type_usage(BT, Args, Args).
% OK go to next validation
validate_list_type_usage(_, [], Args) ->
    validate_no_deep_lists(Args);

% = and != vs all and any
% props
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?pr=", ?BIN>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in this binding type", []}};
validate_list_type_usage(any, [ {<<"x-?pr!=", ?BIN>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in this binding type", []}};
% rk
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?rk=">>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in this binding type", []}};
validate_list_type_usage(any, [ {<<"x-?rk!=">>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in this binding type", []}};
% headers
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?hkv= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in this binding type", []}};
validate_list_type_usage(all, [ {<<"x-?hk?v= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in this binding type", []}};
validate_list_type_usage(any, [ {<<"x-?hkv!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in this binding type", []}};
validate_list_type_usage(any, [ {<<"x-?hk?v!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in this binding type", []}};

% Routing facilities
% --------------------------------------
validate_list_type_usage(_, [ {<< RuleKey:9/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addqre-">> ; RuleKey==<<"x-delqre-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};
validate_list_type_usage(_, [ {<< RuleKey:10/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addq!re-">> ; RuleKey==<<"x-delq!re-">> ; RuleKey==<<"x-add1qre-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};
validate_list_type_usage(_, [ {<< RuleKey:11/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-add1q!re-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};

% --------------------------------------
validate_list_type_usage(BT, [ {<< RuleKey/binary >>, array, _} | Tail ], Args) ->
    RKL = binary_to_list(RuleKey),
    MatchOperators = ["x-?hkv<", "x-?hk?v<", "x-?hkv>", "x-?hk?v>", "x-?pr<", "x-?pr>", "x-?dt"],
    case lists:filter(fun(S) -> lists:prefix(S, RKL) end, MatchOperators) of
        [] -> validate_list_type_usage(BT, Tail, Args);
        _ -> {error, {binding_invalid, "Invalid use of list type with < or > operators and/or datetime related", []}}
    end;
% Else go next
validate_list_type_usage(BT, [ _ | Tail ], Args) ->
    validate_list_type_usage(BT, Tail, Args).


%% Binding can't have array in array :
%% -----------------------------------------------------------------------------
validate_no_deep_lists(Args) -> 
    validate_no_deep_lists(Args, [], Args).

validate_no_deep_lists([], [], Args) ->
    FlattenedArgs = flatten_table(Args),
    validate_op(FlattenedArgs);
validate_no_deep_lists(_, [_ | _], _) ->
    {error, {binding_invalid, "Invalid use of list in list", []}};
validate_no_deep_lists([ {_, array, Vs} | Tail ], _, Args) ->
    ArrInArr = [ bad || {array, _} <- Vs ],
    validate_no_deep_lists(Tail, ArrInArr, Args); 
validate_no_deep_lists([ _ | Tail ], _, Args) ->
    validate_no_deep_lists(Tail, [], Args).


%% Operators validation
%% -----------------------------------------------------------------------------
validate_op([]) -> ok;
% x-match have been validated upstream
validate_op([ {<<"x-match">>, _, _} | Tail ]) ->
    validate_op(Tail);
% Decimal type is forbidden
validate_op([ {_, decimal, _} | _ ]) ->
    {error, {binding_invalid, "Usage of decimal type is forbidden", []}};
% Do not think that can't happen... it can ! :)
validate_op([ {<<>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's argument key can't be empty", []}};

% Properties
% -------------------------------------
% exnx
validate_op([ {Op, longstr, Prop} | Tail ]) when
        (Op == <<"x-?prex">> orelse Op == <<"x-?pr!ex">>) andalso
        (Prop == <<"content_type">> orelse Prop == <<"content_encoding">> orelse
            Prop == <<"correlation_id">> orelse Prop == <<"reply_to">> orelse
            Prop == <<"expiration">> orelse Prop == <<"message_id">> orelse
            Prop == <<"type">> orelse  Prop == <<"user_id">> orelse
            Prop == <<"app_id">> orelse  Prop == <<"cluster_id">> orelse
            Prop == <<"delivery_mode">> orelse Prop == <<"priority">> orelse
            Prop == <<"timestamp">>
        ) ->
    validate_op(Tail);
% str (expiration is naturally an integer)
validate_op([ {ArgK = <<"x-?pr", ?BIN>>, longstr, ArgV} | Tail ]) ->
    case binary:split(ArgK, <<" ">>) of
        [Op, Prop] when (Prop==<<"content_type">> orelse Prop==<<"content_encoding">> orelse
             Prop==<<"correlation_id">> orelse Prop==<<"reply_to">> orelse
             Prop==<<"message_id">> orelse Prop==<<"type">> orelse
             Prop==<<"user_id">> orelse Prop==<<"app_id">> orelse Prop==<<"cluster_id">>) ->
                 if
                     (Op==<<"x-?pr=">> orelse Op==<<"x-?pr!=">>) ->
                         validate_op(Tail);
                     (Op==<<"x-?prre">> orelse Op==<<"x-?pr!re">>) ->
                         validate_regex(ArgV, Tail);
                     true -> {error, {binding_invalid, "Invalid property match operator", []}}
                 end;
        _ -> {error, {binding_invalid, "Invalid property match operator", []}}
    end;
% int
validate_op([ {ArgK = <<"x-?pr", ?BIN>>, _, Int} | Tail ]) when is_integer(Int) ->
    case binary:split(ArgK, <<" ">>) of
        [Op, Prop] when (Prop==<<"delivery_mode">> orelse Prop==<<"priority">> orelse
                 Prop==<<"timestamp">> orelse Prop==<<"expiration">>
                 ) andalso (Op==<<"x-?pr=">> orelse Op==<<"x-?pr!=">>
                 orelse Op==<<"x-?pr<">> orelse Op==<<"x-?pr<=">>
                 orelse Op==<<"x-?pr>=">> orelse Op==<<"x-?pr>">>) ->
                     validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid property match operator", []}}
    end;

% Datettime match ops
validate_op([ {Op, longstr, Regex} | Tail ]) when
        Op==<<"x-?dture">> orelse Op==<<"x-?dtu!re">>
            orelse Op==<<"x-?dtlre">> orelse Op==<<"x-?dtl!re">> ->
    validate_regex(Regex, Tail);

% Routing key ops
validate_op([ {Op, longstr, _} | Tail ]) when Op==<<"x-?rk=">> orelse Op==<<"x-?rk!=">> ->
    validate_op(Tail);
validate_op([ {Op, longstr, Regex} | Tail ]) when Op==<<"x-?rkre">> orelse Op==<<"x-?rk!re">> ->
    validate_regex(Regex, Tail);
% AMQP topics (check is done from the result of the regex only)
validate_op([ {Op, longstr, Topic} | Tail ]) when Op==<<"x-?rkta">> orelse Op==<<"x-?rk!ta">> ->
    Regex = topic_amqp_to_re(Topic),
    case is_regex_valid(Regex) of
        true -> validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid AMQP topic", []}}
    end;
validate_op([ {Op, longstr, Topic} | Tail ]) when Op==<<"x-?rktaci">> orelse Op==<<"x-?rk!taci">> ->
    Regex = topic_amqp_to_re_ci(Topic),
    case is_regex_valid(Regex) of
        true -> validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid AMQP topic", []}}
    end;

% Dests ops (exchange)
validate_op([ {Op, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) when
        Op==<<"x-adde-ontrue">> orelse Op==<<"x-adde-onfalse">> orelse
            Op==<<"x-dele-ontrue">> orelse Op==<<"x-dele-onfalse">> ->
    validate_op(Tail);
% Dests ops (queue)
validate_op([ {Op, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) when
        Op==<<"x-addq-ontrue">> orelse Op==<<"x-addq-onfalse">> orelse
            Op==<<"x-delq-ontrue">> orelse Op==<<"x-delq-onfalse">> ->
    validate_op(Tail);
% Dests ops regex (queue)
validate_op([ {Op, longstr, Regex} | Tail ]) when
        Op==<<"x-addqre-ontrue">> orelse Op==<<"x-addq!re-ontrue">> orelse
            Op==<<"x-addqre-onfalse">> orelse Op==<<"x-addq!re-onfalse">> orelse
            Op==<<"x-add1qre-ontrue">> orelse Op==<<"x-add1q!re-ontrue">> orelse
            Op==<<"x-add1qre-onfalse">> orelse Op==<<"x-add1q!re-onfalse">> orelse
            Op==<<"x-delqre-ontrue">> orelse Op==<<"x-delq!re-ontrue">> orelse
            Op==<<"x-delqre-onfalse">> orelse Op==<<"x-delq!re-onfalse">> ->
    validate_regex(Regex, Tail);

% Msg dests ops
validate_op([ {Op, longstr, <<>>} | Tail ]) when
        Op==<<"x-msg-addq-ontrue">> orelse Op==<<"x-msg-addq-onfalse">> orelse
            Op==<<"x-msg-adde-ontrue">> orelse Op==<<"x-msg-adde-onfalse">> orelse
            Op==<<"x-msg-delq-ontrue">> orelse Op==<<"x-msg-delq-onfalse">> orelse
            Op==<<"x-msg-dele-ontrue">> orelse Op==<<"x-msg-dele-onfalse">> orelse
            Op==<<"x-msg-addqre-ontrue">> orelse Op==<<"x-msg-addqre-onfalse">> orelse
            Op==<<"x-msg-delqre-ontrue">> orelse Op==<<"x-msg-delqre-onfalse">> ->
    validate_op(Tail);

% Binding order
validate_op([ {<<"x-order">>, _, V} | Tail ]) when is_integer(V) andalso V > 0 ->
    IsSuperUser = is_super_user(),
    if
        IsSuperUser -> validate_op(Tail);
        V > 99 andalso V < 1000000 -> validate_op(Tail);
        true -> {error, {binding_invalid, "Binding's order must be an integer between 100 and 999999", []}}
    end;

% Gotos
validate_op([ {<<"x-goto-on", BoolBin/binary>>, _, V} | Tail ]) when
        (BoolBin == <<"true">> orelse BoolBin == <<"false">>)
            andalso is_integer(V) andalso V > 0 ->
    IsSuperUser = is_super_user(),
    if
        IsSuperUser -> validate_op(Tail);
        V > 99 andalso V < 1000000 -> validate_op(Tail);
        true -> {error, {binding_invalid, "Binding's goto must be an integer between 100 and 999999", []}}
   end;

% Stops
validate_op([ {<<"x-stop-ontrue">>, longstr, <<>>} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-stop-onfalse">>, longstr, <<>>} | Tail ]) ->
    validate_op(Tail);

validate_op([ {Op, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) when
        Op==<<"x-?hkex">> orelse Op==<<"x-?hkexs">> orelse Op==<<"x-?hkexn">> orelse
            Op==<<"x-?hkexb">> orelse Op==<<"x-?hkex!s">> orelse Op==<<"x-?hkex!n">> orelse
            Op==<<"x-?hkex!b">> orelse Op==<<"x-?hk!ex">> orelse
            Op==<<"x-?hkexi">> orelse Op==<<"x-?hkex!i">> orelse
            Op==<<"x-?hkexa">> orelse Op==<<"x-?hkex!a">> ->
    validate_op(Tail);

% Operators hkv with < or > must be numeric only.
validate_op([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hk?v<= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hk?v< ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hk?v>= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hk?v> ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};

validate_op([ {<<"x-?hkv= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hk?v!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkvre ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
    validate_regex(Regex, Tail);
validate_op([ {<<"x-?hk?vre ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
    validate_regex(Regex, Tail);
validate_op([ {<<"x-?hkv!re ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
    validate_regex(Regex, Tail);
validate_op([ {<<"x-?hk?v!re ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
    validate_regex(Regex, Tail);

validate_op([ {InvalidKey = <<"x-", _/binary>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Unknown or invalid argument's key '~p'", [InvalidKey]}};
validate_op([ _ | Tail ]) ->
    validate_op(Tail).

% -------------------------------------
validate_regex(Regex, Tail) ->
    case is_regex_valid(Regex) of
        true -> validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid regex '~ts'", [Regex]}}
    end.



% Regex validation
is_regex_valid(<<>>) -> false;
is_regex_valid(Regex) ->
    case re:compile(Regex) of
        {ok, _} -> true;
        _ -> false
    end.



%% Transform AMQP topic to regex
%%     Void topic will be treated as an error
%% -----------------------------------------------------------------------------
topic_amqp_to_re(<<>>) -> <<>>;
topic_amqp_to_re(TopicBin) ->
    topic_amqp_to_re2([ "^" | erlang:binary_to_list(TopicBin)]).
topic_amqp_to_re_ci(<<>>) -> <<>>;
topic_amqp_to_re_ci(TopicBin) ->
    topic_amqp_to_re2([ "^(?i)" | erlang:binary_to_list(TopicBin)]).

topic_amqp_to_re2(Topic) ->
    Topic1 = string:replace(Topic, "*", "(\\w+)", all),
    Topic2 = string:replace(Topic1, "#.", "(\\w+.)*"),
    Topic3 = string:replace(Topic2, ".#", "(.\\w+)*"),
    Topic4 = string:replace(Topic3, ".", "\\.", all),
    Topic5 = string:replace(Topic4, "#", ".*"),
    list_to_binary(lists:flatten([Topic5, "$"])).


%% Transform x-del-dest operator
%% -----------------------------------------------------------------------------
transform_x_del_dest(Args, Dest) -> transform_x_del_dest(Args, [], Dest).

transform_x_del_dest([], Ret, _) -> Ret;
transform_x_del_dest([ {<<"x-del-dest">>, longstr, <<>>} | Tail ], Ret, Dest = #resource{kind = queue, name = RName} ) ->
    transform_x_del_dest(Tail, [ {<<"x-delq-ontrue">>, longstr, RName} | Ret], Dest);
transform_x_del_dest([ {<<"x-del-dest">>, longstr, <<>>} | Tail ], Ret, Dest = #resource{kind = exchange, name = RName} ) ->
    transform_x_del_dest(Tail, [ {<<"x-dele-ontrue">>, longstr, RName} | Ret], Dest);
transform_x_del_dest([ Op | Tail ], Ret, Dest) ->
    transform_x_del_dest(Tail, [ Op | Ret], Dest).



%% -----------------------------------------------------------------------------
%% Get Match Operators
%% Called by add_binding after validation
%% -----------------------------------------------------------------------------

%% Get Header Key operators from binding's arguments
%% -----------------------------------------------------------------------------
get_match_hk_ops(BindingArgs) ->
    MatchOperators = get_match_hk_ops(BindingArgs, []),
    lists:keysort(1, MatchOperators).

get_match_hk_ops([], Result) -> Result;
% Does a key exist ?
get_match_hk_ops([ {<<"x-?hkex">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ex, nil} | Res]);
% Does a key exist and its value of type..
get_match_hk_ops([ {<<"x-?hkexs">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, is, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkexi">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ii, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkexa">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ia, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkexn">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, in, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkexb">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ib, nil} | Res]);
% Does a key exist and its value NOT of type..
get_match_hk_ops([ {<<"x-?hkex!s">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nis, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!i">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nii, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!a">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nia, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!n">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nin, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!b">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nib, nil} | Res]);
% Does a key NOT exist ?
get_match_hk_ops([ {<<"x-?hk!ex">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nx, nil} | Res]);

% operators <= < = != > >=
get_match_hk_ops([ {<<"x-?hkv<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, le}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, le}, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, lt}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, lt}, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, eq}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, eq}, V} | Res]);
get_match_hk_ops([ {<<"x-?hkvre ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, re}, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hk?vre ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, re}, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, ne}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, ne}, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv!re ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, nre}, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hk?v!re ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, nre}, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, gt}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, gt}, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {1, ge}, V} | Res]);
get_match_hk_ops([ {<<"x-?hk?v>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, {0, ge}, V} | Res]);

%% All others beginnig with x- are other operators
get_match_hk_ops([ {<<"x-", _/binary>>, _, _} | Tail ], Res) ->
    get_match_hk_ops (Tail, Res);

% Headers exchange compatibility : all other cases imply 'eq'
get_match_hk_ops([ {K, _, V} | T ], Res) ->
    get_match_hk_ops (T, [ {K, {1, eq}, V} | Res]).


%% Get Routing Key operators from binding's arguments
%% -----------------------------------------------------------------------------
get_match_rk_ops([], Result) -> Result;

get_match_rk_ops([ {<<"x-?rk=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkeq, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkne, V} | Res]);
get_match_rk_ops([ {<<"x-?rkre">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkre, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!re">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rknre, V} | Res]);

get_match_rk_ops([ {<<"x-?rkta">>, _, TopicBin} | Tail ], Res) ->
    RegexBin = topic_amqp_to_re(TopicBin),
    get_match_rk_ops(Tail, [ {rkre, RegexBin} | Res]);
get_match_rk_ops([ {<<"x-?rk!ta">>, _, TopicBin} | Tail ], Res) ->
    RegexBin = topic_amqp_to_re(TopicBin),
    get_match_rk_ops(Tail, [ {rknre, RegexBin} | Res]);
get_match_rk_ops([ {<<"x-?rktaci">>, _, TopicBin} | Tail ], Res) ->
    RegexBin = topic_amqp_to_re_ci(TopicBin),
    get_match_rk_ops(Tail, [ {rkre, RegexBin} | Res]);
get_match_rk_ops([ {<<"x-?rk!taci">>, _, TopicBin} | Tail ], Res) ->
    RegexBin = topic_amqp_to_re_ci(TopicBin),
    get_match_rk_ops(Tail, [ {rknre, RegexBin} | Res]);

get_match_rk_ops([ _ | Tail ], Result) ->
    get_match_rk_ops(Tail, Result).


%% Get Destinations operators from binding's arguments
%% -----------------------------------------------------------------------------
get_match_de_ops([], Result) -> Result;

% No destination at all
get_match_de_ops([ {<<"x-?!de">>, _, << >>} | Tail ], Res) ->
    get_match_de_ops(Tail, [ no | Res]);
% Queue V belongs to dests
get_match_de_ops([ {<<"x-?deq">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_de_ops(Tail, [ {q, V} | Res]);
% Queue V does not belong to dests
get_match_de_ops([ {<<"x-?de!q">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_de_ops(Tail, [ {qn, V} | Res]);
% Regex V match some queue from dests
get_match_de_ops([ {<<"x-?deqre">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_de_ops(Tail, [ {qre, V} | Res]);
% Regex V does not match some queue from dests
get_match_de_ops([ {<<"x-?deq!re">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_de_ops(Tail, [ {qnre, V} | Res]);
%
get_match_de_ops([ _ | Tail ], Res) ->
    get_match_de_ops(Tail, Res).



% Validation is made upstream
get_binding_order(Args, Default) ->
    case rabbit_misc:table_lookup(Args, <<"x-order">>) of
        undefined  -> Default;
        {_, Order} -> Order
    end.


%% Get properties operators from binding's arguments
%% -----------------------------------------------------------------------------
get_match_pr_ops(Args) ->
    get_match_pr_ops(Args, []).

get_match_pr_ops([], Res) ->
    Res;
get_match_pr_ops([ {K, _, V} | Tail ], Res) when (K == <<"x-?prex">> orelse K == <<"x-?pr!ex">>) ->
    BindingOp = case K of
        <<"x-?prex">> -> ex;
        <<"x-?pr!ex">> -> nx
    end,
    PropId = propName2Id(V),
    get_match_pr_ops(Tail, [ {BindingOp, PropId, V} | Res]);
get_match_pr_ops([ {K = <<"x-?pr", ?BIN>>, T, V} | Tail ], Res) ->
    [PropOp, PropName] = binary:split(K, <<" ">>),
    BindingOp = case PropOp of
        <<"x-?pr=">> -> eq;
        <<"x-?pr!=">> -> ne;
        <<"x-?prre">> -> re;
        <<"x-?pr!re">> -> nre;
        <<"x-?pr<">> -> lt;
        <<"x-?pr<=">> -> le;
        <<"x-?pr>=">> -> ge;
        <<"x-?pr>">> -> gt
    end,
    PropId = propName2Id(PropName),
    % Special case for 'expiration' prop..
    case PropId == ex andalso T == longstr of
        true -> get_match_pr_ops(Tail, [ {BindingOp, PropId, binary_to_integer(V)} | Res]);
        _    -> get_match_pr_ops(Tail, [ {BindingOp, PropId, V} | Res])
    end;
get_match_pr_ops([ _ | Tail ], Res) ->
    get_match_pr_ops(Tail, Res).

propName2Id(V) ->
    case V of
        <<"content_type">> -> ct;
        <<"content_encoding">> -> ce;
        <<"correlation_id">> -> co;
        <<"reply_to">> -> re;
        <<"expiration">> -> ex;
        <<"message_id">> -> me;
        <<"type">> -> ty;
        <<"user_id">> -> us;
        <<"app_id">> -> ap;
        <<"cluster_id">> -> cl;
        <<"delivery_mode">> -> de;
        <<"priority">> -> pr;
        <<"timestamp">> -> ti
    end.


%% Get datetime operators from binding's arguments
%% -----------------------------------------------------------------------------
get_match_dt_ops([], Result) -> Result;
get_match_dt_ops([{<<"x-?dture">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dture, V} | Res]);
get_match_dt_ops([{<<"x-?dtunre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtunre, V} | Res]);
get_match_dt_ops([{<<"x-?dtlre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtlre, V} | Res]);
get_match_dt_ops([{<<"x-?dtlnre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtlnre, V} | Res]);
get_match_dt_ops([_ | T], R) ->
    get_match_dt_ops(T, R).

%% Get stop operators from binding's arguments
%% -----------------------------------------------------------------------------
get_stop_operators([], Result) -> Result;
get_stop_operators([{<<"x-stop-ontrue">>, _, _} | T], {_, StopOnFalse}) ->
    get_stop_operators(T, {1, StopOnFalse});
get_stop_operators([{<<"x-stop-onfalse">>, _, _} | T], {StopOnTrue, _}) ->
    get_stop_operators(T, {StopOnTrue, 1});
get_stop_operators([_ | T], R) ->
    get_stop_operators(T, R).

%% Get goto operators from binding's arguments
%% -----------------------------------------------------------------------------
get_goto_operators([], Result) -> Result;
get_goto_operators([{<<"x-goto-ontrue">>, _, N} | T], {_, GotoOnFalse}) ->
    get_goto_operators(T, {N, GotoOnFalse});
get_goto_operators([{<<"x-goto-onfalse">>, _, N} | T], {GotoOnTrue, _}) ->
    get_goto_operators(T, {GotoOnTrue, N});
get_goto_operators([_ | T], R) ->
    get_goto_operators(T, R).


%% DAT : Destinations to Add on True
%% DAF : Destinations to Add on False
%% DDT : Destinations to Del on True
%% DDF : Destinations to Del on False
% Stop when no more ops
get_dests_operators(_, [], Dests, DestsRE) -> {Dests, DestsRE};
% Named
get_dests_operators(VHost, [{<<"x-addq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-addq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
% With regex
get_dests_operators(VHost, [{<<"x-addqre-ontrue">>, longstr, R} | T], Dests, {_,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {R, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addqre-onfalse">>, longstr, R} | T], Dests, {DATRE,_,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, R, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-add1qre-ontrue">>, longstr, R} | T], Dests, {_,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {{R}, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-add1qre-onfalse">>, longstr, R} | T], Dests, {DATRE,_,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, {R}, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,_,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, R, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,_,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, R,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,_,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,R,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,_,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,R,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-add1q!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,_,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE, {R},DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-add1q!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,_,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,{R},DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,_,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,R,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,_}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,R});
get_dests_operators(VHost, [_ | T], Dests, DestsRE) ->
    get_dests_operators(VHost, T, Dests, DestsRE).


%% Operators decided by the message (by the producer)
get_msg_ops([], Result) -> Result;
%% Routing facilities :
get_msg_ops([{<<"x-msg-addq-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 1), DreRight >>);
get_msg_ops([{<<"x-msg-addq-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 2), DreRight >>);
get_msg_ops([{<<"x-msg-adde-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 4), DreRight >>);
get_msg_ops([{<<"x-msg-adde-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 8), DreRight >>);
get_msg_ops([{<<"x-msg-delq-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 16), DreRight >>);
get_msg_ops([{<<"x-msg-delq-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 32), DreRight >>);
get_msg_ops([{<<"x-msg-dele-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 64), DreRight >>);
get_msg_ops([{<<"x-msg-dele-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 128), DreRight >>);
% Via regex : set right to value too !
get_msg_ops([{<<"x-msg-addqre-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 1), (DreRight bor 1) >>);
get_msg_ops([{<<"x-msg-addqre-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 2), (DreRight bor 2) >>);
get_msg_ops([{<<"x-msg-delqre-ontrue">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 16), (DreRight bor 16) >>);
get_msg_ops([{<<"x-msg-delqre-onfalse">>, _, _} | T], << DRight:8, DreRight:8 >>) ->
    get_msg_ops(T, << (DRight bor 32), (DreRight bor 32) >>);
% Go next
get_msg_ops([_ | T], Result) ->
    get_msg_ops(T, Result).



%% Flatten one level for list of values (array)
flatten_table(Args) ->
    flatten_table(Args, []).

flatten_table([], Result) -> Result;
flatten_table ([ {K, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T, V} <- Vs ],
        flatten_table (Tail, lists:append ([ Res , Result ]));
flatten_table ([ {K, T, V} | Tail ], Result) ->
        flatten_table (Tail, [ {K, T, V} | Result ]).


is_super_user() ->
    false.


create(transaction, #exchange{arguments = Args, name = XName}) ->
    Config = get_exchange_config(Args),
    ExchConfig = {0, Config},
    InitRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = [ExchConfig]},
    ok = mnesia:write(?TABLE, InitRecord, write);
create(_, _) -> ok.


validate(#exchange{arguments = Args}) ->
    get_exchange_config(Args),
    ok.


get_exchange_config(Args) ->
    get_exchange_config(Args, []).

get_exchange_config([], PList) -> PList;
get_exchange_config([{<<"min-payload-size">>, long, I} | Tail], PList) ->
    NewPList = [{1, {0, I}} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"min-payload-size">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);
get_exchange_config([{<<"min-payload-size-silent">>, long, I} | Tail], PList) ->
    NewPList = [{1, {1, I}} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"min-payload-size-silent">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);
get_exchange_config([{<<"max-payload-size">>, long, I} | Tail], PList) ->
    NewPList = [{2, {0, I}} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"max-payload-size">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);
get_exchange_config([{<<"max-payload-size-silent">>, long, I} | Tail], PList) ->
    NewPList = [{2, {1, I}} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"max-payload-size-silent">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);

get_exchange_config([{<<"max-headers-depth">>, long, I} | Tail], PList) ->
    MaxH = case proplists:get_value(3, PList) of
        undefined -> {I, 16384};
        {_, MaxSize} -> {I, MaxSize}
    end,
    NewPList = [{3, MaxH} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"max-headers-depth">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);
get_exchange_config([{<<"max-headers-size">>, long, I} | Tail], PList) ->
    MaxH = case proplists:get_value(3, PList) of
        undefined -> {2, I};
        {MaxDepth, _} -> {MaxDepth, I}
    end,
    NewPList = [{3, MaxH} | PList],
    get_exchange_config(Tail, NewPList);
get_exchange_config([{<<"max-headers-size">>, _, _} | _], _) ->
    rabbit_misc:protocol_error(precondition_failed, "Invalid exchange argument.", []);

get_exchange_config([_ | Tail], PList) ->
    get_exchange_config(Tail, PList).


pickoneof([]) -> [];
pickoneof(L) ->
    {_, Elem} = lists:nth(1, lists:sort([ {crypto:strong_rand_bytes(2), N} || N <- L])),
    [Elem].


delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (?TABLE, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.


add_binding(transaction, #exchange{name = #resource{virtual_host = VHost} = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BT = get_match_bt(BindingArgs),

% Branching operators and "super user" (goto and stop cannot be declared in same binding)
    BindingOrder = get_binding_order(BindingArgs, 10000),
    {GOT, GOF} = get_goto_operators(BindingArgs, {1, 1}),
    {SOT, SOF} = get_stop_operators(BindingArgs, {0, 0}),
    {GOT2, SOT2} = case (is_super_user() orelse SOT==0) of
        true -> {GOT, SOT};
        _ -> {1000000, 0}
    end,
    {GOF2, SOF2} = case (is_super_user() orelse SOF==0) of
        true -> {GOF, SOF};
        _ -> {1000000, 0}
    end,
    StopOperators = {SOT2, SOF2},

    << MsgDests:8, MsgDestsRE:8 >> = get_msg_ops(BindingArgs, << 0, 0 >>),
    FlattenedBindindArgs = flatten_table(transform_x_del_dest(BindingArgs, Dest)),

    MatchHKOps = get_match_hk_ops(FlattenedBindindArgs),
    MatchRKOps = get_match_rk_ops(FlattenedBindindArgs, []),
    MatchDTOps = get_match_dt_ops(FlattenedBindindArgs, []),
    MatchATOps = get_match_pr_ops(FlattenedBindindArgs),
    MatchDEOps = get_match_de_ops(FlattenedBindindArgs, []),

    MatchOps = {MatchHKOps, MatchRKOps, MatchDTOps, MatchATOps, MatchDEOps},
    DefaultDests = {ordsets:new(), ordsets:new(), ordsets:new(), ordsets:new()},
    {Dests, DestsRE} = get_dests_operators(VHost, FlattenedBindindArgs, DefaultDests, ?DEFAULT_DESTS_RE),

    NewBinding1 = {BindingOrder, BT, Dest, MatchOps},
    NewBinding2 = case {GOT2, GOF2, StopOperators, Dests, DestsRE, << MsgDests:8, MsgDestsRE:8 >>} of
        {1, 1, {0, 0}, DefaultDests, ?DEFAULT_DESTS_RE, <<0, 0>>} -> NewBinding1;
        {_, _, _, DefaultDests, ?DEFAULT_DESTS_RE, <<0, 0>>} -> erlang:append_element(NewBinding1, {GOT2, GOF2, StopOperators});
        _ -> erlang:append_element(NewBinding1, {GOT2, GOF2, StopOperators, Dests, DestsRE, << MsgDests:8, MsgDestsRE:8 >>})
    end,
    NewBinding = erlang:append_element(NewBinding2, BindingId),
    [#?RECORD{?RECVALUE = CurrentOrderedBindings}] = mnesia:read(?TABLE, XName, write),
    NewBindings = lists:keysort(1, [NewBinding | CurrentOrderedBindings]),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = NewBindings},
    ok = mnesia:write(?TABLE, NewRecord, write);
add_binding(_, _, _) ->
    ok.


remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    [#?RECORD{?RECVALUE = [ExchConfig | CurrentOrderedBindings]}] = mnesia:read(?TABLE, XName, write),
    NewOrderedBindings = remove_bindings_ids(BindingIdsToDelete, CurrentOrderedBindings, []),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = [ExchConfig | NewOrderedBindings]},
    ok = mnesia:write(?TABLE, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
% TODO : use element(tuple_size(Binding), Binding) to use one declaration only
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(#exchange{name = Name, arguments = Args}, RequiredArgs) ->
    rabbit_misc:assert_args_equivalence(Args, RequiredArgs, Name, [<<"alternate-exchange">>, <<"min-payload-size">>, <<"max-payload-size">>, <<"min-payload-size-silent">>, <<"max-payload-size-silent">>, <<"max-headers-depth">>, <<"max-headers-size">>]).

