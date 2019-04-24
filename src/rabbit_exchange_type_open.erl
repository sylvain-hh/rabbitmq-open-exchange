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
    case rabbit_misc:table_lookup(Args2, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_list_type_usage(all, Args2);
        {longstr, <<"any">>} -> validate_list_type_usage(any, Args2);
        undefined            -> validate_list_type_usage(all, Args2);
        _ -> {error, {binding_invalid, "Invalid x-match operator", []}}
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

    CurrentOrderedBindings = case ets:lookup(?TABLE, Name) of
        [] -> [];
        [#?RECORD{?RECVALUE = Bs}] -> Bs
    end,
% TODO : Change goto 0 to 1, also in other get_routes..
    get_routes({RK, MsgProperties}, CurrentOrderedBindings, 0, ordsets:new()).



%% Get routes
%% -----------------------------------------------------------------------------
get_routes(_, [], _, ResDests) ->
    ordsets:to_list(ResDests);

get_routes(MsgData, [ {_, BindingType, Dest, {HKRules, RKRules, DTRules, ATRules}, << 0 >>, _} | T ], _, ResDests) ->
    case is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules) of
        true -> get_routes(MsgData, T, 0, ordsets:add_element(Dest, ResDests));
           _ -> get_routes(MsgData, T, 0, ResDests)
    end;

% Simplest like direct's exchange type
get_routes(MsgData={MsgRK, _}, [ {_, BindingType, _, {HKRules, RKRules, DTRules, ATRules}, << 128 >>, _} | T ], _, ResDests) ->
    case is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules) of
        true -> get_routes(MsgData, T, 0, ordsets:add_element(rabbit_misc:r(get(xopen_vhost), queue, MsgRK), ResDests));
           _ -> get_routes(MsgData, T, 0, ResDests)
    end;

% Jump to the next binding satisfying the last goto operator
get_routes(MsgData, [ {Order, _, _, _, _, _, _} | T ], GotoOrder, ResDests) when GotoOrder > Order ->
    get_routes(MsgData, T, GotoOrder, ResDests);

% At this point, main dest is rewritten for next cases
get_routes(MsgData = {MsgRK, _}, [ {Order, BindingType, _, MatchOps, << 128 >>, Other, BindingId} | T ], GotoOrder, ResDests) ->
    NewDest = rabbit_misc:r(get(xopen_vhost), queue, MsgRK),
    get_routes(MsgData, [{Order, BindingType, NewDest, MatchOps, << 0 >>, Other, BindingId} | T], GotoOrder, ResDests);

get_routes(MsgData, [ {_, BindingType, Dest, {HKRules, RKRules, DTRules, ATRules}, _, {GOT, GOF, StopOperators}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules), StopOperators} of
        {true,{1,_}}  -> ordsets:add_element(Dest, ResDests);
        {false,{_,1}} -> ResDests;
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:add_element(Dest, ResDests));
        {false,_}     -> get_routes(MsgData, T, GOF, ResDests)
    end;

get_routes(MsgData, [ {_, BindingType, Dest, {HKRules, RKRules, DTRules, ATRules}, _, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, ?DEFAULT_DESTS_RE, <<0, 0>>}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT);
        {false,{_,1}} -> ordsets:subtract(ordsets:union(DAF, ResDests), DDF);
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT));
        {false,_}     -> get_routes(MsgData, T, GOF, ordsets:subtract(ordsets:union(DAF, ResDests), DDF))
    end;

get_routes(MsgData={_, MsgProps}, [ {_, BindingType, Dest, {HKRules, RKRules, DTRules, ATRules}, _, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, ?DEFAULT_DESTS_RE, <<BindDest:8, 0>>}, _} | T ], _, ResDests) ->
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
    case {is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union([DAT, MsgAQT, MsgAET, ordsets:add_element(Dest, ResDests)]), ordsets:union([DDT, MsgDQT, MsgDET]));
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF, MsgAQF, MsgAEF, ResDests]), ordsets:union([DDF, MsgDQF, MsgDEF]));
        {true,_}      -> get_routes(MsgData, T, GOT, ordsets:subtract(ordsets:union([DAT, MsgAQT, MsgAET, ordsets:add_element(Dest, ResDests)]), ordsets:union([DDT, MsgDQT, MsgDET])));
        {false,_}     -> get_routes(MsgData, T, GOF, ordsets:subtract(ordsets:union([DAF, MsgAQF, MsgAEF, ResDests]), ordsets:union([DDF, MsgDQF, MsgDEF])))
    end;

get_routes(MsgData={_, MsgProps}, [ {_, BindingType, Dest, {HKRules, RKRules, DTRules, ATRules}, _, {GOT, GOF, StopOperators, {DAT, DAF, DDT, DDF}, {DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE}, <<BindDest:8, BindREDest:8>>}, _} | T ], _, ResDests) ->
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
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATRE, [ {capture, none} ]) == match])
    end,
    DAFREsult = case DAFRE of
        nil -> ordsets:from_list([]);
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
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATNRE, [ {capture, none} ]) /= match])
    end,
    DAFNREsult = case DAFNRE of
        nil -> ordsets:from_list([]);
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

    case {is_match(BindingType, MsgData, HKRules, RKRules, DTRules, ATRules), StopOperators} of
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
flatten_table_msg ([ {Op, array, Vs} | Tail ], Result)
        when Op==<<"x-addq-ontrue">>; Op==<<"x-addq-onfalse">>;
             Op==<<"x-adde-ontrue">>; Op==<<"x-adde-onfalse">>;
             Op==<<"x-delq-ontrue">>; Op==<<"x-delq-onfalse">>;
             Op==<<"x-dele-ontrue">>; Op==<<"x-dele-onfalse">> ->
    Res = [ { Op, T, V } || {T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} <- Vs ],
    flatten_table_msg (Tail, lists:append ([ Res , Result ]));
% Filter ops and on type
flatten_table_msg ([ {Op, longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result)
        when Op==<<"x-addq-ontrue">>; Op==<<"x-addq-onfalse">>;
             Op==<<"x-adde-ontrue">>; Op==<<"x-adde-onfalse">>;
             Op==<<"x-delq-ontrue">>; Op==<<"x-delq-onfalse">>;
             Op==<<"x-dele-ontrue">>; Op==<<"x-dele-onfalse">> ->
    flatten_table_msg (Tail, [ {Op, longstr, V} | Result ]);
flatten_table_msg ([ {Op, longstr, Regex} | Tail ], Result)
        when Op==<<"x-addqre-ontrue">>; Op==<<"x-addqre-onfalse">>;
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

% Optimization for headers only..
is_match(all, {_, MsgProps}, HKRules, [], [], []) ->
    is_match_hk_all(HKRules, MsgProps#'P_basic'.headers);
is_match(any, {_, MsgProps}, HKRules, [], [], []) ->
    is_match_hk_any(HKRules, MsgProps#'P_basic'.headers);

is_match(BindingType, {MsgRK, MsgProps}, HKRules, RKRules, [], []) ->
    case BindingType of
        all -> is_match_rk(BindingType, RKRules, MsgRK)
               andalso is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers);
        any -> is_match_rk(BindingType, RKRules, MsgRK)
               orelse is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers)
    end;
is_match(BindingType, {MsgRK, MsgProps}, HKRules, RKRules, [], ATRules) ->
    case BindingType of
        all -> is_match_rk(BindingType, RKRules, MsgRK)
               andalso is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers)
               andalso is_match_pr(BindingType, ATRules, MsgProps);
        any -> is_match_rk(BindingType, RKRules, MsgRK)
               orelse is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers)
               orelse is_match_pr(BindingType, ATRules, MsgProps)
    end;
is_match(BindingType, {MsgRK, MsgProps}, HKRules, RKRules, DTRules, ATRules) ->
    case BindingType of
        all -> is_match_rk(BindingType, RKRules, MsgRK)
               andalso is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers)
               andalso is_match_pr(BindingType, ATRules, MsgProps)
               andalso is_match_dt(BindingType, DTRules);
        any -> is_match_rk(BindingType, RKRules, MsgRK)
               orelse is_match_hk(BindingType, HKRules, MsgProps#'P_basic'.headers)
               orelse is_match_pr(BindingType, ATRules, MsgProps)
               orelse is_match_dt(BindingType, DTRules)
    end.


%% Match on datetime
%% -----------------------------------------------------------------------------
is_match_dt(all, Rules) ->
    case get(xopen_dtl) of
        undefined -> save_datetimes();
        _ -> ok
    end,
    is_match_dt_all(Rules);
is_match_dt(any, Rules) ->
    case get(xopen_dtl) of
        undefined -> save_datetimes();
        _ -> ok
    end,
    is_match_dt_any(Rules).

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
        match -> is_match_dt_any(Tail);
        _ -> true
    end;
is_match_dt_any([ {dtlre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtlnre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> is_match_dt_any(Tail);
        _ -> true
    end.


%% Match on properties
%% -----------------------------------------------------------------------------
is_match_pr(all, Rules, MsgProp) ->
    is_match_pr_all(Rules, MsgProp);
is_match_pr(any, Rules, MsgProp) ->
    is_match_pr_any(Rules, MsgProp).

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
is_match_pr_any([], _) ->
    false;
is_match_pr_any([ {PropOp, PropId, V} | Tail], MsgProps) ->
    MsgPropV = get_msg_prop_value(PropId, MsgProps),
    if
        PropOp == nx andalso MsgPropV == undefined ->
            true;
        MsgPropV /= undefined ->
            case PropOp of
                ex -> true;
                nx -> is_match_pr_any(Tail, MsgProps);
                eq when MsgPropV == V ->
                    true;
                eq -> is_match_pr_any(Tail, MsgProps);
                ne when MsgPropV /= V ->
                    true;
                ne -> is_match_pr_any(Tail, MsgProps);
                re -> case re:run(MsgPropV, V, [ {capture, none} ]) == match of
                          true -> true;
                          _ -> is_match_pr_any(Tail, MsgProps)
                      end;
                nre -> case re:run(MsgPropV, V, [ {capture, none} ]) == nomatch of
                           true -> true;
                           _ -> is_match_pr_any(Tail, MsgProps)
                       end;
                lt when MsgPropV < V ->
                    true;
                lt -> is_match_pr_any(Tail, MsgProps);
                le when MsgPropV =< V ->
                    true;
                le -> is_match_pr_any(Tail, MsgProps);
                ge when MsgPropV >= V ->
                    true;
                ge -> is_match_pr_any(Tail, MsgProps);
                gt when MsgPropV > V ->
                    true;
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


%% Match on routing key
%% -----------------------------------------------------------------------------
is_match_rk(all, Rules, RK) ->
    is_match_rk_all(Rules, RK);
is_match_rk(any, Rules, RK) ->
    is_match_rk_any(Rules, RK).

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
is_match_hk(all, Args, Headers) ->
    is_match_hk_all(Args, Headers);
is_match_hk(any, Args, Headers) ->
    is_match_hk_any(Args, Headers).

% all
% --------------------------------------
% No more match operator to check; return true
is_match_hk_all([], _) -> true;

% Purge nx op on no data as all these are true
is_match_hk_all([{_, nx, _} | BNext], []) ->
    is_match_hk_all(BNext, []);

% No more message header but still match operator to check other than nx; return false
is_match_hk_all(_, []) -> false;

% Current header key not in match operators; go next header with current match operator
is_match_hk_all(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hk_all(BCur, HNext);
% Current binding key must not exist in data, go next binding
is_match_hk_all([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hk_all(BNext, HCur);
% Current match operator does not exist in message; return false
is_match_hk_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
is_match_hk_all([{_, eq, BV} | BNext], [{_, _, HV} | HNext])
    when BV == HV -> is_match_hk_all(BNext, HNext);
% Current values must match but do not match; return false
is_match_hk_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
is_match_hk_all([{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
is_match_hk_all([{_, ex, _} | BNext], [ _ | HNext]) ->
    is_match_hk_all(BNext, HNext);

%  .. with type checking
is_match_hk_all([{_, is, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        longstr -> is_match_hk_all(BNext, HNext);
        _ -> false
    end;
is_match_hk_all([{_, ib, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        bool -> is_match_hk_all(BNext, HNext);
        _ -> false
    end;
is_match_hk_all([{_, in, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_number(HV) of
        true -> is_match_hk_all(BNext, HNext);
        _ -> false
    end;

is_match_hk_all([{_, nis, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        longstr -> false;
        _ -> is_match_hk_all(BNext, HNext)
    end;
is_match_hk_all([{_, nib, _} | BNext], [{_, Type, _} | HNext]) ->
    case Type of
        bool -> false;
        _ -> is_match_hk_all(BNext, HNext)
    end;
is_match_hk_all([{_, nin, _} | BNext], [{_, _, HV} | HNext]) ->
    case is_number(HV) of
        true -> false;
        _ -> is_match_hk_all(BNext, HNext)
    end;

% <= < != > >=
is_match_hk_all([{_, ne, BV} | BNext], HCur = [{_, _, HV} | _])
    when BV /= HV -> is_match_hk_all(BNext, HCur);
is_match_hk_all([{_, ne, _} | _], _) -> false;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
is_match_hk_all([{_, gt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV > BV -> is_match_hk_all(BNext, HCur);
is_match_hk_all([{_, gt, _} | _], _) -> false;
is_match_hk_all([{_, ge, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV >= BV -> is_match_hk_all(BNext, HCur);
is_match_hk_all([{_, ge, _} | _], _) -> false;
is_match_hk_all([{_, lt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV < BV -> is_match_hk_all(BNext, HCur);
is_match_hk_all([{_, lt, _} | _], _) -> false;
is_match_hk_all([{_, le, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV =< BV -> is_match_hk_all(BNext, HCur);
is_match_hk_all([{_, le, _} | _], _) -> false;

% Regexes
is_match_hk_all([{_, re, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hk_all(BNext, HCur);
        _ -> false
    end;
is_match_hk_all([{_, re, _} | _], _) -> false;
is_match_hk_all([{_, nre, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> is_match_hk_all(BNext, HCur);
        _ -> false
    end;
is_match_hk_all([{_, nre, _} | _], _) -> false.

% any
% --------------------------------------
% No more match operator to check; return false
is_match_hk_any([], _) -> false;
% Yet some nx op without data; return true
is_match_hk_any([{_, nx, _} | _], []) -> true;
% No more message header but still match operator to check; return false
is_match_hk_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
is_match_hk_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hk_any(BCur, HNext);
% Current binding key must not exist in data, return true
is_match_hk_any([{BK, nx, _} | _], [{HK, _, _} | _])
    when BK < HK -> true;
% Current binding key does not exist in message; go next binding
is_match_hk_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hk_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match; return true
is_match_hk_any([{_, eq, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist; return true
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
is_match_hk_any([{_, in, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_number(HV) of
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
is_match_hk_any([{_, nin, _} | BNext], HCur = [{_, _, HV} | _]) ->
    case is_number(HV) of
        true -> is_match_hk_any(BNext, HCur);
        _ -> true
    end;

is_match_hk_any([{_, ne, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;

is_match_hk_any([{_, gt, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV > BV -> true;
is_match_hk_any([{_, ge, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV >= BV -> true;
is_match_hk_any([{_, lt, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV < BV -> true;
is_match_hk_any([{_, le, BV} | _], [{_, _, HV} | _]) when is_number(HV), HV =< BV -> true;

% Regexes
is_match_hk_any([{_, re, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_hk_any(BNext, HCur)
    end;
is_match_hk_any([{_, nre, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
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

%% Validate list type usage
%% -----------------------------------------------------------------------------
validate_list_type_usage(BindingType, Args) ->
    validate_list_type_usage(BindingType, Args, Args).
% OK go to next validation
validate_list_type_usage(_, [], Args) ->
    validate_no_deep_lists(Args);

% = and != vs all and any
% props
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?pr=", ?BIN>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?pr!=", ?BIN>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in binding type 'any'", []}};
% rk
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?rk=">>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?rk!=">>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in binding type 'any'", []}};
% headers
% --------------------------------------
validate_list_type_usage(all, [ {<<"x-?hkv= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator in binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?hkv!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator in binding type 'any'", []}};

% Routing facilities
% --------------------------------------
validate_list_type_usage(_, [ {<< RuleKey:9/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addqre-">> ; RuleKey==<<"x-delqre-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};
validate_list_type_usage(_, [ {<< RuleKey:10/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addq!re-">> ; RuleKey==<<"x-delq!re-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};

% --------------------------------------
validate_list_type_usage(BindingType, [ {<< RuleKey/binary >>, array, _} | Tail ], Args) ->
    RKL = binary_to_list(RuleKey),
    MatchOperators = ["x-?hkv<", "x-?hkv>", "x-?pr<", "x-?pr>", "x-?dt"],
    case lists:filter(fun(S) -> lists:prefix(S, RKL) end, MatchOperators) of
        [] -> validate_list_type_usage(BindingType, Tail, Args);
        _ -> {error, {binding_invalid, "Invalid use of list type with < or > operators and/or datetime related", []}}
    end;
% Else go next
validate_list_type_usage(BindingType, [ _ | Tail ], Args) ->
    validate_list_type_usage(BindingType, Tail, Args).


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
% x-match have been checked upstream due to checks on list type
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
validate_op([ {Op, longstr, Regex} | Tail ])
        when Op==<<"x-?dture">> orelse Op==<<"x-?dtu!re">>
            orelse Op==<<"x-?dtlre">> orelse Op==<<"x-?dtl!re">> ->
    validate_regex(Regex, Tail);

% Routing key ops
validate_op([ {Op, longstr, _} | Tail ])
        when Op==<<"x-?rk=">> orelse Op==<<"x-?rk!=">> ->
    validate_op(Tail);
validate_op([ {Op, longstr, Regex} | Tail ])
        when Op==<<"x-?rkre">> orelse Op==<<"x-?rk!re">> ->
    validate_regex(Regex, Tail);
% AMQP topics (check is done from the result of the regex only)
validate_op([ {Op, longstr, Topic} | Tail ])
        when Op==<<"x-?rkta">> orelse Op==<<"x-?rk!ta">> ->
    Regex = topic_amqp_to_re(Topic),
    case is_regex_valid(Regex) of
        true -> validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid AMQP topic", []}}
    end;
validate_op([ {Op, longstr, Topic} | Tail ])
        when Op==<<"x-?rktaci">> orelse Op==<<"x-?rk!taci">> ->
    Regex = topic_amqp_to_re_ci(Topic),
    case is_regex_valid(Regex) of
        true -> validate_op(Tail);
        _ -> {error, {binding_invalid, "Invalid AMQP topic", []}}
    end;

% Dests ops (exchange)
validate_op([ {Op, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ])
        when Op==<<"x-adde-ontrue">> orelse Op==<<"x-adde-onfalse">> orelse
            Op==<<"x-dele-ontrue">> orelse Op==<<"x-dele-onfalse">> ->
    validate_op(Tail);
% Dests ops (queue)
validate_op([ {Op, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ])
        when Op==<<"x-addq-ontrue">> orelse Op==<<"x-addq-onfalse">> orelse
            Op==<<"x-delq-ontrue">> orelse Op==<<"x-delq-onfalse">> ->
    validate_op(Tail);
% Dests ops regex (queue)
validate_op([ {Op, longstr, Regex} | Tail ])
        when Op==<<"x-addqre-ontrue">> orelse Op==<<"x-addq!re-ontrue">> orelse
            Op==<<"x-addqre-onfalse">> orelse Op==<<"x-addq!re-onfalse">> orelse
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
        Op==<<"x-msg-delqre-ontrue">> orelse Op==<<"x-msg-delqre-onfalse">> orelse
        Op==<<"x-msg-destq-rk">> ->
    validate_op(Tail);

% Binding order
validate_op([ {<<"x-order">>, _, V} | Tail ]) when is_integer(V) ->
    IsSuperUser = is_super_user(),
    if
        IsSuperUser -> validate_op(Tail);
        V > 99 andalso V < 1000000 -> validate_op(Tail);
        true -> {error, {binding_invalid, "Binding's order must be an integer between 100 and 999999", []}}
    end;

% Gotos
validate_op([ {<<"x-goto-on", BoolBin/binary>>, _, V} | Tail ]) when (BoolBin == <<"true">> orelse BoolBin == <<"false">>) andalso is_integer(V) ->
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
        (Op==<<"x-?hkex">> orelse Op==<<"x-?hkexs">> orelse Op==<<"x-?hkexn">> orelse
         Op==<<"x-?hkexb">> orelse Op==<<"x-?hkex!s">> orelse Op==<<"x-?hkex!n">> orelse
         Op==<<"x-?hkex!b">> orelse Op==<<"x-?hk!ex">>
        ) ->
    validate_op(Tail);

% Operators hkv with < or > must be numeric only.
validate_op([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_op([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, Num} | Tail ]) when is_number(Num) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};

validate_op([ {<<"x-?hkv= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkv!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) ->
    validate_op(Tail);
validate_op([ {<<"x-?hkvre ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
    validate_regex(Regex, Tail);
validate_op([ {<<"x-?hkv!re ", ?ONE_CHAR_AT_LEAST>>, longstr, Regex} | Tail ]) ->
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




%% By default the binding type is 'all'; and that's it :)
parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all.

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
get_match_hk_ops([ {<<"x-?hkexn">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, in, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkexb">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ib, nil} | Res]);
% Does a key exist and its value NOT of type..
get_match_hk_ops([ {<<"x-?hkex!s">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nis, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!n">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nin, nil} | Res]);
get_match_hk_ops([ {<<"x-?hkex!b">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nib, nil} | Res]);
% Does a key NOT exist ?
get_match_hk_ops([ {<<"x-?hk!ex">>, _, K} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nx, nil} | Res]);

% operators <= < = != > >=
get_match_hk_ops([ {<<"x-?hkv<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, le, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, lt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, eq, V} | Res]);
get_match_hk_ops([ {<<"x-?hkvre ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, re, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ne, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv!re ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nre, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, gt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ge, V} | Res]);

%% All others beginnig with x- are other operators
get_match_hk_ops([ {<<"x-", _/binary>>, _, _} | Tail ], Res) ->
    get_match_hk_ops (Tail, Res);

% Headers exchange compatibility : all other cases imply 'eq'
get_match_hk_ops([ {K, _, V} | T ], Res) ->
    get_match_hk_ops (T, [ {K, eq, V} | Res]).


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
get_match_pr_ops([ {K, _, V} | Tail ], Res) when
        (K == <<"x-?prex">> orelse K == <<"x-?pr!ex">>) ->
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
get_dests_operators(VHost, [{<<"x-delqre-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,_,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, R, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,_,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, R,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,_,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,R,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,_,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,R,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,_,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,R,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,_}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,R});
get_dests_operators(VHost, [_ | T], Dests, DestsRE) ->
    get_dests_operators(VHost, T, Dests, DestsRE).


%% Operators decided by the message (by the producer)
get_msg_ops([], Result) -> Result;
% Replace dest by rk
get_msg_ops([{<<"x-msg-destq-rk">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << DRight, DreRight, (OTH bor 128) >>);
%% Routing facilities :
get_msg_ops([{<<"x-msg-addq-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 1), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-addq-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 2), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-adde-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 4), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-adde-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 8), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-delq-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 16), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-delq-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 32), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-dele-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 64), DreRight, OTH >>);
get_msg_ops([{<<"x-msg-dele-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 128), DreRight, OTH >>);
% Via regex : set right to value too !
get_msg_ops([{<<"x-msg-addqre-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 1), (DreRight bor 1), OTH >>);
get_msg_ops([{<<"x-msg-addqre-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 2), (DreRight bor 2), OTH >>);
get_msg_ops([{<<"x-msg-delqre-ontrue">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 16), (DreRight bor 16), OTH >>);
get_msg_ops([{<<"x-msg-delqre-onfalse">>, _, _} | T], << DRight:8, DreRight:8, OTH:8 >>) ->
    get_msg_ops(T, << (DRight bor 32), (DreRight bor 32), OTH >>);
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



validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (?TABLE, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.


add_binding(transaction, #exchange{name = #resource{virtual_host = VHost} = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),

% Branching operators and "super user" (goto and stop cannot be declared in same binding)
    BindingOrder = get_binding_order(BindingArgs, 10000),
    {GOT, GOF} = get_goto_operators(BindingArgs, {0, 0}),
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

    << MsgDests:8, MsgDestsRE:8, MsgDestsOpt:8 >> = get_msg_ops(BindingArgs, << 0, 0, 0 >>),
    FlattenedBindindArgs = flatten_table(transform_x_del_dest(BindingArgs, Dest)),
    MatchHKOps = get_match_hk_ops(FlattenedBindindArgs),
    MatchRKOps = get_match_rk_ops(FlattenedBindindArgs, []),
    MatchDTOps = get_match_dt_ops(FlattenedBindindArgs, []),
    MatchATOps = get_match_pr_ops(FlattenedBindindArgs),
    MatchOps = {MatchHKOps, MatchRKOps, MatchDTOps, MatchATOps},
    DefaultDests = {ordsets:new(), ordsets:new(), ordsets:new(), ordsets:new()},
    {Dests, DestsRE} = get_dests_operators(VHost, FlattenedBindindArgs, DefaultDests, ?DEFAULT_DESTS_RE),
    CurrentOrderedBindings = case mnesia:read(?TABLE, XName, write) of
        [] -> [];
        [#?RECORD{?RECVALUE = E}] -> E
    end,
    NewBinding1 = {BindingOrder, BindingType, Dest, MatchOps, << MsgDestsOpt:8 >>},
    NewBinding2 = case {GOT2, GOF2, StopOperators, Dests, DestsRE, << MsgDests:8, MsgDestsRE:8 >>} of
        {0, 0, {0, 0}, DefaultDests, ?DEFAULT_DESTS_RE, <<0, 0>>} -> NewBinding1;
        {_, _, _, DefaultDests, ?DEFAULT_DESTS_RE, <<0, 0>>} -> erlang:append_element(NewBinding1, {GOT2, GOF2, StopOperators});
        _ -> erlang:append_element(NewBinding1, {GOT2, GOF2, StopOperators, Dests, DestsRE, << MsgDests:8, MsgDestsRE:8 >>})
    end,
    NewBinding = erlang:append_element(NewBinding2, BindingId),
    NewBindings = lists:keysort(1, [NewBinding | CurrentOrderedBindings]),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = NewBindings},
    ok = mnesia:write(?TABLE, NewRecord, write);
add_binding(_, _, _) ->
    ok.


remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentOrderedBindings = case mnesia:read(?TABLE, XName, write) of
        [] -> [];
        [#?RECORD{?RECVALUE = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewOrderedBindings = remove_bindings_ids(BindingIdsToDelete, CurrentOrderedBindings, []),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = NewOrderedBindings},
    ok = mnesia:write(?TABLE, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
% TODO : use element(tuple_size(Binding), Binding) to use one declaration only
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

