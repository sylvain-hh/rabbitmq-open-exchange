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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2013 Pivotal Software, Inc.  All rights reserved.
%%

-module(basic_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


%% KV macros helper
-define(KVstr(K,V), {<< K >>, longstr, << V >>}).
-define(KVbool(K,V), {<< K >>, bool, V}).
-define(KVlong(K,V), {<< K >>, long, V}).
-define(KVfloat(K,V), {<< K >>, float, V}).

-define(Karray(K,A), {<< K >>, array, A}).
-define(Vstr(V), {longstr, << V >>}).
-define(Vbool(V), {bool, V}).
-define(Vlong(V), {long, V}).
-define(Vfloat(V), {float, V}).

%% KV shortcuts
-define(KV1s, ?KVstr("k1", "v1")).
-define(KV2s, ?KVstr("k2", "v2")).
-define(KV3s, ?KVstr("k3", "v3")).
-define(KV4s, ?KVstr("k4", "v4")).

%% Binding type operators
-define(BTany, ?KVstr("x-match", "any")).
-define(BTall, ?KVstr("x-match", "all")).

%% Matching operators
%%    On header keys
-define(HKex(K), ?KVstr("x-?hkex", K)).
-define(HKexArr(A), ?Karray("x-?hkex", A)).
-define(HKNex(K), ?KVstr("x-?hk!ex", K)).
-define(HKNexArr(A), ?Karray("x-?hk!ex", A)).

-define(HKisS(K), ?KVstr("x-?hkexs", K)).
-define(HKisSArr(A), ?Karray("x-?hkexs", A)).
-define(HKisN(K), ?KVstr("x-?hkexn", K)).
-define(HKisNArr(A), ?Karray("x-?hkexn", A)).
-define(HKisB(K), ?KVstr("x-?hkexb", K)).
-define(HKisBArr(A), ?Karray("x-?hkexb", A)).
-define(HKisNotS(K), ?KVstr("x-?hkex!s", K)).
-define(HKisNotSArr(A), ?Karray("x-?hkex!s", A)).
-define(HKisNotN(K), ?KVstr("x-?hkex!n", K)).
-define(HKisNotNArr(A), ?Karray("x-?hkex!n", A)).
-define(HKisNotB(K), ?KVstr("x-?hkex!b", K)).
-define(HKisNotBArr(A), ?Karray("x-?hkex!b", A)).
%%    On binding routing key
-define(RKre(R), ?KVstr("x-?rkre", R)).
-define(RKreArr(A), ?Karray("x-?rkre", A)).
-define(RKNre(R), ?KVstr("x-?rk!re", R)).
-define(RKNreArr(A), ?Karray("x-?rk!re", A)).
%%  On properties
-define(ATex(K), ?KVstr("x-?prex", K)).
-define(ATnx(K), ?KVstr("x-?pr!ex", K)).
-define(ATexArr(A), ?Karray("x-?prex", A)).
-define(ATnxArr(A), ?Karray("x-?pr!ex", A)).

%% Branching operators
-define(BOrder(N), ?KVlong("x-order", N)).
-define(GotoOnT(N), ?KVlong("x-goto-ontrue", N)).
-define(GotoOnF(N), ?KVlong("x-goto-onfalse", N)).
-define(StopOnT, ?KVstr("x-stop-ontrue", "")).
-define(StopOnF, ?KVstr("x-stop-onfalse", "")).

%% Routing operators
-define(AddQOnT(Q), ?KVstr("x-addq-ontrue", Q)).
-define(AddQOnF(Q), ?KVstr("x-addq-onfalse", Q)).
-define(AddQReOnT(R), ?KVstr("x-addqre-ontrue", R)).
-define(AddQReOnF(R), ?KVstr("x-addqre-onfalse", R)).
-define(AddQNReOnT(R), ?KVstr("x-addq!re-ontrue", R)).
-define(AddQNReOnF(R), ?KVstr("x-addq!re-onfalse", R)).
-define(DelQOnT(Q), ?KVstr("x-delq-ontrue", Q)).
-define(DelQOnF(Q), ?KVstr("x-delq-onfalse", Q)).
-define(DelQReOnT(Q), ?KVstr("x-delqre-ontrue", Q)).
-define(DelQReOnF(Q), ?KVstr("x-delqre-onfalse", Q)).
-define(DelQNReOnT(Q), ?KVstr("x-delq!re-ontrue", Q)).
-define(DelQNReOnF(Q), ?KVstr("x-delq!re-onfalse", Q)).
-define(DelDest, ?KVstr("x-del-dest", "")).
%%    Allow from message
-define(MsgAddQOnT, ?KVstr("x-msg-addq-ontrue", "")).
-define(MsgAddQReOnT, ?KVstr("x-msg-addqre-ontrue", "")).
%%    From message, RK is the queue destination
-define(RKasQueue, ?KVstr("x-msg-destq-rk", "")).


%% -------------------------------------------------------------------
%% Groups and testcases declaration
%% -------------------------------------------------------------------

all() ->
  [
    { group, match_operators }
  , { group, branching_operators }
  , { group, routing_operators }
  ].

groups() ->
  [
    { match_operators, [],
      [
        { headers_exchange_compatible, [ parallel, {repeat, 10} ], [
            hec_simple_binding_types
          , hec_all_str_only, hec_all_mixed_types, hec_any_str_only, hec_any_mixed_types
        ]}
      , { on_headers_keys_values, [ parallel, {repeat, 10} ], [
            hkv_ltgt, hkv, hkv_array, hkv_re, hkv_re_array
        ]}
      , { on_headers_keys, [ parallel, {repeat, 10} ], [
            hk_exnex, hk_array
          , hk_is, hk_nis, hk_isnis_array
        ]}
      , { on_routing_key, [ parallel, {repeat, 10} ], [
            rk, rk_array, rk_re, rk_re_array, rk_topic_AMQP, rk_topic_AMQP_any, rk_not_topic_AMQP
        ]}
      , { on_properties, [ parallel, {repeat, 10} ], [
            pr_exnx, pr_exnx_array, pr_userid
        ]}
%%    , { on_datetime, [], [
%%          dt
%%      ]}
%%    , { forbidden_declarations, [], [
%%          fail1
%%      ]}
      ]
    }
  , { branching_operators, [ parallel, {repeat, 10} ],
      [ default_order
        , order_goto_ontrue, order_goto_onfalse
        , order_stop_ontrue, order_stop_onfalse
      ]
    }
  , { routing_operators, [ parallel, {repeat, 10} ],
      [ delq, delq_array, delq_re, delq_nre, self_delq
        , addq, addq_array, addq_re, addq_nre
        , msg_addq, msg_addq_re, msg_addq_nre
        , rk_as_queue
      ]
    }
  ].


%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------


% Connection should be shared among tests at suite level
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),

    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).


init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.


init_per_testcase(Testcase, Config) ->
% Open channel
    Channel = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
% Declare exchange
    Exchange = list_to_binary(atom_to_list(Testcase)),
    Decl = #'exchange.declare'{exchange = Exchange, type = <<"x-open">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Decl),
% Declare queues
    TestConfig = [ { atom_to_list(Testcase) ++ ":" ++ QName, Bs } || { QName, Bs } <- ?MODULE:Testcase() ],
    Queues = [
      begin QDecl = #'queue.declare'{queue = list_to_binary(QName2)},
        #'queue.declare_ok'{} = amqp_channel:call(Channel, QDecl),
        list_to_binary(QName2)
      end || { QName2, _ } <- TestConfig 
    ],
% Save all 3
    Config1 = rabbit_ct_helpers:set_config(Config, [ {test_channel, Channel}, {test_exchange, Exchange}, {test_queues, Queues} ]),
% Declare bindings
    declare_qsbs(Config1, TestConfig),
% and let's go !
    rabbit_ct_helpers:testcase_started(Config1, Testcase).


end_per_testcase(Testcase, Config) ->
% Delete exchange (implies bindings)
    CallDelE = #'exchange.delete'{exchange = ?config(test_exchange, Config)},
    #'exchange.delete_ok'{} = amqp_channel:call(?config(test_channel, Config), CallDelE),
% Close channel
    rabbit_ct_client_helpers:close_channel(?config(test_channel, Config)),
% All done.
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

hec_simple_binding_types() ->
    [
        { "all", [ [?BTall] ]}
      , { "any", [ [?BTany] ]}
    ].
hec_simple_binding_types(Config) ->
    [ Qall, Qany ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    M2 = sendmsg_hds(Config, []),
    M3 = sendmsg_rk(Config, "word1.word2 "),

    check_queue_messages(Config, Qall, [M1, M2, M3]),
    check_queue_messages(Config, Qany, []).


hec_all_str_only() ->
    [
        { "123or234", [
              [?KV1s, ?KV2s, ?KV3s]
            , [?KV2s, ?KV3s, ?KV4s]
        ]}
      , { "34", [
              [?KV3s, ?KV4s]
        ]}
      , { "45", [
              [?KV4s, ?KVstr("k5", "v5")]
        ]}
    ].
hec_all_str_only(Config) ->
    [ Q123or234, Q34, Q45 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_hds(Config, [?KV1s, pickoneof([?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_hds(Config, [?KV2s, pickoneof([?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_hds(Config, [?KV3s, ?KVstr("k5", "v5")]),
    sendmsg_hds(Config, pickoneof([[?KVstr("v4", "k4"), ?KVstr("v3", "k3")], [?KVstr("v4", "k4"), ?KVstr("v5", "k5")]])),

    Notv1 = pickoneof(["k1", "v0", "v1 ", " v1", "v2"]),
    Notv2 = pickoneof(["k2", "v1", "v2 ", " v2", "v3"]),
    Notv3 = pickoneof(["k3", "v2", "v3 ", " v3", "v4"]),
    Notv4 = pickoneof(["k4", "v3", "v4 ", " v4", "v5"]),
    Notv5 = pickoneof(["k5", "v4", "v5 ", " v5", "v6"]),
    sendmsg_hds(Config, [kvStr("k1", Notv1), kvStr("k2", Notv2), kvStr("k3", Notv3), kvStr("k4", Notv4), kvStr("k5", Notv5)]),
    Notk1 = pickoneof([" k1", "k1 "]),
    Notk2 = pickoneof([" k2", "k2 "]),
    Notk3 = pickoneof([" k3", "k3 "]),
    Notk4 = pickoneof([" k4", "k4 "]),
    Notk5 = pickoneof([" k5", "k5 "]),
    sendmsg_hds(Config, [kvStr(Notk1, "v1"), kvStr(Notk2, "v2"), kvStr(Notk3, "v3"), kvStr(Notk4, "v4"), kvStr(Notk5, "v5")]),

    P123 = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    P1234 = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s]),
    P12345 = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")]),
    P2345 = sendmsg_hds(Config, [?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")]),
    P34 = sendmsg_hds(Config, [?KV3s, ?KV4s]),

    check_queue_messages(Config, Q123or234, [P123, P1234, P12345, P2345]),
    check_queue_messages(Config, Q34, [P1234, P12345, P2345, P34]),
    check_queue_messages(Config, Q45, [P12345, P2345]).


hec_all_mixed_types() ->
    [
        { "q1", [
              [?KV1s, ?KVbool("b1", true)]
            , [?KVlong("k1", 36), ?KVbool("b1", true)]
            , [?KVlong("k1", 36), ?KVlong("b1", 1)]
        ]}
      , { "q2", [
              [?KV1s, ?KVstr("b1", "true")]
            , [?KVstr("k1", "36"), ?KVbool("b1", true)]
            , [?KVfloat("k1", 36), ?KVfloat("b1", 1)]
        ]}
    ].
hec_all_mixed_types(Config) ->
    [ Q1, Q2 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KV1s]),
    sendmsg_hds(Config, [?KVbool("b1", true)]),
    sendmsg_hds(Config, [?KVbool("b1", false), ?KV1s]),
    sendmsg_hds(Config, [?KVbool(" b1", true), ?KV1s]),
    sendmsg_hds(Config, [?KVbool("b1 ", true), ?KV1s]),
    sendmsg_hds(Config, [?KVstr("b1", "true"), ?KVstr("k1", "36")]),
    sendmsg_hds(Config, [?KVbool("b1", true), ?KVfloat("k1", 36.01)]),

    P1 = sendmsg_hds(Config, [?KVbool("b1", true), ?KVlong("k1", 36)]),
    P2 = sendmsg_hds(Config, [?KVbool("b1", true), ?KVfloat("k1", 36)]),
    P3 = sendmsg_hds(Config, [?KVbool("b1", true), ?KVfloat("k1", 36.0)]),
    P4 = sendmsg_hds(Config, [?KVbool("b1", true), ?KVstr("k1", "36")]),
    P5 = sendmsg_hds(Config, [?KVstr("b1", "true"), ?KV1s]),
    P6 = sendmsg_hds(Config, [?KVfloat("b1", 1.00), ?KVlong("k1", 36)]),
    P7 = sendmsg_hds(Config, [?KVlong("b1", 1), ?KVfloat("k1", 36.00)]),
    P8 = sendmsg_hds(Config, [?KVfloat("b1", 1.00), ?KVfloat("k1", 36.0)]),
    P9 = sendmsg_hds(Config, [?KVlong("b1", 1), ?KVlong("k1", 36)]),

    check_queue_messages(Config, Q1, [P1, P2, P3, P6, P7, P8, P9]),
    check_queue_messages(Config, Q2, [P4, P5, P6, P7, P8, P9]).


hec_any_str_only() ->
    [
        { "q1", [
              [?BTany, ?KV1s, ?KV3s, ?KV2s]
            , [?KV4s, ?BTany, ?KV3s]
        ]}
        , { "q2", [
              [?KV3s, ?KV4s, ?BTany]
        ]}
        , { "q3", [
              [?KVstr("k5", "v5"), ?BTany]
        ]}
    ].
hec_any_str_only(Config) ->
    [ Q1, Q2, Q3 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KVstr(" k1", "v1")]),
    sendmsg_hds(Config, [?KVstr("k1 ", "v1")]),

    P1 = sendmsg_hds(Config, [?KV1s]),
    P2 = sendmsg_hds(Config, [?KV2s]),
    P3 = sendmsg_hds(Config, [?KV3s]),
    P4 = sendmsg_hds(Config, [?KV4s]),
    P5 = sendmsg_hds(Config, [?KV2s, ?KV1s]),
    P6 = sendmsg_hds(Config, [?KV1s, ?KV2s]),
    P7 = sendmsg_hds(Config, [?KV4s, ?KV3s]),
    P8 = sendmsg_hds(Config, [?KV3s, ?KV4s]),
    P9 = sendmsg_hds(Config, [?KVstr("k5", "v5")]),

    check_queue_messages(Config, Q1, [P1, P2, P3, P4, P5, P6, P7, P8]),
    check_queue_messages(Config, Q2, [P3, P4, P7, P8]),
    check_queue_messages(Config, Q3, [P9]).


hec_any_mixed_types() ->
%% Same config as 'all' but binding type is 'any'
    [
        { "q1", [
              [?BTany, ?KV1s, ?KVbool("b1", true)]
            , [?KVlong("k1", 36), ?BTany, ?KVbool("b1", true)]
            , [?KVlong("k1", 36), ?KVlong("b1", 1), ?BTany]
        ]}
      , { "q2", [
              [?BTany, ?KV1s, ?KVstr("b1", "true")]
            , [?KVstr("k1", "36"), ?BTany, ?KVbool("b1", true)]
            , [?KVfloat("k1", 36), ?KVfloat("b1", 1), ?BTany]
        ]}
    ].
hec_any_mixed_types(Config) ->
    [ Q1, Q2 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KVfloat("k1", 36.001)]),
    sendmsg_hds(Config, [?KVbool("b1", false)]),

    P1 = sendmsg_hds(Config, [?KV1s]),
    P2 = sendmsg_hds(Config, [?KVstr("k1", "36")]),
    P3 = sendmsg_hds(Config, [?KVfloat("k1", 36.0)]),
    P4 = sendmsg_hds(Config, [?KVlong("k1", 36)]),
    P5 = sendmsg_hds(Config, [?KVbool("b1", true)]),
    P6 = sendmsg_hds(Config, [?KVfloat("b1", 1.00), ?KVlong("k1", 36)]),
    P7 = sendmsg_hds(Config, [?KVlong("b1", 1), ?KVfloat("k1", 36.00)]),
    P8 = sendmsg_hds(Config, [?KVfloat("b1", 1.00), ?KVfloat("k1", 36.0)]),
    P9 = sendmsg_hds(Config, [?KVlong("b1", 1), ?KVlong("k1", 36)]),

    check_queue_messages(Config, Q1, [P1, P3, P4, P5, P6, P7, P8, P9]),
    check_queue_messages(Config, Q2, [P1, P2, P3, P4, P5, P6, P7, P8, P9]).


hkv_ltgt() ->
    [
        { "cold", [
              [?KVlong("x-?hkv< temp", -5)]
        ]}
        , { "normal", [
              [?KVlong("x-?hkv>= temp", -5), ?KVlong("x-?hkv<= temp", 30)]
        ]}
        , { "hot", [
              [?KVlong("x-?hkv> temp", 30)]
        ]}
    ].
hkv_ltgt(Config) ->
    [ Qcold, Qnormal, Qhot ] = ?config(test_queues, Config),

%% Comparison operators with '<' or '>' can only match on numeric types
    sendmsg_hds(Config, [?KVstr("temp", "1")]),
    sendmsg_hds(Config, [?KVstr("temp", "-1")]),
    sendmsg_hds(Config, [?KVbool("temp", true)]),
    sendmsg_hds(Config, [?KVbool("temp", false)]),

    HotTemp1 = sendmsg_hds(Config, [?KVlong("temp", 36), ?KVstr("k3", "dummy")]),
    HotTemp2 = sendmsg_hds(Config, [?KVfloat("temp", 30.001)]),
    NormalTemp1 = sendmsg_hds(Config, [?KVfloat("temp", 30.000)]),
    NormalTemp2 = sendmsg_hds(Config, [?KVlong("temp", 30)]),
    ColdTemp1 = sendmsg_hds(Config, [?KVfloat("temp", -30.000)]),
    ColdTemp2 = sendmsg_hds(Config, [?KVfloat("temp", -5.0001)]),

    check_queue_messages(Config, Qhot, [HotTemp1, HotTemp2]),
    check_queue_messages(Config, Qnormal, [NormalTemp1, NormalTemp2]),
    check_queue_messages(Config, Qcold, [ColdTemp1, ColdTemp2]).


hkv() ->
    [
        { "30num", [ [?KVlong("x-?hkv= x-num", 30)] ]}
      , { "30str", [ [?KVstr("x-?hkv= x-num", "30")] ]}
      , { "boolT", [ [?KVbool("x-?hkv= x-num", true)] ]}
      , { "boolF", [ [?KVbool("x-?hkv= x-num", false)] ]}
      , { "not30num", [ [?KVlong("x-?hkv!= x-num", 30)] ]}
      , { "not30str", [ [?KVstr("x-?hkv!= x-num", "30")] ]}
      , { "notBoolT", [ [?KVbool("x-?hkv!= x-num", true)] ]}
      , { "notBoolF", [ [?KVbool("x-?hkv!= x-num", false)] ]}

      , { "30num2", [ [?KVlong("x-?hkv= x-num", 30), ?BTany] ]}
      , { "30str2", [ [?KVstr("x-?hkv= x-num", "30"), ?BTany] ]}
      , { "boolT2", [ [?KVbool("x-?hkv= x-num", true), ?BTany] ]}
      , { "boolF2", [ [?KVbool("x-?hkv= x-num", false), ?BTany] ]}
      , { "not30num2", [ [?KVlong("x-?hkv!= x-num", 30), ?BTany] ]}
      , { "not30str2", [ [?KVstr("x-?hkv!= x-num", "30"), ?BTany] ]}
      , { "notBoolT2", [ [?KVbool("x-?hkv!= x-num", true), ?BTany] ]}
      , { "notBoolF2", [ [?KVbool("x-?hkv!= x-num", false), ?BTany] ]}
    ].
hkv(Config) ->
    [ Q30num, Q30str, QboolT, QboolF, Qnot30num, Qnot30str, QnotboolT, QnotboolF, Q30num2, Q30str2, QboolT2, QboolF2, Qnot30num2, Qnot30str2, QnotboolT2, QnotboolF2 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KVstr("xnum", "30")]),
    sendmsg_hds(Config, [?KVbool("xnum", true)]),

    BoolT = sendmsg_hds(Config, [?KVbool("x-num", true)]),
    BoolF = sendmsg_hds(Config, [?KVbool("x-num", false)]),
    Num30 = sendmsg_hds(Config, [?KVfloat("x-num", 30.0)]),
    Str30 = sendmsg_hds(Config, [?KVstr("x-num", "30")]),

    check_queue_messages(Config, Q30num, [Num30]),
    check_queue_messages(Config, Q30num2, [Num30]),
    check_queue_messages(Config, Q30str, [Str30]),
    check_queue_messages(Config, Q30str2, [Str30]),
    check_queue_messages(Config, QboolT, [BoolT]),
    check_queue_messages(Config, QboolT2, [BoolT]),
    check_queue_messages(Config, QboolF, [BoolF]),
    check_queue_messages(Config, QboolF2, [BoolF]),
    check_queue_messages(Config, Qnot30num, [BoolT, BoolF, Str30]),
    check_queue_messages(Config, Qnot30num2, [BoolT, BoolF, Str30]),
    check_queue_messages(Config, Qnot30str, [BoolT, BoolF, Num30]),
    check_queue_messages(Config, Qnot30str2, [BoolT, BoolF, Num30]),
    check_queue_messages(Config, QnotboolT, [BoolF, Num30, Str30]),
    check_queue_messages(Config, QnotboolT2, [BoolF, Num30, Str30]),
    check_queue_messages(Config, QnotboolF, [BoolT, Num30, Str30]),
    check_queue_messages(Config, QnotboolF2, [BoolT, Num30, Str30]).


hkv_array() ->
    [
        { "isInArray", [
              [?BTany, ?Karray("x-?hkv= x-num", [?Vstr("30"), ?Vlong(30), ?Vbool(true), ?Vbool(false)])]
        ]}
      , { "isNOTinArray", [
              [?BTall, ?Karray("x-?hkv!= x-num", [?Vstr("30"), ?Vlong(30), ?Vbool(true), ?Vbool(false)])]
        ]}
    ].
hkv_array(Config) ->
    [ InArray, NotInArray ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KVbool("xnum", true)]),

    BoolT = sendmsg_hds(Config, [?KVbool("x-num", true)]),
    BoolF = sendmsg_hds(Config, [?KVbool("x-num", false)]),
    Num30 = sendmsg_hds(Config, [?KVfloat("x-num", 30.0)]),
    Str30 = sendmsg_hds(Config, [?KVstr("x-num", "30")]),

    Num3 = sendmsg_hds(Config, [?KVlong("x-num", 3)]),
    Str30Dot = sendmsg_hds(Config, [?KVstr("x-num", "30.")]),

    check_queue_messages(Config, InArray, [BoolT, BoolF, Num30, Str30]),
    check_queue_messages(Config, NotInArray, [Num3, Str30Dot]).


hkv_re() ->
    [
        { "public", [
              [?KVstr("x-?hkvre doc_type", "^(?i)pub(lic)?$")]
        ]}
        , { "private", [
              [?KVstr("x-?hkvre doc_type", "^(?i)priv(ate)?$")]
        ]}
        , { "other", [
              [?KVstr("x-?hkv!re doc_type", "^(?i)(pub(lic)?|priv(ate)?)$")]
        ]}
    ].
hkv_re(Config) ->
    [ Qpub, Qpriv, Qo ] = ?config(test_queues, Config),

%% Regex operators can only match on string type
    sendmsg_hds(Config, [?KVlong("doc_type", 1)]),
    sendmsg_hds(Config, [?KVfloat("doc_type", 1.1)]),
    sendmsg_hds(Config, [?KVbool("doc_type", true)]),
    sendmsg_hds(Config, [?KVbool("doc_type", false)]),

    Pu1 = sendmsg_hds(Config, [?KVstr("doc_type", "pUbLiC")]),
    Pu2 = sendmsg_hds(Config, [?KVstr("doc_type", "PUb")]),
    Pr1 = sendmsg_hds(Config, [?KVstr("doc_type", "PriVAte")]),
    Pr2 = sendmsg_hds(Config, [?KVstr("doc_type", "pRIv")]),
    Ot1 = sendmsg_hds(Config, [?KVstr("doc_type", "PriVA")]),
    Ot2 = sendmsg_hds(Config, [?KVstr("doc_type", "pu")]),

    check_queue_messages(Config, Qpub, [Pu1, Pu2]),
    check_queue_messages(Config, Qpriv, [Pr1, Pr2]),
    check_queue_messages(Config, Qo, [Ot1, Ot2]).


hkv_re_array() ->
    [
        { "startAandEndZ", [
              [?BTall, ?Karray("x-?hkvre word", [?Vstr("^A"), ?Vstr("Z$")])]
        ]}
      , { "startAorEndZ", [
              [?BTany, ?Karray("x-?hkvre word", [?Vstr("^A"), ?Vstr("Z$")])]
        ]}
      , { "notStartAnorEndZ", [
              [?BTall, ?Karray("x-?hkv!re word", [?Vstr("^A"), ?Vstr("Z$")])]
        ]}
      , { "notStartAorEndZ", [
              [?BTany, ?Karray("x-?hkv!re word", [?Vstr("^A"), ?Vstr("Z$")])]
        ]}
    ].
hkv_re_array(Config) ->
    [ QstartAandEndZ, QstartAorEndZ, QnotStartAnorEndZ, QnotStartAorEndZ ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KVlong("dummy", 331)]),

    AxZ = sendmsg_hds(Config, [?KVstr("word", "AxZ")]),
    BxZ = sendmsg_hds(Config, [?KVstr("word", "BxZ")]),
    AxY = sendmsg_hds(Config, [?KVstr("word", "AxY")]),
    BxY = sendmsg_hds(Config, [?KVstr("word", "BxY")]),

    check_queue_messages(Config, QstartAandEndZ, [AxZ]),
    check_queue_messages(Config, QstartAorEndZ, [AxZ, BxZ, AxY]),
    check_queue_messages(Config, QnotStartAnorEndZ, [BxY]),
    check_queue_messages(Config, QnotStartAorEndZ, [BxZ, AxY, BxY]).


hk_exnex() ->
    [
        { "hasType", [ [?HKex("type")] ]}
      , { "notHasType", [ [?HKNex("type")] ]}
      , { "hasId", [ [?HKex("id")] ]}
      , { "notHasId", [ [?HKNex("id")] ]}

      , { "hasType2", [ [?BTany, ?HKex("type")] ]}
      , { "notHasType2", [ [?BTany, ?HKNex("type")] ]}
      , { "hasId2", [ [?BTany, ?HKex("id")] ]}
      , { "notHasId2", [ [?BTany, ?HKNex("id")] ]}
    ].
hk_exnex(Config) ->
    [ QhasType, QnotHasType, QhasId, QnotHasId, QhasType2, QnotHasType2, QhasId2, QnotHasId2 ] = ?config(test_queues, Config),

    HasType1 = sendmsg_hds(Config, [?KVlong("dummy", 1), ?KVbool("type", true)]),
    HasType2 = sendmsg_hds(Config, [?KVlong("type", 1), ?KVbool("dummy", true)]),
    HasId1 = sendmsg_hds(Config, [?KVlong("id", 1), ?KVbool("adummy", true)]),
    HasId2 = sendmsg_hds(Config, [?KVlong("id", 1), ?KVbool("dummy", true)]),
    HasBoth1 = sendmsg_hds(Config, [?KVlong("id", 1), ?KVbool("type", true)]),
    HasBoth2 = sendmsg_hds(Config, [?KVlong("type", 1), ?KVbool("id", true)]),

    check_queues_messages(Config, [QhasType, QhasType2], [HasType1, HasType2, HasBoth1, HasBoth2]),
    check_queues_messages(Config, [QnotHasType, QnotHasType2], [HasId1, HasId2]),
    check_queues_messages(Config, [QhasId, QhasId2], [HasId1, HasId2, HasBoth1, HasBoth2]),
    check_queues_messages(Config, [QnotHasId, QnotHasId2], [HasType1, HasType2]).



hk_is() ->
    [
        { "k1StringN1NumB1Bool", [ [?HKisS("k1"), ?HKisN("n1"), ?HKisB("b1")] ]}
      , { "k1StringOrN1NumOrB1Bool", [ [?BTany, ?HKisS("k1"), ?HKisN("n1"), ?HKisB("b1")] ]}
    ].
hk_is(Config) ->
    [ Qk1StringN1NumB1Bool, Qk1StringOrN1NumOrB1Bool ] = ?config(test_queues, Config),

    Rk1NotString = pickoneof([?KVstr("k2", "a"), ?KVfloat("k1", 1.23), ?KVlong("k1", 123), ?KVbool("k1", false), ?KVbool("k1", true)]),
    Rn1NotNum = pickoneof([?KVlong("n0", 123), ?KVstr("n1", "1.23"), ?KVbool("n1", false), ?KVbool("n1", true)]),
    Rb1NotBool = pickoneof([?KVbool("b12", true), ?KVfloat("b1", 1.23), ?KVlong("b1", 123), ?KVstr("b1", "false")]),
    Rk1String = pickoneof([?KVstr("k1", "1.23"), ?KVstr("k1", "")]),
    Rn1Num = pickoneof([?KVlong("n1", 123), ?KVfloat("n1", 2.33)]),
    Rb1Bool = pickoneof([?KVbool("b1", true), ?KVbool("b1", false)]),

    sendmsg_hds(Config, [Rk1NotString, Rn1NotNum, Rb1NotBool]),

    Mone = sendmsg_hds(Config, [pickoneof([Rk1String, Rn1Num, Rb1Bool])]),
    Mall = sendmsg_hds(Config, [Rk1String, Rn1Num, Rb1Bool]),

    check_queue_messages(Config, Qk1StringN1NumB1Bool, [Mall]),
    check_queue_messages(Config, Qk1StringOrN1NumOrB1Bool, [Mone, Mall]).


hk_nis() ->
    [
        { "k1nStringN1nNumB1nBool", [ [?HKisNotS("k1"), ?HKisNotN("n1"), ?HKisNotB("b1")] ]}
      , { "k1nStringOrN1nNumOrB1nBool", [ [?BTany, ?HKisNotS("k1"), ?HKisNotN("n1"), ?HKisNotB("b1")] ]}
    ].
hk_nis(Config) ->
    [ Qk1nStringN1nNumB1nBool, Qk1nStringOrN1nNumOrB1nBool ] = ?config(test_queues, Config),

    Rk1NotString = pickoneof([?KVfloat("k1", 1.23), ?KVlong("k1", 123), ?KVbool("k1", false), ?KVbool("k1", true)]),
    Rn1NotNum = pickoneof([?KVstr("n1", "1.23"), ?KVbool("n1", false), ?KVbool("n1", true)]),
    Rb1NotBool = pickoneof([?KVfloat("b1", 1.23), ?KVlong("b1", 123), ?KVstr("b1", "false")]),
    Rk1String = pickoneof([?KVstr("k1", "1.23"), ?KVstr("k1", "")]),
    Rn1Num = pickoneof([?KVlong("n1", 123), ?KVfloat("n1", 2.33)]),
    Rb1Bool = pickoneof([?KVbool("b1", true), ?KVbool("b1", false)]),

    sendmsg_hds(Config, [Rk1String, Rn1Num, Rb1Bool]),
    sendmsg_hds(Config, [pickoneof([Rk1String, Rn1Num, Rb1Bool])]),

    Mone = sendmsg_hds(Config, [pickoneof([Rk1NotString, Rn1NotNum, Rb1NotBool])]),
    Mnone = sendmsg_hds(Config, [Rk1NotString, Rn1NotNum, Rb1NotBool]),

    check_queue_messages(Config, Qk1nStringN1nNumB1nBool, [Mnone]),
    check_queue_messages(Config, Qk1nStringOrN1nNumOrB1nBool, [Mone, Mnone]).


hk_isnis_array() ->
    K1K2 = [?Vstr("k1"), ?Vstr("k2")],
    N1N2 = [?Vstr("n1"), ?Vstr("n2")],
    B1B2 = [?Vstr("b1"), ?Vstr("b2")],
    [
        { "k1k2n1n2b1b2", [ [?HKisSArr(K1K2), ?HKisNArr(N1N2), ?HKisBArr(B1B2) ] ]}
      , { "notK1k2n1n2b1b2", [ [?HKisNotSArr(K1K2), ?HKisNotNArr(N1N2), ?HKisNotBArr(B1B2) ] ]}
    ].
hk_isnis_array(Config) ->
    [ Qk1k2n1n2b1b2, QnotK1k2n1n2b1b2 ] = ?config(test_queues, Config),

    Rk1S = ?KVstr("k1", "23"), Rk2S = ?KVstr("k2", "0"),
    Rn1N = ?KVfloat("n1", 2.33), Rn2N = ?KVlong("n2", 42),
    Rb1B = ?KVbool("b1", true), Rb2B = ?KVbool("b2", false),
    Rk1NotS = pickoneof([?KVlong("k1", 42), ?KVbool("k1", true)]),
    Rn1NotN = pickoneof([?KVbool("n1", true), ?KVstr("n1", "0")]),
    Rb1NotB = pickoneof([?KVlong("b1", 42), ?KVstr("b1", "0")]),
    Rk2NotS = pickoneof([?KVlong("k2", 42), ?KVbool("k2", true)]),
    Rn2NotN = pickoneof([?KVbool("n2", true), ?KVstr("n2", "0")]),
    Rb2NotB = pickoneof([?KVlong("b2", 42), ?KVstr("b2", "0")]),

    sendmsg_hds(Config, [Rk1S, Rk2S, Rn1N, Rn2N, Rb1B, Rb2NotB]),
    sendmsg_hds(Config, [Rk1S, Rk2NotS, Rn1NotN, Rn2NotN, Rb1NotB, Rb2NotB]),

    Mall = sendmsg_hds(Config, [Rk1S, Rk2S, Rn1N, Rn2N, Rb1B, Rb2B]),
    Mnot = sendmsg_hds(Config, [Rk1NotS, Rk2NotS, Rn1NotN, Rn2NotN, Rb1NotB, Rb2NotB]),

    check_queue_messages(Config, Qk1k2n1n2b1b2, [Mall]),
    check_queue_messages(Config, QnotK1k2n1n2b1b2, [Mnot]).



hk_array() ->
    [
        { "hasTypeAndId", [
              [?HKexArr([?Vstr("type"), ?Vstr("id")])]
        ]}
      , { "hasTypeOrId", [
              [?BTany, ?HKexArr([?Vstr("type"), ?Vstr("id")])]
        ]}
      , { "notHasTypeNorId", [
              [?HKNexArr([?Vstr("type"), ?Vstr("id")])]
        ]}
      , { "notHasTypeOrId", [
              [?BTany, ?HKNexArr([?Vstr("type"), ?Vstr("id")])]
        ]}
    ].
hk_array(Config) ->
    [ QhasTypeAndId, QhasTypeOrId, QnotHasTypeNorId, QnotHasTypeOrId ] = ?config(test_queues, Config),

    WithAll1 = sendmsg_hds(Config, [?KVlong("type", 42), ?KVlong("id", 1337)]),
    WithAll2 = sendmsg_hds(Config, [?KVlong("id", 42), ?KVlong("type", 1337)]),
    OnlyType = sendmsg_hds(Config, [?KVlong("type", 42)]),
    OnlyId = sendmsg_hds(Config, [?KVlong("id", 1337)]),
    None1 = sendmsg_hds(Config, [?KVlong("adummy", 42 * 1337)]),
    None2 = sendmsg_hds(Config, [?KVlong("zdummy", 42 * 1337)]),

    check_queue_messages(Config, QhasTypeAndId, [WithAll1, WithAll2]),
    check_queue_messages(Config, QhasTypeOrId, [WithAll1, WithAll2, OnlyType, OnlyId]),
    check_queue_messages(Config, QnotHasTypeNorId, [None1, None2]),
    check_queue_messages(Config, QnotHasTypeOrId, [OnlyType, OnlyId, None1, None2]).


rk() ->
    ArgRk = ?KVstr("x-?rk=", "word1.word2"),
    ArgNotRk = ?KVstr("x-?rk!=", "word1.word2"),
    [
        { "rkIsWord1Word2", [ [ArgRk] ]}
      , { "rkIsWord1Word2_any", [ [?BTany, ArgRk] ]}
      , { "rkIsNotWord1Word2", [ [ArgNotRk] ]}
      , { "rkIsNotWord1Word2_any", [ [?BTany, ArgNotRk] ]}
    ].
rk(Config) ->
    [ Q1, Q2, Qnot1, Qnot2 ] = ?config(test_queues, Config),

    M1 = sendmsg_rk(Config, "word1.word2"),
    Mnot1 = sendmsg_rk(Config, "word1.word2 "),
    Mnot2 = sendmsg_rk(Config, " word1.word2"),
    Mnot3 = sendmsg_rk(Config, "Word1.word2"),
    Mnot4 = sendmsg_rk(Config, "word1#word2"),

    check_queues_messages(Config, [Q1, Q2], [M1]),
    check_queues_messages(Config, [Qnot1, Qnot2], [Mnot1, Mnot2, Mnot3, Mnot4]).


rk_array() ->
    ArgArray = [?Vstr("word1"), ?Vstr("word2")],
    [
        { "rkIsWord1OrWord2", [ [?BTany, ?Karray("x-?rk=", ArgArray)] ]}
      , { "rkIsNotWord1NorWord2", [ [?BTall, ?Karray("x-?rk!=", ArgArray)] ]}
    ].
rk_array(Config) ->
    [ Q1, Qnot1 ] = ?config(test_queues, Config),

    M1 = sendmsg_rk(Config, "word1"),
    M2 = sendmsg_rk(Config, "word2"),
    Mnot1 = sendmsg_rk(Config, "Word1"),
    Mnot2 = sendmsg_rk(Config, "wOrd2"),
    Mnot3 = sendmsg_rk(Config, "word12"),

    check_queue_messages(Config, Q1, [M1, M2]),
    check_queue_messages(Config, Qnot1, [Mnot1, Mnot2, Mnot3]).


rk_re() ->
    ArgRE = ?RKre("^(?i).*string1.*$"),
    ArgNotRE = ?RKNre("^(?i).*string1.*$"),
    [
        { "containsString1CI", [ [ArgRE] ]}
      , { "containsString1CI_any", [ [?BTany, ArgRE] ]}
      , { "notContainsString1CI", [ [ArgNotRE] ]}
      , { "notContainsString1CI_any", [ [?BTany, ArgNotRE] ]}
    ].
rk_re(Config) ->
    [ Q1, Q2, Qnot1, Qnot2 ] = ?config(test_queues, Config),

    M1 = sendmsg_rk(Config, "word1.wstring1ord2 "),
    M2 = sendmsg_rk(Config, "STRing1ord2 "),
    Mnot1 = sendmsg_rk(Config, " word1.word2"),
    Mnot2 = sendmsg_rk(Config, " word1.string2"),

    check_queues_messages(Config, [Q1, Q2], [M1, M2]),
    check_queues_messages(Config, [Qnot1, Qnot2], [Mnot1, Mnot2]).


rk_re_array() ->
    ArgREarray = ?RKreArr([?Vstr("^A"), ?Vstr("Z$")]),
    ArgNotREarray = ?RKNreArr([?Vstr("^A"), ?Vstr("Z$")]),
    [
        { "startAandEndZ", [ [?BTall, ArgREarray] ]}
      , { "startAorEndZ", [ [?BTany, ArgREarray] ]}
      , { "notStartAnorEndZ", [ [?BTall, ArgNotREarray] ]}
      , { "notStartAorEndZ", [ [?BTany, ArgNotREarray] ]}
    ].
rk_re_array(Config) ->
    [ QstartAandEndZ, QstartAorEndZ, QnotStartAnorEndZ, QnotStartAorEndZ ] = ?config(test_queues, Config),

    WBuzz = sendmsg_rk(Config, "Buzz"),
    WBuzZ = sendmsg_rk(Config, "BuzZ"),
    WAzerty = sendmsg_rk(Config, "Azerty"),
    WAzertyZ = sendmsg_rk(Config, "AzertyZ"),

    check_queue_messages(Config, QstartAandEndZ, [WAzertyZ]),
    check_queue_messages(Config, QstartAorEndZ, [WBuzZ, WAzerty, WAzertyZ]),
    check_queue_messages(Config, QnotStartAnorEndZ, [WBuzz]),
    check_queue_messages(Config, QnotStartAorEndZ, [WBuzz, WBuzZ, WAzerty]).


rk_topic_AMQP() ->
    ArgAll = ?KVstr("x-?rkta", "#"),
    ArgStartByfirstCS = ?KVstr("x-?rkta", "first.#"),
    ArgStartByfirstCI = ?KVstr("x-?rktaci", "first.#"),
    ArgEndBylastCS = ?KVstr("x-?rkta", "#.last"),
    ArgEndBylastCI = ?KVstr("x-?rktaci", "#.last"),
    ArgAny3Words = ?KVstr("x-?rkta", "*.*.*"),
    ArgAtLeast3WordsSecondIssecondCS = ?KVstr("x-?rkta", "*.second.*.#"),
    ArgAtLeast3WordsSecondIssecondCI = ?KVstr("x-?rktaci", "*.second.*.#"),
    ArgMatchSubjectAnywhere = ?KVstr("x-?rkta", "#.subject.#"),
    [
        { "all", [ [ArgAll] ]}
      , { "startByfirstCS", [ [ArgStartByfirstCS] ]}
      , { "startByfirstCI", [ [ArgStartByfirstCI] ]}
      , { "endBylastCS", [ [ArgEndBylastCS] ]}
      , { "endBylastCI", [ [ArgEndBylastCI] ]}
      , { "any3Words", [ [ArgAny3Words] ]}
      , { "atLeast3WordsSecondIssecondCS", [ [ArgAtLeast3WordsSecondIssecondCS] ]}
      , { "atLeast3WordsSecondIssecondCI", [ [ArgAtLeast3WordsSecondIssecondCI] ]}
      , { "matchSubjectAnywhere", [ [ArgMatchSubjectAnywhere] ]}
    ].
rk_topic_AMQP(Config) ->
    rk_topic_AMQP_skel(Config).

rk_topic_AMQP_skel(Config) ->
    [ Qall, QstartByfirstCS, QstartByfirstCI, QendBylastCS, QendBylastCI, Qany3Words, QatLeast3WordsSecondIssecondCS, QatLeast3WordsSecondIssecondCI, QmatchSubjectAnywhere ] = ?config(test_queues, Config),

    MvoidRK = sendmsg_rk(Config, ""),
    MDummy = sendmsg_rk(Config, "dummy dummy dummy"),
    MSubject1 = sendmsg_rk(Config, "subject"),
    MSubject2 = sendmsg_rk(Config, "subject.dummy"),
    MSubject3 = sendmsg_rk(Config, "dummy.subject"),
    MSubject4 = sendmsg_rk(Config, "dummy.subject.dummy.dummy"),
    MSubject5 = sendmsg_rk(Config, "dummy.dummy.dummy.subject"),

    Mfirst = sendmsg_rk(Config, "first"),
    MFIRSt = sendmsg_rk(Config, "FIRSt"),
    Mlast = sendmsg_rk(Config, "last"),
    MLASt = sendmsg_rk(Config, "LASt"),
    MfirstSECond = sendmsg_rk(Config, "first.SECond"),
    MFIrstsecond = sendmsg_rk(Config, "FIrst.second"),
    Mfirstsecondthird = sendmsg_rk(Config, "first.second.third"),
    MfirstSECONDw3last = sendmsg_rk(Config, "first.SECOND.w3.last"),
    MWords3 = sendmsg_rk(Config, "any1.ANY2.other"),

    check_queue_messages(Config, Qall, [
        MvoidRK, MDummy, MSubject1, MSubject2, MSubject3, MSubject4, MSubject5, Mfirst, MFIRSt, Mlast, MLASt, MfirstSECond, MFIrstsecond, Mfirstsecondthird, MfirstSECONDw3last, MWords3
        ]),
    check_queue_messages(Config, QstartByfirstCS, [
        Mfirst, MfirstSECond, Mfirstsecondthird, MfirstSECONDw3last
        ]),
    check_queue_messages(Config, QstartByfirstCI, [
        Mfirst, MFIRSt, MfirstSECond, MFIrstsecond, Mfirstsecondthird, MfirstSECONDw3last
        ]),
    check_queue_messages(Config, QendBylastCS, [
        Mlast, MfirstSECONDw3last
        ]),
    check_queue_messages(Config, QendBylastCI, [
        Mlast, MLASt, MfirstSECONDw3last
        ]),
    check_queue_messages(Config, Qany3Words, [
        Mfirstsecondthird, MWords3
        ]),
    check_queue_messages(Config, QatLeast3WordsSecondIssecondCS, [
        Mfirstsecondthird
        ]),
    check_queue_messages(Config, QatLeast3WordsSecondIssecondCI, [
        Mfirstsecondthird, MfirstSECONDw3last
        ]),
    check_queue_messages(Config, QmatchSubjectAnywhere, [
        MSubject1, MSubject2, MSubject3, MSubject4, MSubject5
        ]).


rk_topic_AMQP_any() ->
    [ { Q, [ [ ?BTany | Args ] ] } || { Q, [ Args ] } <- rk_topic_AMQP() ].
rk_topic_AMQP_any(Config) ->
    rk_topic_AMQP_skel(Config).


rk_not_topic_AMQP() ->
    [
        { "empty", [ [?KVstr("x-?rkta", "first.#"), ?KVstr("x-?rk!ta", "first.#")] ]}
      , { "emptyCi", [ [?KVstr("x-?rktaci", "first.#"), ?KVstr("x-?rk!taci", "first.#")] ]}
      , { "first3wNotEndLast", [ [?KVstr("x-?rkta", "first.*.*"), ?KVstr("x-?rk!ta", "*.*.last")] ]}
      , { "first3wNotEndLastCi", [ [?KVstr("x-?rktaci", "first.*.*"), ?KVstr("x-?rk!taci", "*.*.last")] ]}
    ].
rk_not_topic_AMQP(Config) ->
    [ Qempty, QemptyCi, Qfirst3wNotEndLast, Qfirst3wNotEndLastCi ] = ?config(test_queues, Config),

    sendmsg_rk(Config, "first"),
    sendmsg_rk(Config, "First"),
    sendmsg_rk(Config, "lasT"),
    sendmsg_rk(Config, "first.second.last"),
    sendmsg_rk(Config, "FIRST.other.Last"),

    M1 = sendmsg_rk(Config, "first.second.LAST"),
    M2 = sendmsg_rk(Config, "FIRST.second.theLast"),

    check_queues_messages(Config, [Qempty, QemptyCi], []),
    check_queue_messages(Config, Qfirst3wNotEndLast, [M1]),
    check_queue_messages(Config, Qfirst3wNotEndLastCi, [M2]).


default_order() ->
    [
        { "9999",  [ [?BOrder(9999), ?KV1s, ?StopOnT] ]}
      , { "default",   [ [?BTany, ?KV2s, ?KVstr("x-?hkv!= k3", "v3"), ?StopOnT] ]}
      , { "10001", [ [?BOrder(100001), ?StopOnT] ]}
    ].
default_order(Config) ->
    [ Q9999, Qdefault, Q10001 ] = ?config(test_queues, Config),

    MKV1s = sendmsg_hds(Config, [?KV1s]),
    MKV2s = sendmsg_hds(Config, [?KV2s]),
    MKV3s = sendmsg_hds(Config, [?KV3s]),

    check_queue_messages(Config, Q9999, [MKV1s]),
    check_queue_messages(Config, Qdefault, [MKV2s]),
    check_queue_messages(Config, Q10001, [MKV3s]).


order_goto_ontrue() ->
    [
        { "q1000", [ [?BOrder(1000), ?GotoOnT(2999)] ]}
      , { "q2000", [ [?BOrder(2000), ?GotoOnT(10000)] ]}
      , { "q3000", [ [?BOrder(3000), ?GotoOnT(10200)] ]}
      , { "q10100", [ [?BOrder(10100), ?GotoOnT(20000)] ]}
      , { "q10200", [ [?BOrder(10200), ?GotoOnT(10301)] ]}
      , { "q10300", [ [?BOrder(10300), ?GotoOnT(20000)] ]}
    ].
order_goto_ontrue(Config) ->
    [ Q1, Q2, Q3, Q4, Q5, Q6 ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, [?KVstr("m1", "")]),
    M2 = sendmsg_hds(Config, [?KVstr("m2", "")]),

    check_queues_messages(Config, [Q1, Q3, Q5], [M1, M2]),
    check_queues_messages(Config, [Q2, Q4, Q6], []).


order_goto_onfalse() ->
    [
        { "q1000", [ [?BTany, ?BOrder(1000), ?GotoOnF(10000)] ]}
      , { "q2000", [ [?BOrder(2000), ?GotoOnF(10000)] ]}
      , { "q3000", [ [?BOrder(3000), ?GotoOnF(10000)] ]}
      , { "q10100", [ [?BTany, ?BOrder(10100), ?GotoOnF(10250)] ]}
      , { "q10200", [ [?BOrder(10200), ?GotoOnF(20000)] ]}
      , { "q10300", [ [?BOrder(10300), ?GotoOnF(20000)] ]}
    ].
order_goto_onfalse(Config) ->
    [ Q1, Q2, Q3, Q4, Q5, Q6 ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, [?KVstr("m1", "")]),
    M2 = sendmsg_hds(Config, [?KVstr("m2", "")]),

    check_queues_messages(Config, [Q6], [M1, M2]),
    check_queues_messages(Config, [Q1, Q2, Q3, Q4, Q5], []).


order_stop_ontrue() ->
    [
        { "k12v12", [ [?BOrder(1200), ?KV1s, ?KV2s, ?StopOnT] ]}
      , { "k123v123", [ [?BOrder(3000), ?KV1s, ?KV2s, ?KV3s, ?StopOnT] ]}
      , { "k2v2", [ [?BOrder(12000), ?KV2s, ?StopOnT] ]}
      , { "other", [ [?BOrder(30000), ?StopOnT] ]}
      , { "empty", [ [?BOrder(130000), ?StopOnT] ]}
    ].
order_stop_ontrue(Config) ->
    [ Qk12v12, Qk123v123, Qk2v2, Qother, Qempty ] = ?config(test_queues, Config),

    MKV1s = sendmsg_hds(Config, [?KV1s]),
    MKV2s = sendmsg_hds(Config, [?KV2s]),
    MKV3s = sendmsg_hds(Config, [?KV3s]),
    MKV12s = sendmsg_hds(Config, [?KV1s, ?KV2s]),
    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),

    check_queue_messages(Config, Qk12v12, [MKV12s, MKV123s]),
    check_queue_messages(Config, Qk123v123, []),
    check_queue_messages(Config, Qk2v2, [MKV2s]),
    check_queue_messages(Config, Qother, [MKV1s, MKV3s]),
    check_queue_messages(Config, Qempty, []).


order_stop_onfalse() ->
    [
        { "k12v12", [ [?BOrder(1200), ?KV1s, ?KV2s, ?StopOnF] ]}
      , { "k123v123", [ [?BOrder(3000), ?KV1s, ?KV2s, ?KV3s, ?StopOnF] ]}
      , { "k2v2", [ [?BOrder(12000), ?KV2s, ?StopOnF] ]}
      , { "other", [ [?BOrder(30000), ?StopOnF] ]}
      , { "empty", [ [?BOrder(130000), ?StopOnF] ]}
    ].
order_stop_onfalse(Config) ->
    [ Qk12v12, Qk123v123, Qk2v2, Qother, Qempty ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KV1s]),
    sendmsg_hds(Config, [?KV2s]),
    sendmsg_hds(Config, [?KV3s]),
    MKV12s = sendmsg_hds(Config, [?KV1s, ?KV2s]),
    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),

    check_queue_messages(Config, Qk12v12, [MKV12s, MKV123s]),
    check_queue_messages(Config, Qk123v123, [MKV123s]),
    check_queue_messages(Config, Qk2v2, [MKV123s]),
    check_queue_messages(Config, Qother, [MKV123s]),
    check_queue_messages(Config, Qempty, [MKV123s]).


addq() ->
    [
        { "kv1s", [ [?KV1s, ?AddQOnT("addq:kv1s_2"), ?AddQOnF("addq:notKv1s")] ]}
      , { "kv1s_2", [  ]}
      , { "notKv1s", [  ]}
    ].
addq(Config) ->
    [ Qkv1s, Qkv1s2, QnotKv1s ] = ?config(test_queues, Config),

    MKV1s = sendmsg_hds(Config, [?KV1s]),
    MKV2s = sendmsg_hds(Config, [?KV2s]),
    MKV12s = sendmsg_hds(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qkv1s, Qkv1s2], [MKV1s, MKV12s]),
    check_queue_messages(Config, QnotKv1s, [MKV2s]).


msg_addq() ->
    [
        { "kv1s", [ [?KV1s, ?MsgAddQOnT] ]}
      , { "kv1s_2", [  ]}
    ].
msg_addq(Config) ->
    [ Qkv1s, Qkv1s2 ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, [?KV1s]),
    M2 = sendmsg_hds(Config, [?KV1s, ?AddQOnT("msg_addq:kv1s_2")]),

    check_queue_messages(Config, Qkv1s, [M1, M2]),
    check_queue_messages(Config, Qkv1s2, [M2]).


addq_array() ->
    [
        { "kv1s", [ [?KV1s, ?Karray("x-addq-ontrue", [?Vstr("addq_array:kv1s_2"), ?Vstr("addq_array:kv1s_3"), ?Vstr("addq_array:kv1s_4")]), ?Karray("x-addq-onfalse", [?Vstr("addq_array:notKv1s"), ?Vstr("addq_array:notKv1s_2"), ?Vstr("addq_array:notKv1s_3")])] ]}
      , { "kv1s_2", [  ]}
      , { "kv1s_3", [  ]}
      , { "kv1s_4", [  ]}
      , { "notKv1s", [  ]}
      , { "notKv1s_2", [  ]}
      , { "notKv1s_3", [  ]}
    ].
addq_array(Config) ->
    [ Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4, QnotKv1s, QnotKv1s2, QnotKv1s3 ] = ?config(test_queues, Config),

    MKV1s = sendmsg_hds(Config, [?KV1s]),
    MKV2s = sendmsg_hds(Config, [?KV2s]),
    MKV12s = sendmsg_hds(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4], [MKV1s, MKV12s]),
    check_queues_messages(Config, [QnotKv1s, QnotKv1s2, QnotKv1s3], [MKV2s]).


addq_re() ->
    [
        { "kv1s", [ [?KV1s, ?AddQReOnT("^addq_re:(.*)kv1s(.*)$"), ?AddQReOnF("^addq_re:(.*)Kv1(.*)$")] ]}
      , { "kv1s_2", [  ]}
      , { "kv1s_3", [  ]}
      , { "kv1s_4", [  ]}
      , { "notKv1s", [  ]}
      , { "notKv1s_2", [  ]}
      , { "notKv1s_3", [  ]}
    ].
addq_re(Config) ->
    [ Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4, QnotKv1s, QnotKv1s2, QnotKv1s3 ] = ?config(test_queues, Config),

    MKV1s = sendmsg_hds(Config, [?KV1s]),
    MKV2s = sendmsg_hds(Config, [?KV2s]),
    MKV12s = sendmsg_hds(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4], [MKV1s, MKV12s]),
    check_queues_messages(Config, [QnotKv1s, QnotKv1s2, QnotKv1s3], [MKV2s]).


msg_addq_re() ->
    [
        { "kv1s", [ [?KV1s, ?MsgAddQReOnT] ]}
      , { "kv1s_2", [  ]}
      , { "kv1s_3", [  ]}
      , { "kv1s_4", [  ]}
    ].
msg_addq_re(Config) ->
    [ Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KV2s, ?AddQReOnT("^msg_addq_re:kv1s.3$")]),
    M1 = sendmsg_hds(Config, [?KV1s]),
    M2 = sendmsg_hds(Config, [?KV1s, ?KV2s, ?AddQReOnT("^msg_addq_re:kv1s.3$")]),

    check_queue_messages(Config, Qkv1s, [M1, M2]),
    check_queue_messages(Config, Qkv1s3, [M2]),
    check_queues_messages(Config, [Qkv1s2, Qkv1s4], []).


addq_nre() ->
    [
        { "kv4s", [ [?KV4s, ?BOrder(2500), ?AddQNReOnT("^addq_nre:(.*)Kv4s(.*)$"), ?AddQNReOnF("^addq_nre:(.*)kv4s(.*)$")] ]}
      , { "kv4s_2", []}
      , { "kv4s_3", []}
      , { "notKv4s", []}
      , { "notKv4s_2", []}
      , { "notKv4s_3", []}

      , { "dummy", [ [?BOrder(2700), ?DelQNReOnT("^addq_nre:")] ]}
    ].
addq_nre(Config) ->
    [ Qkv4s, Qkv4s2, Qkv4s3, QnotKv4s, QnotKv4s2, QnotKv4s3, _Qdummy ] = ?config(test_queues, Config),

    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_hds(Config, [?KV4s]),

    check_queues_messages(Config, [Qkv4s, Qkv4s2, Qkv4s3], [MKV4s]),
    check_queues_messages(Config, [QnotKv4s, QnotKv4s2, QnotKv4s3], [MKV123s]).


msg_addq_nre() ->
    [
        { "kv1s", [ [?KV1s, ?BOrder(2300), ?MsgAddQReOnT] ]}
      , { "kv1s_2", [  ]}
      , { "kv1s_3", [  ]}
      , { "kv1s_4", [  ]}

      , { "cleaning", [ [?BOrder(2700), ?DelQNReOnT("^msg_addq_nre:")] ]}
    ].
msg_addq_nre(Config) ->
    [ Qkv1s, Qkv1s2, Qkv1s3, Qkv1s4, _ ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KV2s, ?AddQNReOnT("^msg_addq_nre:kv1s.3$")]),
    M1 = sendmsg_hds(Config, [?KV1s]),
    M2 = sendmsg_hds(Config, [?KV1s, ?KV2s, ?AddQNReOnT("^msg_addq_nre:kv1s.3$")]),

    check_queue_messages(Config, Qkv1s, [M1, M2]),
    check_queue_messages(Config, Qkv1s3, []),
    check_queues_messages(Config, [Qkv1s2, Qkv1s4], [M2]).


delq() ->
    [
        { "kv4s", [ [?BOrder(2500)] ]}
      , { "notKv4s", [ [?BOrder(2500)] ]}

      , { "dummy", [ [?BOrder(2800), ?KV4s, ?DelQOnT("delq:notKv4s"), ?DelQOnF("delq:kv4s")] ]}
    ].
delq(Config) ->
    [ Qkv4s, QnotKv4s, _Qdummy ] = ?config(test_queues, Config),

    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_hds(Config, [?KV4s]),

    check_queue_messages(Config, Qkv4s, [MKV4s]),
    check_queue_messages(Config, QnotKv4s, [MKV123s]).


delq_array() ->
    [
        { "kv4s", [ [?BOrder(2500)] ]}
      , { "kv4s_2", [ [?BOrder(2500)] ]}
      , { "kv4s_3", [ [?BOrder(2500)] ]}
      , { "notKv4s", [ [?BOrder(2500)] ]}
      , { "notKv4s_2", [ [?BOrder(2500)] ]}
      , { "notKv4s_3", [ [?BOrder(2500)] ]}

      , { "dummy", [ [?BOrder(2800), ?KV4s, ?Karray("x-delq-ontrue", [?Vstr("delq_array:notKv4s"), ?Vstr("delq_array:notKv4s_2"), ?Vstr("delq_array:notKv4s_3")]), ?Karray("x-delq-onfalse", [?Vstr("delq_array:kv4s"), ?Vstr("delq_array:kv4s_2"), ?Vstr("delq_array:kv4s_3")])] ]}
    ].
delq_array(Config) ->
    [ Qkv4s, Qkv4s2, Qkv4s3, QnotKv4s, QnotKv4s2, QnotKv4s3, _Qdummy ] = ?config(test_queues, Config),

    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_hds(Config, [?KV4s]),

    check_queues_messages(Config, [Qkv4s, Qkv4s2, Qkv4s3], [MKV4s]),
    check_queues_messages(Config, [QnotKv4s, QnotKv4s2, QnotKv4s3], [MKV123s]).


delq_re() ->
    [
        { "kv4s", [ [?BOrder(2500)] ]}
      , { "kv4s_2", [ [?BOrder(2500)] ]}
      , { "kv4s_3", [ [?BOrder(2500)] ]}
      , { "notKv4s", [ [?BOrder(2500)] ]}
      , { "notKv4s_2", [ [?BOrder(2500)] ]}
      , { "notKv4s_3", [ [?BOrder(2500)] ]}

      , { "dummy", [ [?BOrder(2800), ?KV4s, ?DelQReOnT("^delq_re:.*Kv.*$"), ?DelQReOnF("^delq_re:.*kv.*$")] ]}
    ].
delq_re(Config) ->
    [ Qkv4s, Qkv4s2, Qkv4s3, QnotKv4s, QnotKv4s2, QnotKv4s3, _Qdummy ] = ?config(test_queues, Config),

    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_hds(Config, [?KV4s]),

    check_queues_messages(Config, [Qkv4s, Qkv4s2, Qkv4s3], [MKV4s]),
    check_queues_messages(Config, [QnotKv4s, QnotKv4s2, QnotKv4s3], [MKV123s]).


delq_nre() ->
    [
        { "kv4s", [ [?BOrder(2500)] ]}
      , { "kv4s_2", [ [?BOrder(2500)] ]}
      , { "kv4s_3", [ [?BOrder(2500)] ]}
      , { "notKv4s", [ [?BOrder(2500)] ]}
      , { "notKv4s_2", [ [?BOrder(2500)] ]}
      , { "notKv4s_3", [ [?BOrder(2500)] ]}

      , { "dummy", [ [?BOrder(2800), ?KV4s, ?DelQNReOnT("^delq_nre:.*kv.*$"), ?DelQNReOnF("^delq_nre:.*Kv.*$")] ]}
    ].
delq_nre(Config) ->
    [ Qkv4s, Qkv4s2, Qkv4s3, QnotKv4s, QnotKv4s2, QnotKv4s3, _Qdummy ] = ?config(test_queues, Config),

    MKV123s = sendmsg_hds(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_hds(Config, [?KV4s]),

    check_queues_messages(Config, [Qkv4s, Qkv4s2, Qkv4s3], [MKV4s]),
    check_queues_messages(Config, [QnotKv4s, QnotKv4s2, QnotKv4s3], [MKV123s]).


self_delq() ->
    [
        { "alwaysempty", [ [?KV1s, ?DelQOnT("self_delq:alwaysempty")] ]}
      , { "alwaysempty2", [ [?KV1s, ?DelDest] ]}
    ].
self_delq(Config) ->
    [ Qalwaysempty, Qalwaysempty2 ] = ?config(test_queues, Config),

    sendmsg_hds(Config, []),
    sendmsg_hds(Config, [?KV1s]),
    sendmsg_hds(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qalwaysempty, Qalwaysempty2], []).


rk_as_queue() ->
    [
        { "q1", [  ]}, { "q2", [  ]}, { "q3", [  ]}
      , { "filter1", [ [?KV1s, ?DelDest, ?RKasQueue] ]}
      , { "filter2", [ [?KV2s, ?DelDest, ?RKasQueue, ?RKre("^rk_as_queue:q[12]$")] ]}
    ].
rk_as_queue(Config) ->
    [ Q1, Q2, Q3, _, _ ] = ?config(test_queues, Config),

    sendmsg_hds(Config, [?KV1s, ?KV2s]),
    sendmsg_hds_rk(Config, [?KV2s], "rk_as_queue:404"),
    Mk1Q1 = sendmsg_hds_rk(Config, [?KV1s], "rk_as_queue:q1"),
    Mk1Q2 = sendmsg_hds_rk(Config, [?KV1s], "rk_as_queue:q2"),
    Mk1Q3 = sendmsg_hds_rk(Config, [?KV1s], "rk_as_queue:q3"),
    Mk2Q1 = sendmsg_hds_rk(Config, [?KV2s], "rk_as_queue:q1"),
    Mk2Q2 = sendmsg_hds_rk(Config, [?KV2s], "rk_as_queue:q2"),
    _Mk2Q3 = sendmsg_hds_rk(Config, [?KV2s], "rk_as_queue:q3"),

    check_queue_messages(Config, Q1, [Mk1Q1, Mk2Q1]),
    check_queue_messages(Config, Q2, [Mk1Q2, Mk2Q2]),
    check_queue_messages(Config, Q3, [Mk1Q3]).



%% Match on properties
%% -----------------------------------------------------------------------------

pr_userid() ->
    [
        { "exUS", [ [?ATex("user_id")] ]}
      , { "nxUS", [ [?ATnx("user_id")] ]}
      , { "notGuest", [ [?KVstr("x-?pr!= user_id", "guest")] ]}
      , { "guest", [ [?KVstr("x-?pr= user_id", "guest")] ]}
    ].
pr_userid(Config) ->
    [ QexUS, QnxUS, QnotGuest, Qguest ] = ?config(test_queues, Config),

    M = sendmsg_hds(Config, [?KVstr("user_id", "bad")]),
    Mme = sendmsg_props(Config, [{me, "mess id 1"}]),
% Nothing else than current user else channel is closed by server
    Mguest = sendmsg_props(Config, [{us, "guest"}]),

    check_queue_messages(Config, QexUS, [Mguest]),
    check_queue_messages(Config, QnxUS, [M, Mme]),
% Cannot really test QnotGuest because we use guest user only
    check_queue_messages(Config, QnotGuest, []),
    check_queue_messages(Config, Qguest, [Mguest]).


pr_exnx() ->
    [
        { "exTY", [ [?ATex("type")] ]}
      , { "exAP", [ [?ATex("app_id")] ]}
      , { "exEX", [ [?ATex("expiration")] ]}
      , { "nxTY", [ [?ATnx("type")] ]}
      , { "nxAP", [ [?ATnx("app_id")] ]}
      , { "nxEX", [ [?ATnx("expiration")] ]}
    ].
pr_exnx(Config) ->
    [ QexTY, QexAP, QexEX, QnxTY, QnxAP, QnxEX ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, []),
    M2 = sendmsg_props(Config, []),
    Mall = sendmsg_props(Config, shuffle([{co, ""}, {ct, ""}, {ap, ""}, {ty, ""}, {ce, ""}, {re, ""}, {me, ""}, {cl, ""}, {de, 2}, {pr, 2}, {ti, 1}, {ex, "10000"}])),
    Mtyre = sendmsg_props(Config, [{ty, ""}, {re, ""}]),
    Mtyap = sendmsg_props(Config, [{ap, "b"}, {ty, "c"}]),
    Mde = sendmsg_props(Config, [{de, 1}]),

    check_queue_messages(Config, QexTY, [Mall, Mtyre, Mtyap]),
    check_queue_messages(Config, QexAP, [Mall, Mtyap]),
    check_queue_messages(Config, QexEX, [Mall]),
    check_queue_messages(Config, QnxTY, [M1, M2, Mde]),
    check_queue_messages(Config, QnxAP, [M1, M2, Mtyre, Mde]),
    check_queue_messages(Config, QnxEX, [M1, M2, Mtyre, Mtyap, Mde]).


pr_exnx_array() ->
    AllExProps = ?ATexArr([?Vstr("app_id"), ?Vstr("cluster_id"), ?Vstr("content_type")
          , ?Vstr("content_encoding"), ?Vstr("message_id"), ?Vstr("correlation_id"), ?Vstr("timestamp")
          , ?Vstr("reply_to"), ?Vstr("expiration"), ?Vstr("type"), ?Vstr("delivery_mode"), ?Vstr("priority")]),
    AllNxProps = ?ATnxArr([?Vstr("app_id"), ?Vstr("cluster_id"), ?Vstr("content_type")
          , ?Vstr("content_encoding"), ?Vstr("message_id"), ?Vstr("correlation_id"), ?Vstr("timestamp")
          , ?Vstr("reply_to"), ?Vstr("expiration"), ?Vstr("type"), ?Vstr("delivery_mode"), ?Vstr("priority")]),
    [
        { "exTY", [ [?ATex("type")] ]}
      , { "exAP", [ [?ATex("app_id")] ]}
      , { "exALL", [ [ AllExProps ] ] }
      , { "exANY", [ [?BTany, AllExProps ] ] }
      , { "nxTY", [ [?ATnx("type")] ]}
      , { "nxAP", [ [?ATnx("app_id")] ]}
      , { "nxALL", [ [ AllNxProps ] ] }
      , { "nxANY", [ [?BTany, AllNxProps ] ] }
    ].
pr_exnx_array(Config) ->
    [ QexTY, QexAP, QexALL, QexANY, QnxTY, QnxAP, QnxALL, QnxANY ] = ?config(test_queues, Config),

    M1 = sendmsg_hds(Config, []),
    M2 = sendmsg_props(Config, []),
    Mall = sendmsg_props(Config, shuffle([{co, ""}, {ct, ""}, {ap, ""}, {ty, ""}, {ce, ""}, {re, ""}, {me, ""}, {cl, ""}, {de, 2}, {pr, 2}, {ti, 1}, {ex, "10000"}])),
    Mtyre = sendmsg_props(Config, [{ty, ""}, {re, ""}]),
    Mtyap = sendmsg_props(Config, [{ap, "b"}, {ty, "c"}]),
    Mde = sendmsg_props(Config, [{de, 1}]),

    check_queue_messages(Config, QexTY, [Mall, Mtyre, Mtyap]),
    check_queue_messages(Config, QexAP, [Mall, Mtyap]),
    check_queue_messages(Config, QexALL, [Mall]),
    check_queue_messages(Config, QexANY, [Mall, Mtyre, Mtyap, Mde]),
    check_queue_messages(Config, QnxTY, [M1, M2, Mde]),
    check_queue_messages(Config, QnxAP, [M1, M2, Mtyre, Mde]),
    check_queue_messages(Config, QnxALL, [M1, M2]),
    check_queue_messages(Config, QnxANY, [M1, M2, Mtyre, Mtyap, Mde]).


%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

kvStr(K, V) -> {list_to_binary(K), longstr, list_to_binary(V)}.


% Don't need seeding here
shuffle(L) -> [X||{_,X} <- lists:sort([ {crypto:strong_rand_bytes(2), N} || N <- L])].
pickoneof(L) -> lists:nth(1, shuffle(L)).


%% Declare queues and bindings : 3 levels shuffling for better tests :)
%% -------------------------------------------------------------------
order_qblist(QBList) ->  order_qblist(QBList, 1, []).

order_qblist([], _, OrderedList) ->
    OrderedList;
order_qblist([ {Q, Bs} | Tail ], N, OrderedList) ->
    order_qblist(Tail, N + 1, [ {N, Q, Bs} | OrderedList]).

declare_qsbs(Config, QBList) ->
    OrderedQBList = order_qblist(QBList),
    declare_qsbs(Config, shuffle(OrderedQBList), []).

declare_qsbs(_Config, [], UnorderedQueues) ->
    [ Q || { _, Q } <- lists:sort(UnorderedQueues) ];
declare_qsbs(Config, [ { QueueOrder, QueueName, BindingsArgs } | Tail], UnorderedQueues) ->
    Q = list_to_binary(QueueName),
    declare_qbs(Config, Q, shuffle(BindingsArgs)),
    declare_qsbs(Config, Tail, [ { QueueOrder, Q } | UnorderedQueues]).

declare_qbs(_Config, _Q, []) -> ok;
declare_qbs(Config, Q, [BArgs | Tail]) ->
    Binding = #'queue.bind'{ queue       = Q
                           , exchange    = ?config(test_exchange, Config)
                           , arguments   = shuffle(BArgs)
                           },
    #'queue.bind_ok'{} = amqp_channel:call(?config(test_channel, Config), Binding),
    declare_qbs(Config, Q, Tail).


check_queues_messages(_Config, [], _) ->
    ok;
check_queues_messages(Config, [Queue | Tail], Payloads) ->
    ok = check_queue_messages(Config, Queue, Payloads),
    check_queues_messages(Config, Tail, Payloads).

check_queue_messages(Config, Queue, Payloads) ->
    check_queue_messages(Config, Queue, Payloads, first).

check_queue_messages(Config, Queue, [], LastPayload) ->
    check_queue_empty(Config, Queue, LastPayload);
check_queue_messages(Config, Queue, [Payload | Tail], LastPayload) ->
    { Tag, Content } = case amqp_channel:call(?config(test_channel, Config), #'basic.get'{queue = Queue}) of
        {#'basic.get_ok'{delivery_tag = T}, C} -> { T, C };
        {'basic.get_empty', <<>>} ->
            ct:fail("Unexpected empty queue '~s' while expecting message '~s'. Last message was '~s'", [Queue, Payload, LastPayload])
    end,
    amqp_channel:cast(?config(test_channel, Config), #'basic.ack'{delivery_tag = Tag}),
    P = Content#'amqp_msg'.payload,
    case P of
      Payload -> ok;
      Other -> ct:fail("Unexpected message '~s' while expecting message '~s' from queue '~s'. Last message was '~s'", [Other, Payload, Queue, LastPayload])
    end,
    check_queue_messages(Config, Queue, Tail, P).

check_queue_empty(Config, Queue, LastPayload) ->
    Get = #'basic.get'{queue = Queue},
    case amqp_channel:call(?config(test_channel, Config), Get) of
        {'basic.get_empty', <<>>} -> ok;
        {#'basic.get_ok'{}, Content} ->
            ct:fail("Unexpected message '~s' while expecting queue '~s' empty. Last message was '~s'", [Content#'amqp_msg'.payload, Queue, LastPayload])
    end.


%% Send message : headers are always shuffled for better tests :)
%% -------------------------------------------------------------------
sendmsg_hds(Config, Headers) ->
    Content = lists:flatten(io_lib:format("hds : ~p", [Headers])),
    Payload = list_to_binary(Content),
    sendmsg_hds_rk_pl(Config, Headers, "", Payload).

sendmsg_rk(Config, RoutingKey) ->
    Content = lists:flatten(io_lib:format("rk : ~p", [RoutingKey])),
    Payload = list_to_binary(Content),
    sendmsg_hds_rk_pl(Config, [], RoutingKey, Payload).

sendmsg_hds_rk(Config, Headers, RoutingKey) ->
    Content = lists:flatten(io_lib:format("hds : ~p~nrk : ~p", [Headers, RoutingKey])),
    Payload = list_to_binary(Content),
    sendmsg_hds_rk_pl(Config, Headers, RoutingKey, Payload).

sendmsg_hds_rk_pl(Config, Headers, RoutingKey, Payload) ->
    Publish = #'basic.publish'{exchange = ?config(test_exchange, Config), routing_key = list_to_binary(RoutingKey)},
    AMQPmsg = #'amqp_msg'{props=#'P_basic'{headers = shuffle(Headers)}, payload = Payload},
    ok = amqp_channel:cast(?config(test_channel, Config), Publish, AMQPmsg),
    true = amqp_channel:wait_for_confirms_or_die(?config(test_channel, Config), 5000),
    Payload.


sendmsg_props(Config, Props) ->
    Content = lists:flatten(io_lib:format("props : ~p", [Props])),
    Payload = list_to_binary(Content),
    sendmsg_props(Config, Props, #'P_basic'{}, Payload).

sendmsg_props(Config, [{Att, V} | Tail], MsgProps, Payload) ->
    MsgProps2 = case Att of
        ct -> MsgProps#'P_basic'{content_type = list_to_binary(V)};
        ce -> MsgProps#'P_basic'{content_encoding = list_to_binary(V)};
        co -> MsgProps#'P_basic'{correlation_id = list_to_binary(V)};
        re -> MsgProps#'P_basic'{reply_to = list_to_binary(V)};
        % Expiration is a NUMBER represented by a STRING (?); what is the story behind this ?!
        ex -> MsgProps#'P_basic'{expiration = list_to_binary(V)};
        me -> MsgProps#'P_basic'{message_id = list_to_binary(V)};
        ty -> MsgProps#'P_basic'{type = list_to_binary(V)};
        us -> MsgProps#'P_basic'{user_id = list_to_binary(V)};
        ap -> MsgProps#'P_basic'{app_id = list_to_binary(V)};
        cl -> MsgProps#'P_basic'{cluster_id = list_to_binary(V)};
        de -> MsgProps#'P_basic'{delivery_mode = V};
        pr -> MsgProps#'P_basic'{priority = V};
        ti -> MsgProps#'P_basic'{timestamp = V}
    end,
    sendmsg_props(Config, Tail, MsgProps2, Payload);
sendmsg_props(Config, [], RecProps, Payload) ->
    Publish = #'basic.publish'{exchange = ?config(test_exchange, Config)},
    AMQPmsg = #'amqp_msg'{props=RecProps, payload = Payload},
    ok = amqp_channel:cast(?config(test_channel, Config), Publish, AMQPmsg),
    true = amqp_channel:wait_for_confirms_or_die(?config(test_channel, Config), 5000),
    Payload.

