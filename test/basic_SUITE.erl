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
-define(BTset0, ?KVstr("x-match", "set 0")).
-define(BTset1, ?KVstr("x-match", "set 1")).
-define(BTset2, ?KVstr("x-match", "set 2")).
-define(BTeq, ?KVstr("x-match", "eq")).

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

-define(Add1QReOnT(R), ?KVstr("x-add1qre-ontrue", R)).
-define(Add1QReOnF(R), ?KVstr("x-add1qre-onfalse", R)).
-define(Add1QNReOnT(R), ?KVstr("x-add1q!re-ontrue", R)).
-define(Add1QNReOnF(R), ?KVstr("x-add1q!re-onfalse", R)).

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
      , { binding_types, [ parallel, {repeat, 10} ], [
            bt_any, bt_all, bt_set0, bt_set1, bt_set2, bt_eq
        ]}
      , { on_headers_keys_values, [ parallel, {repeat, 10} ], [
            hkv_ltgt, hkv, hkv_array, hkv_re, hkv_re_array,
            if_hkv_all_1, if_hkv_any_1, if_hkv_set0_1, if_hkv_set1_1, if_hkv_set2_1, if_hkv_eq_1
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
        , add1q_re_t, add1q_re_f
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


%% -----------------------------------------------------------------------------
%% Test cases.
%% -----------------------------------------------------------------------------


%% ---------------------------------------------------------
%% Binding types
%% ---------------------------------------------------------

bt_common_config() ->
    [
        { "one", [ [?ATex("user_id"), ?RKre("^(?i).*sOMErk.*$"), ?KV1s, ?KV2s, ?KV3s] ]}
    ].

%% bt_any
%% -------------------------------------
bt_any() ->
    [ { Q, [ [ ?BTany | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_any(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),

    M1 = sendmsg_p(Config, [{us, "guest"}]),
    M2 = sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "", [{us, "guest"}]),
    M3 = sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    M4 = sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),
    MnoH = sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    Mall1 = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mall2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset1 = sendmsg(Config, [?KV2s], "somerk", [{us, "guest"}]),
    Mset2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset3 = sendmsg(Config, [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),
    Meq = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),

    check_queue_messages(Config, Q, [M1, M2, M3, M4, MnoH, Mall1, Mall2, Mset1, Mset2, Mset3, Meq]).


%% bt_all
%% -------------------------------------
bt_all() ->
    [ { Q, [ [ ?BTall | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_all(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),
    sendmsg_p(Config, [{us, "guest"}]),
    sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "", [{us, "guest"}]),
    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),
    sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    sendmsg(Config, [?KV2s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),

    Mall1 = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mall2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Meq = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),
    
    check_queue_messages(Config, Q, [Mall1, Mall2, Meq]).

%% bt_eq
%% -------------------------------------
bt_eq() ->
    [ { Q, [ [ ?BTeq | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_eq(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),
    sendmsg_p(Config, [{us, "guest"}]),
    sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "", [{us, "guest"}]),
    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),
    sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    sendmsg(Config, [?KV2s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "someerk", [{us, "guest"}]),
    sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", []),
    sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),

    Meq = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),
    
    check_queue_messages(Config, Q, [Meq]).

%% bt_set0
%% -------------------------------------
bt_set0() ->
    [ { Q, [ [ ?BTset0 | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_set0(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),
    sendmsg_p(Config, [{us, "guest"}]),
    sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "", [{us, "guest"}]),
    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),

    M0 = sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    M = sendmsg(Config, [?KVstr("hu", "ho")], "!!somerk is here!!", [{us, "guest"}]),
    Mall1 = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mall2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset1 = sendmsg(Config, [?KV2s], "somerk", [{us, "guest"}]),
    Mset2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset3 = sendmsg(Config, [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),
    Meq = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),

    check_queue_messages(Config, Q, [M0, M, Mall1, Mall2, Mset1, Mset2, Mset3, Meq]).

%% bt_set2
%% -------------------------------------
bt_set2() ->
    [ { Q, [ [ ?BTset2 | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_set2(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),
    sendmsg_p(Config, [{us, "guest"}]),
    sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),
    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),
    sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho")], "!!somerk is here!!", [{us, "guest"}]),
    sendmsg(Config, [?KV2s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),

    M1 = sendmsg(Config, [?KVstr("hu", "ho"), ?KVstr("z", "ho"), ?KV2s, ?KV1s], "somerk", [{us, "guest"}]),
    M2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KVstr("z", "ho"), ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),
    M3 = sendmsg(Config, [?KVstr("hu", "ho"), ?KVstr("z", "ho"), ?KV1s, ?KV3s], "somerk", [{us, "guest"}]),
    Mall1 = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mall2 = sendmsg(Config, [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Meq = sendmsg(Config, [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),

    check_queue_messages(Config, Q, [M1, M2, M3, Mall1, Mall2, Meq]).

%% bt_set
%% -------------------------------------
bt_set1() ->
    [ { Q, [ [ ?BTset1 | Args ] ] } || { Q, [ Args ] } <- bt_common_config() ].

bt_set1(Config) ->
    [ Q ] = ?config(test_queues, Config),

    sendmsg_p(Config, [{me, "mess id 1"}]),
    sendmsg_h(Config, [?KV4s, ?KVstr("k5", "v5")]),
    sendmsg_r(Config, "   none "),
    sendmsg_hr(Config, [?KV4s, ?KVstr("k5", "v5")], "   none "),
    sendmsg_p(Config, [{us, "guest"}]),
    sendmsg(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])], "", [{us, "guest"}]),
    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s])]),
    sendmsg_r(Config, pickoneof(["somerk", "  someRK!"])),
    sendmsg(Config, [], "!!somerk is here!!", [{us, "guest"}]),
    sendmsg(Config, [?KVstr("hu", "ho")], "!!somerk is here!!", [{us, "guest"}]),

    Mall1 = sendmsg2(Config, "Mall1", [?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mall2 = sendmsg2(Config, "Mall2", [?KVstr("hu", "ho"), ?KV1s, ?KV2s, ?KV3s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset1 = sendmsg2(Config, "Mset1", [?KV2s], "somerk", [{us, "guest"}]),
    Mset2 = sendmsg2(Config, "Mset2", [?KVstr("hu", "ho"), ?KV1s, ?KV4s], "somerk", [{us, "guest"}]),
    Mset3 = sendmsg2(Config, "Mset3", [?KVstr("hu", "ho"), pickoneof([?KV1s, ?KV2s, ?KV3s])], "somerk", [{us, "guest"}]),
    Meq = sendmsg2(Config, "Meq", [?KV1s, ?KV2s, ?KV3s], "somerk", [{us, "guest"}]),

    check_queue_messages(Config, Q, [Mall1, Mall2, Mset1, Mset2, Mset3, Meq]).



%% HEC
%% -----------------------------------------------------------------------------

hec_simple_binding_types() ->
    [
        { "all", [ [?BTall] ]}
      , { "any", [ [?BTany] ]}
    ].
hec_simple_binding_types(Config) ->
    [ Qall, Qany ] = ?config(test_queues, Config),

    M1 = sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    M2 = sendmsg_h(Config, []),
    M3 = sendmsg_r(Config, "word1.word2 "),

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

    sendmsg_h(Config, [pickoneof([?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_h(Config, [?KV1s, pickoneof([?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_h(Config, [?KV2s, pickoneof([?KV3s, ?KV4s, ?KVstr("k5", "v5")])]),
    sendmsg_h(Config, [?KV3s, ?KVstr("k5", "v5")]),
    sendmsg_h(Config, pickoneof([[?KVstr("v4", "k4"), ?KVstr("v3", "k3")], [?KVstr("v4", "k4"), ?KVstr("v5", "k5")]])),

    Notv1 = pickoneof(["k1", "v0", "v1 ", " v1", "v2"]),
    Notv2 = pickoneof(["k2", "v1", "v2 ", " v2", "v3"]),
    Notv3 = pickoneof(["k3", "v2", "v3 ", " v3", "v4"]),
    Notv4 = pickoneof(["k4", "v3", "v4 ", " v4", "v5"]),
    Notv5 = pickoneof(["k5", "v4", "v5 ", " v5", "v6"]),
    sendmsg_h(Config, [kvStr("k1", Notv1), kvStr("k2", Notv2), kvStr("k3", Notv3), kvStr("k4", Notv4), kvStr("k5", Notv5)]),
    Notk1 = pickoneof([" k1", "k1 "]),
    Notk2 = pickoneof([" k2", "k2 "]),
    Notk3 = pickoneof([" k3", "k3 "]),
    Notk4 = pickoneof([" k4", "k4 "]),
    Notk5 = pickoneof([" k5", "k5 "]),
    sendmsg_h(Config, [kvStr(Notk1, "v1"), kvStr(Notk2, "v2"), kvStr(Notk3, "v3"), kvStr(Notk4, "v4"), kvStr(Notk5, "v5")]),

    P123 = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    P1234 = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s]),
    P12345 = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")]),
    P2345 = sendmsg_h(Config, [?KV2s, ?KV3s, ?KV4s, ?KVstr("k5", "v5")]),
    P34 = sendmsg_h(Config, [?KV3s, ?KV4s]),

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

    sendmsg_h(Config, [?KV1s]),
    sendmsg_h(Config, [?KVbool("b1", true)]),
    sendmsg_h(Config, [?KVbool("b1", false), ?KV1s]),
    sendmsg_h(Config, [?KVbool(" b1", true), ?KV1s]),
    sendmsg_h(Config, [?KVbool("b1 ", true), ?KV1s]),
    sendmsg_h(Config, [?KVstr("b1", "true"), ?KVstr("k1", "36")]),
    sendmsg_h(Config, [?KVbool("b1", true), ?KVfloat("k1", 36.01)]),

    P1 = sendmsg_h(Config, [?KVbool("b1", true), ?KVlong("k1", 36)]),
    P2 = sendmsg_h(Config, [?KVbool("b1", true), ?KVfloat("k1", 36)]),
    P3 = sendmsg_h(Config, [?KVbool("b1", true), ?KVfloat("k1", 36.0)]),
    P4 = sendmsg_h(Config, [?KVbool("b1", true), ?KVstr("k1", "36")]),
    P5 = sendmsg_h(Config, [?KVstr("b1", "true"), ?KV1s]),
    P6 = sendmsg_h(Config, [?KVfloat("b1", 1.00), ?KVlong("k1", 36)]),
    P7 = sendmsg_h(Config, [?KVlong("b1", 1), ?KVfloat("k1", 36.00)]),
    P8 = sendmsg_h(Config, [?KVfloat("b1", 1.00), ?KVfloat("k1", 36.0)]),
    P9 = sendmsg_h(Config, [?KVlong("b1", 1), ?KVlong("k1", 36)]),

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

    sendmsg_h(Config, [?KVstr(" k1", "v1")]),
    sendmsg_h(Config, [?KVstr("k1 ", "v1")]),

    P1 = sendmsg_h(Config, [?KV1s]),
    P2 = sendmsg_h(Config, [?KV2s]),
    P3 = sendmsg_h(Config, [?KV3s]),
    P4 = sendmsg_h(Config, [?KV4s]),
    P5 = sendmsg_h(Config, [?KV2s, ?KV1s]),
    P6 = sendmsg_h(Config, [?KV1s, ?KV2s]),
    P7 = sendmsg_h(Config, [?KV4s, ?KV3s]),
    P8 = sendmsg_h(Config, [?KV3s, ?KV4s]),
    P9 = sendmsg_h(Config, [?KVstr("k5", "v5")]),

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

    sendmsg_h(Config, [?KVfloat("k1", 36.001)]),
    sendmsg_h(Config, [?KVbool("b1", false)]),

    P1 = sendmsg_h(Config, [?KV1s]),
    P2 = sendmsg_h(Config, [?KVstr("k1", "36")]),
    P3 = sendmsg_h(Config, [?KVfloat("k1", 36.0)]),
    P4 = sendmsg_h(Config, [?KVlong("k1", 36)]),
    P5 = sendmsg_h(Config, [?KVbool("b1", true)]),
    P6 = sendmsg_h(Config, [?KVfloat("b1", 1.00), ?KVlong("k1", 36)]),
    P7 = sendmsg_h(Config, [?KVlong("b1", 1), ?KVfloat("k1", 36.00)]),
    P8 = sendmsg_h(Config, [?KVfloat("b1", 1.00), ?KVfloat("k1", 36.0)]),
    P9 = sendmsg_h(Config, [?KVlong("b1", 1), ?KVlong("k1", 36)]),

    check_queue_messages(Config, Q1, [P1, P3, P4, P5, P6, P7, P8, P9]),
    check_queue_messages(Config, Q2, [P1, P2, P3, P4, P5, P6, P7, P8, P9]).


% If HKV
cc_if_hkv_config() ->
    [
        { "k1k2", [ [?KV1s, ?KV2s] ]}
      , { "k1ifk2", [ [?KV1s, ?KVstr("x-?hk?v= k2", "v2")] ]}
      , { "ifk1k2", [ [?KVstr("x-?hk?v= k1", "v1"), ?KV2s] ]}
      , { "ifk1ifk2", [ [?KVstr("x-?hk?v= k1", "v1"), ?KVstr("x-?hk?v= k2", "v2")] ]}
    ].

cc_if_hkv_send(Config) ->
    Mk1 = sendmsg_h(Config, [?KV1s]),
    Mk2 = sendmsg_h(Config, [?KV2s]),
    Mk3 = sendmsg_h(Config, [?KV3s]),
    Mk1k2 = sendmsg_h(Config, [?KV1s, ?KV2s]),
    Mk1k3 = sendmsg_h(Config, [?KV1s, ?KV3s]),
    Mk1k2k3 = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    Mk1k2bad = sendmsg_h(Config, [?KV1s, ?KVstr("k2", "v3")]),
    Mk1badk2 = sendmsg_h(Config, [?KVstr("k1", "v0"), ?KV2s]),
    Mk1badk2bad = sendmsg_h(Config, [?KVstr("k1", "v2"), ?KVstr("k2", "v1")]),
    [Mk1, Mk2, Mk3, Mk1k2, Mk1k3, Mk1k2k3, Mk1k2bad, Mk1badk2, Mk1badk2bad].

if_hkv_all_1() ->
    [ { Q, [ [ ?BTall | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_all_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [Mk1, Mk2, Mk3, Mk1k2, Mk1k3, Mk1k2k3, _, _, _] = cc_if_hkv_send(Config),

    check_queue_messages(Config, Qk1k2, [Mk1k2, Mk1k2k3]),
    check_queue_messages(Config, Qk1ifk2, [Mk1, Mk1k2, Mk1k3, Mk1k2k3]),
    check_queue_messages(Config, Qifk1k2, [Mk2, Mk1k2, Mk1k2k3]),
    check_queue_messages(Config, Qifk1ifk2, [Mk1, Mk2, Mk3, Mk1k2, Mk1k3, Mk1k2k3]).


if_hkv_eq_1() ->
    [ { Q, [ [ ?BTeq | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_eq_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [Mk1, Mk2, _, Mk1k2, _, _, _, _, _] = cc_if_hkv_send(Config),

    check_queue_messages(Config, Qk1k2, [Mk1k2]),
    check_queue_messages(Config, Qk1ifk2, [Mk1, Mk1k2]),
    check_queue_messages(Config, Qifk1k2, [Mk2, Mk1k2]),
    check_queue_messages(Config, Qifk1ifk2, [Mk1, Mk2, Mk1k2]).


if_hkv_set0_1() ->
    [ { Q, [ [ ?BTset0 | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_set0_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [Mk1, Mk2, Mk3, Mk1k2, Mk1k3, Mk1k2k3, _, _, _] = cc_if_hkv_send(Config),

    % For set, messages are always the same
    SameMessages = [Mk1, Mk2, Mk3, Mk1k2, Mk1k3, Mk1k2k3],
    check_queues_messages(Config, [Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2], SameMessages).

if_hkv_set1_1() ->
    [ { Q, [ [ ?BTset1 | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_set1_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [Mk1, Mk2, _, Mk1k2, Mk1k3, Mk1k2k3, _, _, _] = cc_if_hkv_send(Config),

    % For set, messages are always the same
    SameMessages = [Mk1, Mk2, Mk1k2, Mk1k3, Mk1k2k3],
    check_queues_messages(Config, [Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2], SameMessages).

if_hkv_set2_1() ->
    [ { Q, [ [ ?BTset2 | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_set2_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [_, _, _, Mk1k2, _, Mk1k2k3, _, _, _] = cc_if_hkv_send(Config),

    % For set, messages are always the same
    SameMessages = [Mk1k2, Mk1k2k3],
    check_queues_messages(Config, [Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2], SameMessages).


if_hkv_any_1() ->
    [ { Q, [ [ ?BTany | Args ] ] } || { Q, [ Args ] } <- cc_if_hkv_config() ].
if_hkv_any_1(Config) ->
    [ Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2 ] = ?config(test_queues, Config),

    [Mk1, Mk2, _, Mk1k2, Mk1k3, Mk1k2k3, Mk1k2bad, Mk1badk2, _] = cc_if_hkv_send(Config),

    % For any, messages are always the same
    SameMessages = [Mk1, Mk2, Mk1k2, Mk1k3, Mk1k2k3, Mk1k2bad, Mk1badk2],
    check_queues_messages(Config, [Qk1k2, Qk1ifk2, Qifk1k2, Qifk1ifk2], SameMessages).


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
    sendmsg_h(Config, [?KVstr("temp", "1")]),
    sendmsg_h(Config, [?KVstr("temp", "-1")]),
    sendmsg_h(Config, [?KVbool("temp", true)]),
    sendmsg_h(Config, [?KVbool("temp", false)]),

    HotTemp1 = sendmsg_h(Config, [?KVlong("temp", 36), ?KVstr("k3", "dummy")]),
    HotTemp2 = sendmsg_h(Config, [?KVfloat("temp", 30.001)]),
    NormalTemp1 = sendmsg_h(Config, [?KVfloat("temp", 30.000)]),
    NormalTemp2 = sendmsg_h(Config, [?KVlong("temp", 30)]),
    ColdTemp1 = sendmsg_h(Config, [?KVfloat("temp", -30.000)]),
    ColdTemp2 = sendmsg_h(Config, [?KVfloat("temp", -5.0001)]),

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

    sendmsg_h(Config, [?KVstr("xnum", "30")]),
    sendmsg_h(Config, [?KVbool("xnum", true)]),

    BoolT = sendmsg_h(Config, [?KVbool("x-num", true)]),
    BoolF = sendmsg_h(Config, [?KVbool("x-num", false)]),
    Num30 = sendmsg_h(Config, [?KVfloat("x-num", 30.0)]),
    Str30 = sendmsg_h(Config, [?KVstr("x-num", "30")]),

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

    sendmsg_h(Config, [?KVbool("xnum", true)]),

    BoolT = sendmsg_h(Config, [?KVbool("x-num", true)]),
    BoolF = sendmsg_h(Config, [?KVbool("x-num", false)]),
    Num30 = sendmsg_h(Config, [?KVfloat("x-num", 30.0)]),
    Str30 = sendmsg_h(Config, [?KVstr("x-num", "30")]),

    Num3 = sendmsg_h(Config, [?KVlong("x-num", 3)]),
    Str30Dot = sendmsg_h(Config, [?KVstr("x-num", "30.")]),

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
    sendmsg_h(Config, [?KVlong("doc_type", 1)]),
    sendmsg_h(Config, [?KVfloat("doc_type", 1.1)]),
    sendmsg_h(Config, [?KVbool("doc_type", true)]),
    sendmsg_h(Config, [?KVbool("doc_type", false)]),

    Pu1 = sendmsg_h(Config, [?KVstr("doc_type", "pUbLiC")]),
    Pu2 = sendmsg_h(Config, [?KVstr("doc_type", "PUb")]),
    Pr1 = sendmsg_h(Config, [?KVstr("doc_type", "PriVAte")]),
    Pr2 = sendmsg_h(Config, [?KVstr("doc_type", "pRIv")]),
    Ot1 = sendmsg_h(Config, [?KVstr("doc_type", "PriVA")]),
    Ot2 = sendmsg_h(Config, [?KVstr("doc_type", "pu")]),

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

    sendmsg_h(Config, [?KVlong("dummy", 331)]),

    AxZ = sendmsg_h(Config, [?KVstr("word", "AxZ")]),
    BxZ = sendmsg_h(Config, [?KVstr("word", "BxZ")]),
    AxY = sendmsg_h(Config, [?KVstr("word", "AxY")]),
    BxY = sendmsg_h(Config, [?KVstr("word", "BxY")]),

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

    HasType1 = sendmsg_h(Config, [?KVlong("dummy", 1), ?KVbool("type", true)]),
    HasType2 = sendmsg_h(Config, [?KVlong("type", 1), ?KVbool("dummy", true)]),
    HasId1 = sendmsg_h(Config, [?KVlong("id", 1), ?KVbool("adummy", true)]),
    HasId2 = sendmsg_h(Config, [?KVlong("id", 1), ?KVbool("dummy", true)]),
    HasBoth1 = sendmsg_h(Config, [?KVlong("id", 1), ?KVbool("type", true)]),
    HasBoth2 = sendmsg_h(Config, [?KVlong("type", 1), ?KVbool("id", true)]),

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

    sendmsg_h(Config, [Rk1NotString, Rn1NotNum, Rb1NotBool]),

    Mone = sendmsg_h(Config, [pickoneof([Rk1String, Rn1Num, Rb1Bool])]),
    Mall = sendmsg_h(Config, [Rk1String, Rn1Num, Rb1Bool]),

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

    sendmsg_h(Config, [Rk1String, Rn1Num, Rb1Bool]),
    sendmsg_h(Config, [pickoneof([Rk1String, Rn1Num, Rb1Bool])]),

    Mone = sendmsg_h(Config, [pickoneof([Rk1NotString, Rn1NotNum, Rb1NotBool])]),
    Mnone = sendmsg_h(Config, [Rk1NotString, Rn1NotNum, Rb1NotBool]),

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

    sendmsg_h(Config, [Rk1S, Rk2S, Rn1N, Rn2N, Rb1B, Rb2NotB]),
    sendmsg_h(Config, [Rk1S, Rk2NotS, Rn1NotN, Rn2NotN, Rb1NotB, Rb2NotB]),

    Mall = sendmsg_h(Config, [Rk1S, Rk2S, Rn1N, Rn2N, Rb1B, Rb2B]),
    Mnot = sendmsg_h(Config, [Rk1NotS, Rk2NotS, Rn1NotN, Rn2NotN, Rb1NotB, Rb2NotB]),

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

    WithAll1 = sendmsg_h(Config, [?KVlong("type", 42), ?KVlong("id", 1337)]),
    WithAll2 = sendmsg_h(Config, [?KVlong("id", 42), ?KVlong("type", 1337)]),
    OnlyType = sendmsg_h(Config, [?KVlong("type", 42)]),
    OnlyId = sendmsg_h(Config, [?KVlong("id", 1337)]),
    None1 = sendmsg_h(Config, [?KVlong("adummy", 42 * 1337)]),
    None2 = sendmsg_h(Config, [?KVlong("zdummy", 42 * 1337)]),

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

    M1 = sendmsg_r(Config, "word1.word2"),
    Mnot1 = sendmsg_r(Config, "word1.word2 "),
    Mnot2 = sendmsg_r(Config, " word1.word2"),
    Mnot3 = sendmsg_r(Config, "Word1.word2"),
    Mnot4 = sendmsg_r(Config, "word1#word2"),

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

    M1 = sendmsg_r(Config, "word1"),
    M2 = sendmsg_r(Config, "word2"),
    Mnot1 = sendmsg_r(Config, "Word1"),
    Mnot2 = sendmsg_r(Config, "wOrd2"),
    Mnot3 = sendmsg_r(Config, "word12"),

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

    M1 = sendmsg_r(Config, "word1.wstring1ord2 "),
    M2 = sendmsg_r(Config, "STRing1ord2 "),
    Mnot1 = sendmsg_r(Config, " word1.word2"),
    Mnot2 = sendmsg_r(Config, " word1.string2"),

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

    WBuzz = sendmsg_r(Config, "Buzz"),
    WBuzZ = sendmsg_r(Config, "BuzZ"),
    WAzerty = sendmsg_r(Config, "Azerty"),
    WAzertyZ = sendmsg_r(Config, "AzertyZ"),

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

    MvoidRK = sendmsg_r(Config, ""),
    MDummy = sendmsg_r(Config, "dummy dummy dummy"),
    MSubject1 = sendmsg_r(Config, "subject"),
    MSubject2 = sendmsg_r(Config, "subject.dummy"),
    MSubject3 = sendmsg_r(Config, "dummy.subject"),
    MSubject4 = sendmsg_r(Config, "dummy.subject.dummy.dummy"),
    MSubject5 = sendmsg_r(Config, "dummy.dummy.dummy.subject"),

    Mfirst = sendmsg_r(Config, "first"),
    MFIRSt = sendmsg_r(Config, "FIRSt"),
    Mlast = sendmsg_r(Config, "last"),
    MLASt = sendmsg_r(Config, "LASt"),
    MfirstSECond = sendmsg_r(Config, "first.SECond"),
    MFIrstsecond = sendmsg_r(Config, "FIrst.second"),
    Mfirstsecondthird = sendmsg_r(Config, "first.second.third"),
    MfirstSECONDw3last = sendmsg_r(Config, "first.SECOND.w3.last"),
    MWords3 = sendmsg_r(Config, "any1.ANY2.other"),

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

    sendmsg_r(Config, "first"),
    sendmsg_r(Config, "First"),
    sendmsg_r(Config, "lasT"),
    sendmsg_r(Config, "first.second.last"),
    sendmsg_r(Config, "FIRST.other.Last"),

    M1 = sendmsg_r(Config, "first.second.LAST"),
    M2 = sendmsg_r(Config, "FIRST.second.theLast"),

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

    MKV1s = sendmsg_h(Config, [?KV1s]),
    MKV2s = sendmsg_h(Config, [?KV2s]),
    MKV3s = sendmsg_h(Config, [?KV3s]),

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

    M1 = sendmsg_h(Config, [?KVstr("m1", "")]),
    M2 = sendmsg_h(Config, [?KVstr("m2", "")]),

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

    M1 = sendmsg_h(Config, [?KVstr("m1", "")]),
    M2 = sendmsg_h(Config, [?KVstr("m2", "")]),

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

    MKV1s = sendmsg_h(Config, [?KV1s]),
    MKV2s = sendmsg_h(Config, [?KV2s]),
    MKV3s = sendmsg_h(Config, [?KV3s]),
    MKV12s = sendmsg_h(Config, [?KV1s, ?KV2s]),
    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),

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

    sendmsg_h(Config, [?KV1s]),
    sendmsg_h(Config, [?KV2s]),
    sendmsg_h(Config, [?KV3s]),
    MKV12s = sendmsg_h(Config, [?KV1s, ?KV2s]),
    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),

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

    MKV1s = sendmsg_h(Config, [?KV1s]),
    MKV2s = sendmsg_h(Config, [?KV2s]),
    MKV12s = sendmsg_h(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qkv1s, Qkv1s2], [MKV1s, MKV12s]),
    check_queue_messages(Config, QnotKv1s, [MKV2s]).


msg_addq() ->
    [
        { "kv1s", [ [?KV1s, ?MsgAddQOnT] ]}
      , { "kv1s_2", [  ]}
    ].
msg_addq(Config) ->
    [ Qkv1s, Qkv1s2 ] = ?config(test_queues, Config),

    M1 = sendmsg_h(Config, [?KV1s]),
    M2 = sendmsg_h(Config, [?KV1s, ?AddQOnT("msg_addq:kv1s_2")]),

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

    MKV1s = sendmsg_h(Config, [?KV1s]),
    MKV2s = sendmsg_h(Config, [?KV2s]),
    MKV12s = sendmsg_h(Config, [?KV1s, ?KV2s]),

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

    MKV1s = sendmsg_h(Config, [?KV1s]),
    MKV2s = sendmsg_h(Config, [?KV2s]),
    MKV12s = sendmsg_h(Config, [?KV1s, ?KV2s]),

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

    sendmsg_h(Config, [?KV2s, ?AddQReOnT("^msg_addq_re:kv1s.3$")]),
    M1 = sendmsg_h(Config, [?KV1s]),
    M2 = sendmsg_h(Config, [?KV1s, ?KV2s, ?AddQReOnT("^msg_addq_re:kv1s.3$")]),

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

    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_h(Config, [?KV4s]),

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

    sendmsg_h(Config, [?KV2s, ?AddQNReOnT("^msg_addq_nre:kv1s.3$")]),
    M1 = sendmsg_h(Config, [?KV1s]),
    M2 = sendmsg_h(Config, [?KV1s, ?KV2s, ?AddQNReOnT("^msg_addq_nre:kv1s.3$")]),

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

    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_h(Config, [?KV4s]),

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

    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_h(Config, [?KV4s]),

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

    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_h(Config, [?KV4s]),

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

    MKV123s = sendmsg_h(Config, [?KV1s, ?KV2s, ?KV3s]),
    MKV4s = sendmsg_h(Config, [?KV4s]),

    check_queues_messages(Config, [Qkv4s, Qkv4s2, Qkv4s3], [MKV4s]),
    check_queues_messages(Config, [QnotKv4s, QnotKv4s2, QnotKv4s3], [MKV123s]).


self_delq() ->
    [
        { "alwaysempty", [ [?KV1s, ?DelQOnT("self_delq:alwaysempty")] ]}
      , { "alwaysempty2", [ [?KV1s, ?DelDest] ]}
    ].
self_delq(Config) ->
    [ Qalwaysempty, Qalwaysempty2 ] = ?config(test_queues, Config),

    sendmsg_h(Config, []),
    sendmsg_h(Config, [?KV1s]),
    sendmsg_h(Config, [?KV1s, ?KV2s]),

    check_queues_messages(Config, [Qalwaysempty, Qalwaysempty2], []).


add1q_re_t() ->
    [
        { "q1", [  ]}, { "q2", [  ]}, { "q3", [  ]}
      , { "q10", [  ]}, { "q20", [  ]}, { "q30", [  ]}
      , { "devnull", [ [?DelDest, ?KV1s, ?Add1QReOnT("^add1q_re_t:q3")] ]}
    ].
add1q_re_t(Config) ->
    [ Q1, Q2, Q3, Q10, Q20, Q30, _] = ?config(test_queues, Config),

    M1 = sendmsg_h(Config, [?KV1s]),
    _ = [ begin sendmsg_h(Config, [?KV1s]), 0 end || _ <- lists:seq(1, 100)],

    check_queues_messages(Config, [Q3, Q30], [M1, M1, M1, M1, M1, M1, M1, M1, []]),
    check_queues_empty(Config, [Q1, Q2, Q10, Q20]).


add1q_re_f() ->
    [
        { "q1", [  ]}, { "q2", [  ]}, { "q3", [  ]}
      , { "q10", [  ]}, { "q20", [  ]}, { "q30", [  ]}
      , { "devnull", [ [?DelDest, ?KV1s, ?Add1QReOnF("^add1q_re_f:q2")] ]}
    ].
add1q_re_f(Config) ->
    [ Q1, Q2, Q3, Q10, Q20, Q30, _] = ?config(test_queues, Config),

    M2 = sendmsg_h(Config, [?KV2s]),
    _ = [ begin sendmsg_h(Config, [?KV2s]), 0 end || _ <- lists:seq(1, 100)],

    check_queues_messages(Config, [Q2, Q20], [M2, M2, M2, M2, M2, M2, M2, M2, []]),
    check_queues_empty(Config, [Q1, Q3, Q10, Q30]).




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

    M = sendmsg_h(Config, [?KVstr("user_id", "bad")]),
    Mme = sendmsg_p(Config, [{me, "mess id 1"}]),
% Nothing else than current user else channel is closed by server
    Mguest = sendmsg_p(Config, [{us, "guest"}]),

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

    M1 = sendmsg_h(Config, []),
    M2 = sendmsg_p(Config, []),
    Mall = sendmsg_p(Config, shuffle([{co, ""}, {ct, ""}, {ap, ""}, {ty, ""}, {ce, ""}, {re, ""}, {me, ""}, {cl, ""}, {de, 2}, {pr, 2}, {ti, 1}, {ex, "10000"}])),
    Mtyre = sendmsg_p(Config, [{ty, ""}, {re, ""}]),
    Mtyap = sendmsg_p(Config, [{ap, "b"}, {ty, "c"}]),
    Mde = sendmsg_p(Config, [{de, 1}]),

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

    M1 = sendmsg_h(Config, []),
    M2 = sendmsg_p(Config, []),
    Mall = sendmsg_p(Config, shuffle([{co, ""}, {ct, ""}, {ap, ""}, {ty, ""}, {ce, ""}, {re, ""}, {me, ""}, {cl, ""}, {de, 2}, {pr, 2}, {ti, 1}, {ex, "10000"}])),
    Mtyre = sendmsg_p(Config, [{ty, ""}, {re, ""}]),
    Mtyap = sendmsg_p(Config, [{ap, "b"}, {ty, "c"}]),
    Mde = sendmsg_p(Config, [{de, 1}]),

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
%% Instead of leaving queue with some messages, purge it so tests may be factorized (see add1q..)
% We may also implement test to check that queue have only n times the same message
check_queue_messages(_, _, [ [] ], _) ->
    ok;
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


check_queues_empty(_Config, []) ->
    ok;
check_queues_empty(Config, [Queue | Tail]) ->
    ok = check_queue_empty(Config, Queue, abnormal),
    check_queues_empty(Config, Tail).


check_queue_empty(Config, Queue, LastPayload) ->
    Get = #'basic.get'{queue = Queue},
    case amqp_channel:call(?config(test_channel, Config), Get) of
        {'basic.get_empty', <<>>} -> ok;
        {#'basic.get_ok'{}, Content} ->
            ct:fail("Unexpected non empty queue '~s', got message '~s'. Last message was '~s'", [Queue, Content#'amqp_msg'.payload, LastPayload])
    end.


%% Send message : headers are always shuffled for better tests :)
%% -------------------------------------------------------------------

sendmsg_h(Config, Headers) ->
    sendmsg(Config, Headers, "", []).

sendmsg_hr(Config, Headers, RoutingKey) ->
    sendmsg(Config, Headers, RoutingKey, []).

sendmsg_r(Config, RoutingKey) ->
    sendmsg(Config, [], RoutingKey, []).

sendmsg_p(Config, Props) ->
    sendmsg(Config, [], "", Props).

sendmsg(Config, Headers, RoutingKey, Props) ->
    Content = io_lib:format("h : ~4096p r : ~4096p p : ~4096p", [Headers, RoutingKey, Props]),
    Payload = list_to_binary(lists:flatten(Content)),

    MsgProps = #'P_basic'{headers = shuffle(Headers)},
    MsgProps2 = msg_add_props(Props, MsgProps),

    AMQPmsg = #'amqp_msg'{props = MsgProps2, payload = Payload},

    Publish = #'basic.publish'{exchange = ?config(test_exchange, Config), routing_key = list_to_binary(RoutingKey)},

    ok = amqp_channel:cast(?config(test_channel, Config), Publish, AMQPmsg),
    true = amqp_channel:wait_for_confirms_or_die(?config(test_channel, Config), 5000),
    Payload.


sendmsg2_h(Config, Content, Headers) ->
    sendmsg2(Config, Content, Headers, "", []).

sendmsg2_hr(Config, Content, Headers, RoutingKey) ->
    sendmsg2(Config, Content, Headers, RoutingKey, []).

sendmsg2_r(Config, Content, RoutingKey) ->
    sendmsg2(Config, Content, [], RoutingKey, []).

sendmsg2_p(Config, Content, Props) ->
    sendmsg2(Config, Content, [], "", Props).

sendmsg2(Config, Content, Headers, RoutingKey, Props) ->
    Payload = list_to_binary(Content),

    MsgProps = #'P_basic'{headers = shuffle(Headers)},
    MsgProps2 = msg_add_props(Props, MsgProps),

    AMQPmsg = #'amqp_msg'{props = MsgProps2, payload = Payload},

    Publish = #'basic.publish'{exchange = ?config(test_exchange, Config), routing_key = list_to_binary(RoutingKey)},

    ok = amqp_channel:cast(?config(test_channel, Config), Publish, AMQPmsg),
    true = amqp_channel:wait_for_confirms_or_die(?config(test_channel, Config), 5000),
    Payload.
    

msg_add_props([], Msg) -> Msg;
msg_add_props([{Att, V} | Tail], Msg) ->
    Msg2 = case Att of
        ct -> Msg#'P_basic'{content_type = list_to_binary(V)};
        ce -> Msg#'P_basic'{content_encoding = list_to_binary(V)};
        co -> Msg#'P_basic'{correlation_id = list_to_binary(V)};
        re -> Msg#'P_basic'{reply_to = list_to_binary(V)};
        % Expiration is a NUMBER represented by a STRING (?); what is the story behind this ?!
        ex -> Msg#'P_basic'{expiration = list_to_binary(V)};
        me -> Msg#'P_basic'{message_id = list_to_binary(V)};
        ty -> Msg#'P_basic'{type = list_to_binary(V)};
        us -> Msg#'P_basic'{user_id = list_to_binary(V)};
        ap -> Msg#'P_basic'{app_id = list_to_binary(V)};
        cl -> Msg#'P_basic'{cluster_id = list_to_binary(V)};
        de -> Msg#'P_basic'{delivery_mode = V};
        pr -> Msg#'P_basic'{priority = V};
        ti -> Msg#'P_basic'{timestamp = V}
    end,
    msg_add_props(Tail, Msg2).

