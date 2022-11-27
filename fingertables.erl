-module(fingertables).
-import(project3,[get_successor_i/4]).
-export([finger_table_data/5,fingertable_data/4,fingertable_node_pass/3,fingertable_send/2]).

finger_table_data(_, _, M, M,FingerList) ->
    FingerList;
finger_table_data(Node, NetworkState, M, I, FingerList) ->
    Hash = element(1, Node),
    Ith_succesor = get_successor_i(Hash, NetworkState, trunc(math:pow(2, I)), M),
    finger_table_data(Node, NetworkState, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, NetworkState)}] )
.


fingertable_data(_, [], FTDict,_) ->
    FTDict;

fingertable_data(NetworkState, NetList, FTDict,M) ->
    [First | Rest] = NetList,
    FingerTables = finger_table_data(First, NetworkState,M, 0,[]),
    fingertable_data(NetworkState, Rest, dict:store(element(1, First), FingerTables, FTDict), M)
.



fingertable_node_pass([], _, _) ->
    ok;
fingertable_node_pass(NodesToSend, NetworkState, FingerTables) ->
    [First|Rest] = NodesToSend,
    Pid = dict:fetch(First ,NetworkState),
    Pid ! {fix_fingers, dict:from_list(dict:fetch(First, FingerTables))},
    fingertable_node_pass(Rest, NetworkState, FingerTables)
.


fingertable_send(NetworkState,M) ->
    FingerTables = fingertable_data(NetworkState, dict:to_list(NetworkState), dict:new(),M),
    % io:format("~n~p~n", [FingerTables]),
    fingertable_node_pass(dict:fetch_keys(FingerTables), NetworkState, FingerTables)
.