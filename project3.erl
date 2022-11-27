-module(project3).
-import(fingertables,[finger_table_data/5,fingertable_data/4,fingertable_node_pass/3,fingertable_send/2]).
-import(servers,[node_listen/1,totalHops/0]).
-export([start/2, start_network/2, listen_task_completion/2, node/4,get_successor_i/4,near_node/3,next_node/2,prev_node/2]).

get_m(NumNodes) ->
    trunc(math:ceil(math:log2(NumNodes)))
.


randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).


add_node_to_chord(ChordNodes, TotalNodes, M, NetworkState) ->
    RemainingHashes = lists:seq(0, TotalNodes - 1, 1) -- ChordNodes,
    Hash = lists:nth(rand:uniform(length(RemainingHashes)), RemainingHashes),
    Pid = spawn(project3, node, [Hash, M, ChordNodes, dict:new()]),
    % io:format("~n ~p ~p ~n", [Hash, Pid]),
    [Hash, dict:store(Hash, Pid, NetworkState)]
.


get_forward_distance(Key, Key, _, Distance) ->
    Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
    get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1)
.

get_closest(_, [], MinNode, _, _) ->
    MinNode;
get_closest(Key, FingerNodeIds, MinNode, MinVal, State) ->
    [First| Rest] = FingerNodeIds,
    Distance = get_forward_distance(Key, First, dict:fetch(m, State), 0),
    if
        Distance < MinVal ->
            get_closest(Key, Rest, First, Distance, State);
        true -> 
            get_closest(Key, Rest, MinNode, MinVal, State)
    end
.

near_node(Key, FingerNodeIds, State) ->
    case lists:member(Key, FingerNodeIds) of
        true -> Key;
        _ -> get_closest(Key, FingerNodeIds, -1, 10000000, State)
    end

.


is_in_range(From, To, Key, M) ->
    if 
        From < To -> 
            (From =< Key) and (Key =< To);
        trunc(From) == trunc(To) ->
            trunc(Key) == trunc(From);
        From > To ->
            ((Key >= 0) and (Key =< To)) or ((Key >= From) and (Key < trunc(math:pow(2, M))))
    end
.

closest_preceding_finger(_, NodeState, 0) -> NodeState;
closest_preceding_finger(Id, NodeState, M) -> 
    MthFinger = lists:nth(M, dict:fetch(finger_table, NodeState)),
    
    case is_in_range(dict:fetch(id, NodeState), Id, dict:fetch(node ,MthFinger), dict:fetch(m, NodeState)) of
        true -> 

            dict:fetch(pid ,MthFinger) ! {state, self()},
            receive
                {statereply, FingerNodeState} ->
                    FingerNodeState
            end,
            FingerNodeState;

        _ -> closest_preceding_finger(Id, NodeState, M - 1)
    end
.

prev_node(Id, NodeState) ->
    case 
        is_in_range(dict:fetch(id, NodeState) + 1, dict:fetch(id, dict:fetch(successor, NodeState)), Id, dict:fetch(m, NodeState)) of 
            true -> NodeState;
            _ -> prev_node(Id, closest_preceding_finger(Id, NodeState, dict:fetch(m, NodeState)))
    end
.

next_node(Id, NodeState) ->
    PredicessorNodeState = prev_node(Id, NodeState),
    dict:fetch(successor, PredicessorNodeState)
.





node(Hash, M, ChordNodes, _NodeState) -> 
    %io:format("Node is spawned with hash ~p",[Hash]),
    FingerTable = lists:duplicate(M, randomNode(Hash, ChordNodes)),
    NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, nil}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
    node_listen(NodeStateUpdated)        
.


create_nodes(ChordNodes, _, _, 0, NetworkState) -> 
    [ChordNodes, NetworkState];
create_nodes(ChordNodes, TotalNodes, M, NumNodes, NetworkState) ->
    [Hash, NewNetworkState] = add_node_to_chord(ChordNodes, TotalNodes,  M, NetworkState),
    create_nodes(lists:append(ChordNodes, [Hash]), TotalNodes, M, NumNodes - 1, NewNetworkState)
.



get_successor_i(Hash, NetworkState, I,  M) -> 
    case dict:find((Hash + I) rem trunc(math:pow(2, M)), NetworkState) of
        error ->
             get_successor_i(Hash, NetworkState, I + 1, M);
        _ -> (Hash + I) rem trunc(math:pow(2, M))
    end
.



get_node_pid(Hash, NetworkState) -> 
    case dict:find(Hash, NetworkState) of
        error -> nil;
        _ -> dict:fetch(Hash, NetworkState)
    end
.

send_message_to_node(_, [], _) ->
    ok;
send_message_to_node(Key, ChordNodes, NetworkState) ->
    [First | Rest] = ChordNodes,
    Pid = get_node_pid(First, NetworkState),
    Pid ! {lookup, First, Key, 0, self()},
    send_message_to_node(Key, Rest, NetworkState)
.

send_messages_all_nodes(_, 0, _, _) ->
    ok;
send_messages_all_nodes(ChordNodes, NumRequest, M, NetworkState) ->
    timer:sleep(1000),
    Key = lists:nth(rand:uniform(length(ChordNodes)), ChordNodes),
    send_message_to_node(Key, ChordNodes, NetworkState),
    send_messages_all_nodes(ChordNodes, NumRequest - 1, M, NetworkState)
.

kill_all_nodes([], _) ->
    ok;
kill_all_nodes(ChordNodes, NetworkState) -> 
    [First | Rest] = ChordNodes,
    get_node_pid(First, NetworkState) ! {kill},
    kill_all_nodes(Rest, NetworkState).



listen_task_completion(0, HopsCount) ->
    mainprocess ! {totalhops, HopsCount}
;

listen_task_completion(NumRequests, HopsCount) ->
    receive 
        {completed, _Pid, HopsCountForTask, _Key} ->
            % io:format("received completion from ~p, Number of Hops ~p, For Key ~p", [Pid, HopsCountForTask, Key]),
            listen_task_completion(NumRequests - 1, HopsCount + HopsCountForTask)
    end
.


send_messages_and_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState) ->
    register(taskcompletionmonitor, spawn(project3, listen_task_completion, [NumNodes * NumRequest, 0])),

    send_messages_all_nodes(ChordNodes, NumRequest, M, NetworkState),

    TotalHops = totalHops(),
    
    {ok, File} = file:open("./stats.txt", [append]),
    io:format(File, "~n Average Hops = ~p ~n", [TotalHops/(NumNodes * NumRequest)]),
    io:format("~n Average Hops = ~p ~n", [TotalHops/(NumNodes * NumRequest)]),
    kill_all_nodes(ChordNodes, NetworkState)
.

start_network(NumNodes, NumRequest) ->
    M = get_m(NumNodes),
    [ChordNodes, NetworkState] = create_nodes([], round(math:pow(2, M)), M, NumNodes, dict:new()),
    
    fingertable_send(NetworkState,M),
    send_messages_and_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState)
.


start(NumNodes, NumRequest) ->
    register(mainprocess, spawn(project3, start_network, [NumNodes, NumRequest]))
.
