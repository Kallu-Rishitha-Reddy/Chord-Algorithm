-module(servers).
-import(fingertables,[finger_table_data/5,fingertable_data/4,fingertable_node_pass/3,fingertable_send/2]).
-import(project3,[near_node/3,next_node/2]).
-export([node_listen/1,totalHops/0]).


node_listen(NodeState) ->
    Hash = dict:fetch(id, NodeState),
    receive
            
        {lookup, Id, Key, HopsCount, _Pid} ->

                NodeVal = near_node(Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
                UpdatedState = NodeState,
                %io:format("Lookup::: ~p  For Key ~p  ClosestNode ~p ~n", [Hash, Key, NodeVal]),
                if 
                    
                    (Hash == Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    (NodeVal == Key) and (Hash =/= Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    
                    true ->
                        dict:fetch(NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, Key, HopsCount + 1, self()}
                end
                ;
        {kill} ->
            UpdatedState = NodeState,
            exit("received exit signal");
        {state, Pid} -> Pid ! NodeState,
                        UpdatedState = NodeState;
        {get_successor, Id, Pid} ->
                        FoundSeccessor = next_node(Id, NodeState),
                        UpdatedState = NodeState,
                        {Pid} ! {get_successor_reply, FoundSeccessor};

        
        {fix_fingers, FingerTable} -> 
            % io:format("Received Finger for ~p ~p", [Hash, FingerTable]),
            UpdatedState = dict:store(finger_table, FingerTable, NodeState)
    end, 
    node_listen(UpdatedState).


totalHops() ->
    receive
        {totalhops, HopsCount} ->
            HopsCount
        end.
