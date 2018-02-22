defmodule RaftKVTest do
  use ExUnit.Case

  test "register/deregister keyspaces" do
#    :ok = RaftFleet.activate("zone")
#    :ok = RaftKV.init()
#    assert RaftKV.list_keyspaces() == []
#    assert RaftFleet.consensus_groups() |> Map.keys() == [RaftKV.Keyspaces]
#
#    policy = %RaftKV.SplitMergePolicy{min_ranges: 1, max_ranges: 10, load_per_query_to_missing_key: 1}
#    :ok = RaftKV.register_keyspace(:foo, [], KV, nil, policy)
#    assert RaftKV.list_keyspaces() == [:foo]
#    assert RaftKV.register_keyspace(:foo, [], KV, nil, policy) == {:error, :already_registered}
#
#    :ok = RaftKV.deregister_keyspace(:foo)
#    assert RaftKV.list_keyspaces() == []
#    assert RaftKV.deregister_keyspace(:foo) == {:error, :no_such_keyspace}
  end
end
