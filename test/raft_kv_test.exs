defmodule RaftKVTest do
  use ExUnit.Case
  @moduletag timeout: 200_000

  @n_keys 1000
  @ks_name :kv
  @policy1 %RaftKV.SplitMergePolicy{min_shards:                    4,
                                    max_shards:                    8,
                                    merge_threshold_ratio:         0.5,
                                    load_per_query_to_missing_key: 1}
  @policy2 Map.put(@policy1, :max_size_per_shard, 50)

  defp with_clients(f) do
    pids = Enum.map(0 .. (@n_keys - 1), fn i -> spawn_link(fn -> client_loop(i) end) end)
    f.()
    Enum.each(pids, fn pid ->
      send(pid, :finish)
      ref = Process.monitor(pid)
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      end
    end)
  end

  defp client_loop(i) do
    v = KV.get("#{i}")
    :ok = KV.set("#{i}", "#{String.to_integer(v) + 1}")
    receive do
      :finish -> :ok
    after
      100 -> client_loop(i)
    end
  end

  defp consensus_group_names() do
    for {g, _} <- RaftFleet.consensus_groups(), String.starts_with?("#{g}", "#{@ks_name}_") do
      g
    end
  end

  defp wait_until_all_consensus_groups_removed(tries \\ 0) do
    if tries > 10 do
      raise "timeout in waiting for removal of consensus groups for #{@ks_name}!"
    end
    RaftFleet.consensus_groups()
    |> Enum.filter(fn {g, _} ->
      Atom.to_string(g) |> String.starts_with?("#{@ks_name}_")
    end)
    |> case do
      []         -> :ok
      _non_empty ->
        :timer.sleep(1000)
        wait_until_all_consensus_groups_removed(tries + 1)
    end
  end

  setup_all do
    case RaftFleet.activate("zone") do
      :ok                     -> :timer.sleep(100)
      {:error, :not_inactive} -> :ok
    end
    :ok = RaftKV.init()
    assert RaftKV.list_keyspaces() == []
    assert RaftFleet.consensus_groups() |> Map.keys() == [RaftKV.Keyspaces]
    :ok
  end

  test "split/merge shards in a keyspace while handling client queries/commands" do
    :ok = RaftKV.register_keyspace(@ks_name, [], KV, Hook, @policy1)
    assert RaftKV.list_keyspaces() == [@ks_name]
    assert RaftKV.register_keyspace(@ks_name, [], KV, Hook, @policy1) == {:error, :already_registered}

    Enum.each(0 .. (@n_keys - 1), fn i -> KV.set("#{i}", "#{i}") end)
    assert consensus_group_names() |> length() == 1

    with_clients(fn ->
      assert RaftKV.get_keyspace_policy(@ks_name) == @policy1
      :ok = RaftKV.set_keyspace_policy(@ks_name, @policy2)
      assert RaftKV.get_keyspace_policy(@ks_name) == @policy2
      :timer.sleep(25_000)
      assert consensus_group_names() |> length() == 8

      :ok = RaftKV.set_keyspace_policy(@ks_name, @policy1)
      assert RaftKV.get_keyspace_policy(@ks_name) == @policy1
      :timer.sleep(30_000)
      assert consensus_group_names() |> length() == 4
    end)

    Enum.each(0 .. (@n_keys - 1), fn i ->
      :ok = KV.unset("#{i}")
      assert KV.get("#{i}") == nil
    end)

    :ok = RaftKV.deregister_keyspace(@ks_name)
    assert RaftKV.list_keyspaces() == []
    assert RaftKV.deregister_keyspace(@ks_name) == {:error, :no_such_keyspace}
    wait_until_all_consensus_groups_removed()
  end
end
