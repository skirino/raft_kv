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

  defp with_clients(n_clients, loop_fn, f) do
    pids = Enum.map(0 .. (n_clients - 1), fn i -> spawn_link(fn -> loop_fn.(i) end) end)
    f.()
    Enum.each(pids, fn pid ->
      send(pid, :finish)
      ref = Process.monitor(pid)
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      end
    end)
  end

  defp inc_client_loop(i) do
    v = KV.get("#{i}")
    :ok = KV.set("#{i}", "#{String.to_integer(v) + 1}")
    receive do
      :finish -> :ok
    after
      100 -> inc_client_loop(i)
    end
  end

  defp append_zero_all_client_loop(i) do
    shard_names = RaftKV.reduce_keyspace_shard_names(@ks_name, [], &[&1 | &2])
    Enum.each(shard_names, fn shard_name ->
      :ok = RaftKV.command_on_all_keys_in_shard(shard_name, :append_zero)
    end)
    receive do
      :finish -> :ok
    after
      100 -> append_zero_all_client_loop(i)
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

  defp get_all_keys() do
    RaftKV.reduce_keyspace_shard_names(@ks_name, [], &[&1 | &2])
    |> Enum.flat_map(&RaftKV.list_keys_in_shard/1)
    |> Enum.sort()
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

    keys = Enum.map(0 .. (@n_keys - 1), fn i -> "#{i}" end) |> Enum.sort()
    Enum.each(keys, fn k -> KV.set(k, k) end)
    assert consensus_group_names() |> length() == 1

    with_clients(@n_keys, &inc_client_loop/1, fn ->
      with_clients(1, &append_zero_all_client_loop/1, fn ->
        assert get_all_keys() == keys
        assert RaftKV.get_keyspace_policy(@ks_name) == @policy1
        :ok = RaftKV.set_keyspace_policy(@ks_name, @policy2)
        assert RaftKV.get_keyspace_policy(@ks_name) == @policy2
        :timer.sleep(25_000)
        assert consensus_group_names() |> length() == 8
        assert get_all_keys() == keys
        :ok = RaftKV.set_keyspace_policy(@ks_name, @policy1)
        assert RaftKV.get_keyspace_policy(@ks_name) == @policy1
        :timer.sleep(30_000)
        assert consensus_group_names() |> length() == 4
        assert get_all_keys() == keys
      end)
    end)

    Enum.each(keys, fn k ->
      :ok = KV.unset(k)
      assert KV.get(k) == nil
    end)
    assert get_all_keys() == []

    :ok = RaftKV.deregister_keyspace(@ks_name)
    assert RaftKV.list_keyspaces() == []
    assert RaftKV.deregister_keyspace(@ks_name) == {:error, :no_such_keyspace}
    wait_until_all_consensus_groups_removed()
  end
end
