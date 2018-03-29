defmodule RaftKVTest do
  use ExUnit.Case
  @moduletag timeout: 200_000

  @n_keys 1000
  @ks_name :kv
  @policy1 %RaftKV.SplitMergePolicy{max_shards: 8, min_shards: 4, max_keys_per_shard: 1000}
  @policy2 Map.put(@policy1, :max_keys_per_shard, 50)

  defp with_clients(n_clients, loop_fn, f) do
    pairs = Enum.map(0 .. (n_clients - 1), fn i -> {i, spawn_link(fn -> loop_fn.(i) end)} end)
    f.()
    Enum.map(pairs, fn {i, pid} ->
      Process.unlink(pid)
      ref = Process.monitor(pid)
      send(pid, :finish)
      receive do
        {:DOWN, ^ref, :process, ^pid, {:shutdown, n_inc}} -> {i, n_inc}
      end
    end)
  end

  defp get_or_inc_client_loop(i, n_inc \\ 0, n_times \\ 0) do
    n_inc2 =
      if rem(n_times, 2) == 0 do
        assert KV.get(i) == n_inc
        n_inc
      else
        assert KV.inc(i) == :ok
        n_inc + 1
      end
    receive do
      :finish -> exit({:shutdown, n_inc2})
    after
      100 -> get_or_inc_client_loop(i, n_inc2, n_times + 1)
    end
  end

  defp inc_all_client_loop(i, n_times \\ 0) do
    shard_names = RaftKV.reduce_keyspace_shard_names(@ks_name, [], &[&1 | &2])
    Enum.each(shard_names, fn shard_name ->
      assert RaftKV.command_on_all_keys_in_shard(shard_name, :inc) == :ok
    end)
    n_times2 = n_times + 1
    receive do
      :finish -> exit({:shutdown, n_times2})
    after
      100 -> inc_all_client_loop(i, n_times2)
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

  defp switch_policy(policy) do
    assert RaftKV.set_keyspace_policy(@ks_name, policy) == :ok
    assert RaftKV.get_keyspace_policy(@ks_name) == policy
  end

  setup_all do
    :ok = RaftKV.init()
    assert RaftKV.list_keyspaces() == []
    assert RaftFleet.consensus_groups() |> Map.keys() == [RaftKV.Keyspaces]
    :ok
  end

  test "split/merge shards in a keyspace while handling client queries/commands" do
    :ok = RaftKV.register_keyspace(@ks_name, [], KV, Hook, @policy1)
    assert RaftKV.list_keyspaces() == [@ks_name]
    assert RaftKV.register_keyspace(@ks_name, [], KV, Hook, @policy1) == {:error, :already_registered}

    keys = Enum.to_list(0 .. (@n_keys - 1))
    Enum.each(keys, fn i -> KV.set(i, 0) end)
    assert consensus_group_names() |> length() == 1

    n_inc_alls =
      with_clients(10, &inc_all_client_loop/1, fn ->
        switch_policy(@policy2)
        :timer.sleep(30_000)
        assert consensus_group_names() |> length() == 8
        switch_policy(@policy1)
        :timer.sleep(30_000)
        assert consensus_group_names() |> length() == 4
      end)
    n_inc_all = Enum.map(n_inc_alls, fn {_, n} -> n end) |> Enum.sum()
    assert get_all_keys() == keys
    Enum.each(keys, fn i ->
      # "Fetching all shard names and then running :inc as all_keys command" is inherently not rigorous;
      # we accept slight deviations from the expected value.
      v = KV.get(i)
      assert n_inc_all - 10 < v
      assert v              < n_inc_all + 10
    end)

    # reset values
    Enum.each(keys, fn i -> KV.set(i, 0) end)

    n_increments =
      with_clients(@n_keys, &get_or_inc_client_loop/1, fn ->
        switch_policy(@policy2)
        :timer.sleep(30_000)
        assert consensus_group_names() |> length() == 8
        switch_policy(@policy1)
        :timer.sleep(30_000)
        assert consensus_group_names() |> length() == 4
      end)
    assert get_all_keys() == keys
    Enum.each(n_increments, fn {i, n} ->
      assert KV.get(i) == n
    end)

    Enum.each(keys, fn i ->
      :ok = KV.unset(i)
      assert KV.get(i) == nil
    end)
    assert get_all_keys() == []

    :ok = RaftKV.deregister_keyspace(@ks_name)
    assert RaftKV.list_keyspaces() == []
    assert RaftKV.deregister_keyspace(@ks_name) == {:error, :no_such_keyspace}
    wait_until_all_consensus_groups_removed()
  end

  test "error response in manipulating keyspaces" do
    assert RaftKV.get_keyspace_policy(:foo)           == nil
    assert RaftKV.set_keyspace_policy(:foo, nil)      == {:error, :invalid_policy}
    assert RaftKV.set_keyspace_policy(:foo, @policy1) == {:error, :no_such_keyspace}
  end
end
