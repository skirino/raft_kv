defmodule RaftKV.SplitMergeTest do
  use ExUnit.Case
  alias RaftKV.{Hash, Shard}
  alias RaftKV.Workflow.{SplitShard, MergeShards}

  # In this test we want to test `SplitShard.transfer_latter_half/3` and `MergeShards.transfer_latter_half/3` in isolation,
  # and thus we don't call `RaftKV.init/0` (i.e., `Keyspaces` consensus group is not ready).

  @ks_name        :kv2
  @split_position div(Hash.upper_bound(), 2)
  @cg_former      :"#{@ks_name}_0"
  @cg_latter      :"#{@ks_name}_#{@split_position}"

  setup_all do
    :ok = RaftKV.add_1st_consensus_group(@ks_name, [])
    :ok = Shard.initialize_1st_shard(@ks_name, KV, nil, 0, Hash.upper_bound())
    :ok = SplitShard.create_consensus_group(@ks_name, 0, @split_position)
    groups = RaftFleet.consensus_groups() |> Map.keys() |> Enum.sort() # may contain `Keyspaces` depending on test execution order
    assert @cg_former in groups
    assert @cg_latter in groups
    on_exit(fn ->
      Enum.each(groups, &RaftFleet.remove_consensus_group/1)
      kill_member(@cg_former)
      kill_member(@cg_latter)
    end)
  end

  defp kill_member(cg) do
    try do
      :gen_statem.stop(cg)
    catch
      :exit, _ -> :ok
    end
  end

  defp assert_shard_state(cg, status, range_start, range_end) do
    s = get_shard_state(cg)
    assert s.status      == status
    assert s.range_start == range_start
    assert s.range_end   == range_end
  end

  defp get_shard_state(cg) do
    {:leader, %RaftedValue.Server.State{data: shard}} = :sys.get_state(cg)
    shard
  end

  test "SplitShard.transfer_latter_half/3 and MergeShards.transfer_latter_half/3 should be retriable" do
    #
    # split
    #
    catch_error SplitShard.transfer_latter_half(@ks_name, 0, @split_position, 0)
    assert_shard_state(@cg_former, {:pre_split_former, []}, 0, Hash.upper_bound())
    assert get_shard_state(@cg_latter) == :uninitialized

    catch_error SplitShard.transfer_latter_half(@ks_name, 0, @split_position, 1)
    assert_shard_state(@cg_former, {:pre_split_former, []}, 0, Hash.upper_bound())
    assert_shard_state(@cg_latter, {:pre_split_latter, @split_position}, Hash.upper_bound(), Hash.upper_bound())

    catch_error SplitShard.transfer_latter_half(@ks_name, 0, @split_position, 2)
    assert_shard_state(@cg_former, {:post_split_former, []}, 0, @split_position)
    assert_shard_state(@cg_latter, {:pre_split_latter, @split_position}, Hash.upper_bound(), Hash.upper_bound())

    catch_error SplitShard.transfer_latter_half(@ks_name, 0, @split_position, 3)
    assert_shard_state(@cg_former, {:post_split_former, []}, 0, @split_position)
    assert_shard_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    :ok = SplitShard.transfer_latter_half(@ks_name, 0, @split_position)
    assert_shard_state(@cg_former, :normal, 0, @split_position)
    assert_shard_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    # retry as a whole (in case reporting to `Keyspaces` consensus group failed)
    :ok = SplitShard.transfer_latter_half(@ks_name, 0, @split_position)
    assert_shard_state(@cg_former, :normal, 0, @split_position)
    assert_shard_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    #
    # merge
    #
    catch_error MergeShards.transfer_latter_half(@ks_name, 0, @split_position, 0)
    assert_shard_state(@cg_former, :normal, 0, @split_position)
    assert_shard_state(@cg_latter, {:pre_merge_latter, []}, @split_position, Hash.upper_bound())

    latter_shard_data = get_shard_state(@cg_latter) |> Map.take([:range_start, :range_middle, :range_end, :keys_former_half, :keys_latter_half, :size_former_half, :size_latter_half])
    catch_error MergeShards.transfer_latter_half(@ks_name, 0, @split_position, 1)
    assert_shard_state(@cg_former, {:pre_merge_former, latter_shard_data}, 0, @split_position)
    assert_shard_state(@cg_latter, {:pre_merge_latter, []}, @split_position, Hash.upper_bound())

    catch_error MergeShards.transfer_latter_half(@ks_name, 0, @split_position, 2)
    assert_shard_state(@cg_former, {:pre_merge_former, latter_shard_data}, 0, @split_position)
    assert_shard_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())

    :ok = MergeShards.transfer_latter_half(@ks_name, 0, @split_position)
    assert_shard_state(@cg_former, :normal, 0, Hash.upper_bound())
    assert_shard_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())

    # retry as a whole (in case reporting to `Keyspaces` consensus group failed)
    :ok = MergeShards.transfer_latter_half(@ks_name, 0, @split_position)
    assert_shard_state(@cg_former, :normal, 0, Hash.upper_bound())
    assert_shard_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())
  end
end
