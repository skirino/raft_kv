defmodule RaftKV.SplitTest do
  use ExUnit.Case
  alias RaftKV.{Hash, Range}
  alias RaftKV.Workflow.{SplitRange, MergeRanges}

  # In this test we want to test `SplitRange.transfer_latter_half/3` and `MergeRanges.transfer_latter_half/3` in isolation,
  # and thus we don't call `RaftKV.init/0` (i.e., `Keyspaces` consensus group is not ready).

  @ks_name        :kv2
  @split_position div(Hash.upper_bound(), 2)
  @cg_former      :"#{@ks_name}_0"
  @cg_latter      :"#{@ks_name}_#{@split_position}"

  setup_all do
    case RaftFleet.activate("zone") do
      :ok                     -> :timer.sleep(100)
      {:error, :not_inactive} -> :ok
    end
    :ok = RaftKV.add_1st_consensus_group(@ks_name, [])
    :ok = Range.initialize_1st_range(@ks_name, KV, nil, 0, Hash.upper_bound())
    :ok = SplitRange.create_consensus_group(@ks_name, 0, @split_position)
    groups = RaftFleet.consensus_groups() |> Map.keys() |> Enum.sort()
    assert groups == [@cg_former, @cg_latter]
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

  defp assert_range_state(cg, status, range_start, range_end) do
    s = get_range_state(cg)
    assert s.status      == status
    assert s.range_start == range_start
    assert s.range_end   == range_end
  end

  defp get_range_state(cg) do
    {:leader, %RaftedValue.Server.State{data: range}} = :sys.get_state(cg)
    range
  end

  test "SplitRange.transfer_latter_half/3 and MergeRanges.transfer_latter_half/3 should be retriable" do
    #
    # split
    #
    catch_error SplitRange.transfer_latter_half(@ks_name, 0, @split_position, 0)
    assert_range_state(@cg_former, {:pre_split_former, []}, 0, Hash.upper_bound())
    assert get_range_state(@cg_latter) == :uninitialized

    catch_error SplitRange.transfer_latter_half(@ks_name, 0, @split_position, 1)
    assert_range_state(@cg_former, {:pre_split_former, []}, 0, Hash.upper_bound())
    assert_range_state(@cg_latter, {:pre_split_latter, @split_position}, Hash.upper_bound(), Hash.upper_bound())

    catch_error SplitRange.transfer_latter_half(@ks_name, 0, @split_position, 2)
    assert_range_state(@cg_former, {:post_split_former, []}, 0, @split_position)
    assert_range_state(@cg_latter, {:pre_split_latter, @split_position}, Hash.upper_bound(), Hash.upper_bound())

    catch_error SplitRange.transfer_latter_half(@ks_name, 0, @split_position, 3)
    assert_range_state(@cg_former, {:post_split_former, []}, 0, @split_position)
    assert_range_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    :ok = SplitRange.transfer_latter_half(@ks_name, 0, @split_position)
    assert_range_state(@cg_former, :normal, 0, @split_position)
    assert_range_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    # retry as a whole (in case reporting to `Keyspaces` consensus group failed)
    :ok = SplitRange.transfer_latter_half(@ks_name, 0, @split_position)
    assert_range_state(@cg_former, :normal, 0, @split_position)
    assert_range_state(@cg_latter, :normal, @split_position, Hash.upper_bound())

    #
    # merge
    #
    catch_error MergeRanges.transfer_latter_half(@ks_name, 0, @split_position, 0)
    assert_range_state(@cg_former, :normal, 0, @split_position)
    assert_range_state(@cg_latter, {:pre_merge_latter, []}, @split_position, Hash.upper_bound())

    latter_range_data = get_range_state(@cg_latter) |> Map.take([:range_start, :range_middle, :range_end, :keys_former_half, :keys_latter_half, :size_former_half, :size_latter_half])
    catch_error MergeRanges.transfer_latter_half(@ks_name, 0, @split_position, 1)
    assert_range_state(@cg_former, {:pre_merge_former, latter_range_data}, 0, @split_position)
    assert_range_state(@cg_latter, {:pre_merge_latter, []}, @split_position, Hash.upper_bound())

    catch_error MergeRanges.transfer_latter_half(@ks_name, 0, @split_position, 2)
    assert_range_state(@cg_former, {:pre_merge_former, latter_range_data}, 0, @split_position)
    assert_range_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())

    :ok = MergeRanges.transfer_latter_half(@ks_name, 0, @split_position)
    assert_range_state(@cg_former, :normal, 0, Hash.upper_bound())
    assert_range_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())

    # retry as a whole (in case reporting to `Keyspaces` consensus group failed)
    :ok = MergeRanges.transfer_latter_half(@ks_name, 0, @split_position)
    assert_range_state(@cg_former, :normal, 0, Hash.upper_bound())
    assert_range_state(@cg_latter, {:post_merge_latter, []}, Hash.upper_bound(), Hash.upper_bound())
  end
end
