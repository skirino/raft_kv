use Croma

defmodule RaftKV.Workflow do
  alias RaftKV.{Hash, Range, Keyspaces, EtsRecordManager}

  defmodule DeregisterKeyspace do
    # Deregistering a keyspace involves:
    # (1) `RaftKV.deregister_keyspace/1` removes the entry in `Keyspaces` consensus group.
    # (2) `WorkflowExecutor` removes all consensus groups for the keyspace.
    # (3) `WorkflowExecutor` confirms that all relevant nodes have removed ETS records for the keyspace.
    # (2) and (3) are executed as a 2-step workflow.

    @type t :: {:remove_consensus_groups | :remove_ets_records_in_all_nodes, atom}

    defun first(ks_name :: v[atom]) :: t do
      {:remove_consensus_groups, ks_name}
    end

    defun next(t :: t) :: nil | t do
      {:remove_consensus_groups, ks_name}   -> {:remove_ets_records_in_all_nodes, ks_name}
      {:remove_ets_records_in_all_nodes, _} -> nil
    end

    defun execute(t :: t) :: :ok do
      {:remove_consensus_groups, ks_name}         -> remove_consensus_groups(ks_name)
      {:remove_ets_records_in_all_nodes, ks_name} -> EtsRecordManager.ensure_record_changed_in_all_relevant_nodes!({:delete_all, ks_name})
    end

    defunp remove_consensus_groups(ks_name :: v[atom]) :: :ok do
      prefix = Atom.to_string(ks_name) <> "_"
      RaftFleet.consensus_groups()
      |> Map.keys()
      |> Enum.filter(&group_name_matches_keyspace?(&1, prefix))
      |> Enum.each(&remove_consensus_group!/1)
    end

    defp remove_consensus_group!(g) do
      case RaftFleet.remove_consensus_group(g) do
        :ok                  -> :ok
        {:error, :not_found} -> :ok
      end
    end

    defunp group_name_matches_keyspace?(g :: v[atom], prefix :: v[String.t]) :: boolean do
      size = byte_size(prefix)
      case Atom.to_string(g) do
        <<^prefix :: binary-size(size), number :: binary>> ->
          try do
            _ = String.to_integer(number)
            true
          rescue
            ArgumentError -> false
          end
        _ -> false
      end
    end
  end

  defmodule SplitRange do
    # Spliting a range into two involves:
    # (1) `WorkflowExecutor` polls from `Keyspaces` consensus group and find that a range needs to be split.
    # (2) `WorkflowExecutor` creates new consensus group for the latter half of the target range and
    #     waits until the 3 members become up and running.
    # (3) `WorkflowExecutor` takes data from original range, passes the data to the new range,
    #     changes the original range to handle only former half and then tell the new range to handle client requests.
    # (4) `WorkflowExecutor` confirms that all relevant nodes have added the ETS record for the new range.
    # (2), (3) and (4) are executed as a 3-step workflow.

    @type t :: {:create_consensus_group | :transfer_latter_half | :add_ets_record_in_all_nodes, atom, Hash.t, Hash.t}

    defun first(ks_name :: v[atom], target_range_start :: v[Hash.t], new_range_start :: v[Hash.t]) :: t do
      {:create_consensus_group, ks_name, target_range_start, new_range_start}
    end

    defun next(t :: t) :: nil | t do
      {:create_consensus_group     , ks , s1 , s2 } -> {:transfer_latter_half       , ks, s1, s2}
      {:transfer_latter_half       , ks , s1 , s2 } -> {:add_ets_record_in_all_nodes, ks, s1, s2}
      {:add_ets_record_in_all_nodes, _ks, _s1, _s2} -> nil
    end

    defun execute(t :: t) :: :ok | {:abort, String.t} do
      {:create_consensus_group     , ks, s1 , s2} -> create_consensus_group(ks, s1, s2)
      {:transfer_latter_half       , ks, s1 , s2} -> transfer_latter_half(ks, s1, s2)
      {:add_ets_record_in_all_nodes, ks, _s1, s2} -> EtsRecordManager.ensure_record_changed_in_all_relevant_nodes!({:ensure_record_created, ks, s2})
    end

    defpt create_consensus_group(ks_name, former_range_start, latter_range_start) do
      cg_name = Range.consensus_group_name(ks_name, latter_range_start)
      case RaftFleet.add_consensus_group(cg_name, 3, get_rv_config(ks_name, former_range_start)) do
        :ok                      -> :ok
        {:error, :already_added} -> :ok
      end
      wait_until_consensus_members_are_ready(cg_name)
    end

    defp get_rv_config(ks_name, former_range_start) do
      former_cg_name = Range.consensus_group_name(ks_name, former_range_start)
      leader = RaftFleet.whereis_leader(former_cg_name)
      %{config: rv_config} = RaftedValue.status(leader)
      rv_config
    end

    defp wait_until_consensus_members_are_ready(cg_name) do
      case RaftFleet.active_nodes() |> Enum.flat_map(fn {_z, ns} -> ns end) do
        [_1, _2, _3 | _]   -> wait_until_3_members_are_ready(cg_name)
        _only_1_or_2_nodes -> :ok
      end
    end

    defp wait_until_3_members_are_ready(cg_name, n_tries \\ 0) do
      if n_tries > 5 do
        raise ""
      else
        :timer.sleep(2_000)
        leader = RaftFleet.whereis_leader(cg_name)
        case RaftedValue.status(leader) do
          %{members: ms, unresponsive_followers: []} when length(ms) >= 3 -> :ok
          _otherwise                                                      -> wait_until_3_members_are_ready(cg_name, n_tries + 1)
        end
      end
    end

    defpt transfer_latter_half(ks_name, former_range_start, latter_range_start, halt_at \\ 10) do
      cg_former = Range.consensus_group_name(ks_name, former_range_start)
      cg_latter = Range.consensus_group_name(ks_name, latter_range_start)
      case prepare_former(cg_former, former_range_start, latter_range_start) do
        {:ok, data}      -> transfer_latter_half_impl1(cg_former, cg_latter, latter_range_start, data, halt_at)
        {:skip, cmds}    -> transfer_latter_half_impl2(cg_former, cg_latter, latter_range_start, cmds, halt_at)
        :skip_all        -> :ok
        {:abort, reason} -> {:abort, reason}
      end
    end

    defp transfer_latter_half_impl1(cg_former, cg_latter, latter_range_start, data, halt_at) do
      with :ok         <- prepare_latter(cg_latter, latter_range_start, data, halt_at),
           {:ok, cmds} <- shift_range_former(cg_former, latter_range_start, halt_at) do
        transfer_latter_half_impl2(cg_former, cg_latter, latter_range_start, cmds, halt_at)
      else
        {:abort, reason} -> {:abort, reason}
      end
    end

    defp transfer_latter_half_impl2(cg_former, cg_latter, latter_range_start, cmds, halt_at) do
      case shift_range_latter(cg_latter, latter_range_start, cmds, halt_at) do
        :ok              -> discard_commands(cg_former, halt_at)
        {:abort, reason} -> {:abort, reason}
      end
    end

    defunp prepare_former(cg_former :: v[atom], former_range_start :: v[Hash.t], latter_range_start :: v[Hash.t]) :: {:ok, map} | {:skip, [any]} | :skip_all | {:abort, String.t} do
      {:ok, r} = RaftFleet.command(cg_former, {:split_prepare, latter_range_start})
      case r do
        {:ok, data}                                        -> {:ok, data}
        {:error, {:range_already_shifted, cmds}}           -> {:skip, cmds}
        {:error, {:status_unmatch, label, r_start, r_end}} ->
          case {label, r_start, r_end} do
            {:normal, ^former_range_start, ^latter_range_start} -> :skip_all # Already split, nothing to do.
            _                                                   -> {:abort, "range #{cg_former} can't be split due to status unmatch: #{label}, #{r_start}-#{r_end}"}
          end
      end
    end

    defunp prepare_latter(cg_latter :: v[atom], latter_range_start :: v[Hash.t], data :: v[map], halt_at :: v[non_neg_integer]) :: :ok do
      if halt_at <= 0, do: raise "halt for testing: #{halt_at}"
      {:ok, r} = RaftFleet.command(cg_latter, {:split_install, latter_range_start, data})
      case r do
        :ok                            -> :ok
        {:error, :already_initialized} -> :ok
      end
    end

    defunp shift_range_former(cg_former :: v[atom], latter_range_start :: v[Hash.t], halt_at :: v[non_neg_integer]) :: {:ok, [{any, any}]} | {:abort, String.t} do
      if halt_at <= 1, do: raise "halt for testing: #{halt_at}"
      {:ok, r} = RaftFleet.command(cg_former, {:split_shift_range_end, latter_range_start})
      case r do
        {:ok, cmds}                                        -> {:ok, cmds}
        {:error, {:status_unmatch, label, r_start, r_end}} -> {:abort, "range #{cg_former} can't shift range_end due to status unmatch: #{label}, #{r_start}-#{r_end}"}
      end
    end

    defunp shift_range_latter(cg_latter :: v[atom], latter_range_start :: v[Hash.t], cmds :: [{any, any}], halt_at :: v[non_neg_integer]) :: :ok | {:abort, String.t} do
      if halt_at <= 2, do: raise "halt for testing: #{halt_at}"
      # Commands given from the former half is in reversed order; here we reverse it (outside of Raft consensus group)
      {:ok, r} = RaftFleet.command(cg_latter, {:split_shift_range_start, Enum.reverse(cmds)})
      case r do
        :ok                                                               -> :ok
        {:error, {:status_unmatch, :normal, ^latter_range_start, _r_end}} -> :ok # Already shifted, proceed
        {:error, {:status_unmatch, label, r_start, r_end}}                -> {:abort, "range #{cg_latter} can't shift range_start due to status unmatch: #{label}, #{r_start}-#{r_end}"}
      end
    end

    defunp discard_commands(cg_former :: v[atom], halt_at :: v[non_neg_integer]) :: :ok do
      if halt_at <= 3, do: raise "halt for testing: #{halt_at}"
      {:ok, :ok} = RaftFleet.command(cg_former, :split_discard_commands)
      :ok
    end
  end

  defmodule MergeRanges do
    # Merging two consecutive ranges into one involves:
    # (1) `WorkflowExecutor` polls from `Keyspaces` consensus group and find that two consecutive ranges need to be merged.
    # (2) `WorkflowExecutor` takes data from the latter range, passes the data to the former range,
    #     turns the latter range into inactive state and then tell the former range to handle both of the ranges.
    # (3) `WorkflowExecutor` confirms that all relevant nodes have removed the ETS record for the latter range.
    # (4) `WorkflowExecutor` removes the consensus group for the latter range.
    # (2), (3) and (4) are executed as a 3-step workflow.

    @type t :: {:transfer_latter_half | :remove_ets_record_in_all_nodes | :remove_consensus_group, atom, Hash.t, Hash.t}

    defun first(ks_name :: v[atom], former_range_start :: v[Hash.t], latter_range_start :: v[Hash.t]) :: t do
      {:transfer_latter_half, ks_name, former_range_start, latter_range_start}
    end

    defun next(t :: t) :: nil | t do
      {:transfer_latter_half          , ks , s1 , s2 } -> {:remove_ets_record_in_all_nodes, ks, s1, s2}
      {:remove_ets_record_in_all_nodes, ks , s1 , s2 } -> {:remove_consensus_group        , ks, s1, s2}
      {:remove_consensus_group        , _ks, _s1, _s2} -> nil
    end

    defun execute(t :: t) :: :ok | {:abort, String.t} do
      {:transfer_latter_half          , ks, s1 , s2} -> transfer_latter_half(ks, s1, s2)
      {:remove_ets_record_in_all_nodes, ks, _s1, s2} -> EtsRecordManager.ensure_record_changed_in_all_relevant_nodes!({:ensure_record_removed, ks, s2})
      {:remove_consensus_group        , ks, _s1, s2} -> remove_consensus_group(ks, s2)
    end

    defpt transfer_latter_half(ks_name, former_range_start, latter_range_start, halt_at \\ 10) do
      cg_former = Range.consensus_group_name(ks_name, former_range_start)
      cg_latter = Range.consensus_group_name(ks_name, latter_range_start)
      case prepare_latter(cg_latter) do
        {:ok, data}      -> transfer_latter_half_impl(cg_former, cg_latter, latter_range_start, data, halt_at)
        {:skip, cmds}    -> shift_range_former(cg_former, latter_range_start, cmds, halt_at)
        {:abort, reason} -> {:abort, reason}
      end
    end

    defp transfer_latter_half_impl(cg_former, cg_latter, latter_range_start, data, halt_at) do
      with :ok         <- prepare_former(cg_former, data, halt_at),
           {:ok, cmds} <- shift_range_latter(cg_latter, halt_at) do
        shift_range_former(cg_former, latter_range_start, cmds, halt_at)
      else
        {:abort, reason} -> {:abort, reason}
      end
    end

    defunp prepare_latter(cg_latter :: v[atom]) :: {:ok, map} | {:skip, [any]} | {:abort, String.t} do
      {:ok, r} = RaftFleet.command(cg_latter, :merge_prepare)
      case r do
        {:ok, data}                                        -> {:ok, data}
        {:error, {:range_already_shifted, cmds}}           -> {:skip, cmds}
        {:error, {:status_unmatch, label, r_start, r_end}} -> {:abort, "range #{cg_latter} can't be merged due to status unmatch: #{label}, #{r_start}-#{r_end}"}
      end
    end

    defunp prepare_former(cg_former :: v[atom], data :: v[map], halt_at :: v[Hash.t]) :: :ok | {:abort, String.t} do
      if halt_at <= 0, do: raise "halt for testing: #{halt_at}"
      {:ok, r} = RaftFleet.command(cg_former, {:merge_install, data})
      case r do
        :ok                                                -> :ok
        {:error, {:status_unmatch, label, r_start, r_end}} -> {:abort, "range #{cg_former} cannot install data from successive range due to status unmatch: #{label}, #{r_start}-#{r_end}"}
      end
    end

    defunp shift_range_latter(cg_latter :: v[atom], halt_at :: v[Hash.t]) :: {:ok, [any]} | {:abort, String.t} do
      if halt_at <= 1, do: raise "halt for testing: #{halt_at}"
      {:ok, r} = RaftFleet.command(cg_latter, :merge_shift_range_start)
      case r do
        {:ok, cmds}                                        -> {:ok, cmds}
        {:error, {:status_unmatch, label, r_start, r_end}} -> {:abort, "range #{cg_latter} can't shift range start due to status unmatch: #{label}, #{r_start}-#{r_end}"}
      end
    end

    defunp shift_range_former(cg_former :: v[atom], latter_range_start :: v[Hash.t], cmds :: v[[any]], halt_at :: v[Hash.t]) :: :ok do
      if halt_at <= 2, do: raise "halt for testing: #{halt_at}"
      # Commands given from the latter half is in reversed order; here we reverse it (outside of Raft consensus group)
      {:ok, :ok} = RaftFleet.command(cg_former, {:merge_shift_range_end, latter_range_start, cmds})
      :ok
    end

    defp remove_consensus_group(ks_name, latter_range_start) do
      cg_name = Range.consensus_group_name(ks_name, latter_range_start)
      case RaftFleet.remove_consensus_group(cg_name) do
        :ok                  -> :ok
        {:error, :not_found} -> :ok
      end
    end
  end

  @type t :: {DeregisterKeyspace, DeregisterKeyspace.t} | {SplitRange, SplitRange.t} | {MergeRanges, MergeRanges.t}

  defun valid?(t :: any) :: boolean do
    {DeregisterKeyspace, _} -> true
    {SplitRange        , _} -> true
    {MergeRanges       , _} -> true
    _                       -> false
  end

  defun execute({mod, data} = pair :: v[t]) :: :ok do
    case mod.execute(data) do
      :ok ->
        case Keyspaces.workflow_proceed(pair) do
          nil  -> :ok
          next -> execute(next)
        end
      {:abort, reason} ->
        require Logger
        Logger.error("aborting #{inspect(mod)} #{inspect(data)} : #{reason}")
        Keyspaces.workflow_abort(pair)
    end
  end
end
