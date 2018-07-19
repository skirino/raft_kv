use Croma

defmodule RaftKV.Keyspaces do
  alias RaftedValue.Data, as: RVData
  alias RaftKV.{Hash, KeyspaceInfo, SplitMergePolicy, Workflow, EtsRecordManager, Config}

  defmodule KeyspaceMap do
    use Croma.SubtypeOfMap, key_module: Croma.Atom, value_module: RaftKV.KeyspaceInfo
  end

  defmodule OngoingWorkflow do
    use Croma.SubtypeOfTuple, elem_modules: [Workflow, Croma.Atom, Croma.NonNegInteger]
  end

  defmodule SplitCandidates do
    defmodule Elem do
      use Croma.SubtypeOfTuple, elem_modules: [Croma.Float, Croma.Atom, Hash]
    end

    use Croma.SubtypeOfList, max_length: 5, elem_module: Elem

    # Note that `new_cs` may be longer than `@max`
    defun add(cs :: v[t], new_cs :: v[[Elem.t]]) :: v[t] do
      Enum.uniq_by(new_cs ++ cs, fn {_usage, ks_name, range_start} -> {ks_name, range_start} end)
      |> Enum.sort(&>=/2)
      |> Enum.take(@max)
    end

    defun remove_entries_of_keyspace(cs :: v[t], ks_name :: v[atom]) :: v[t] do
      Enum.reject(cs, fn {_usage, ks, _range_start} -> ks == ks_name end)
    end
  end

  defmodule MergeCandidates do
    defmodule Elem do
      use Croma.SubtypeOfTuple, elem_modules: [Croma.Float, Croma.Atom, Hash, Hash]
    end

    use Croma.SubtypeOfList, max_length: 5, elem_module: Elem

    # Note that `new_cs` may be longer than `@max`
    defun add(cs :: v[t], new_cs :: v[[Elem.t]]) :: v[t] do
      Enum.uniq_by(new_cs ++ cs, fn {_usage, ks_name, range_start1, _range_start2} -> {ks_name, range_start1} end)
      |> Enum.sort()
      |> Enum.take(@max)
    end

    defun remove_entries_of_keyspace(cs :: v[t], ks_name :: v[atom]) :: v[t] do
      Enum.reject(cs, fn {_usage, ks, _range_start1, _range_start2} -> ks == ks_name end)
    end

    defun remove_overlapping_entries(cs :: v[t], ks_name :: v[atom], range_start1 :: v[Hash.t], range_start2 :: v[Hash.t]) :: v[t] do
      Enum.reject(cs, fn {_usage, ks, s1, s2} ->
        ks == ks_name and (s1 == range_start2 or s2 == range_start1)
      end)
    end
  end

  use Croma.Struct, fields: [
    keyspaces:                     KeyspaceMap,
    ongoing_workflow:              Croma.TypeGen.nilable(OngoingWorkflow),
    pending_deregistrations_queue: Croma.Tuple, # :queue.queue(keyspace_name)
    split_candidates:              SplitCandidates,
    merge_candidates:              MergeCandidates,
  ]

  @behaviour RVData

  @impl true
  defun new() :: t do
    %__MODULE__{keyspaces: %{}, pending_deregistrations_queue: :queue.new(), split_candidates: [], merge_candidates: []}
  end

  #
  # command
  #
  @impl true
  defun command(state :: v[t], arg :: RVData.command_arg) :: {RVData.command_ret, t} do
    case arg do
      {:register, ks_name, policy}    -> register_keyspace(state, ks_name, policy)
      {:deregister, ks_name}          -> deregister_keyspace(state, ks_name)
      {:set_policy, ks_name, policy}  -> set_keyspace_policy(state, ks_name, policy)
      {:stats, stats_map, t}          -> {:ok, store_stats(state, stats_map, t)}
      {:workflow_fetch, n, t, l}      -> workflow_fetch(state, n, t, l)
      {:workflow_proceed, pair, n, t} -> workflow_proceed(state, pair, n, t)
      {:workflow_abort, pair}         -> {:ok, workflow_abort(state, pair)}
      _                               -> {:ok, state}
    end
  end

  defp register_keyspace(%__MODULE__{keyspaces: keyspaces} = state, ks_name, policy) do
    case Map.get(keyspaces, ks_name) do
      nil ->
        new_keyspaces = Map.put(keyspaces, ks_name, KeyspaceInfo.make(policy))
        {:ok, %__MODULE__{state | keyspaces: new_keyspaces}}
      _keyspace_exists ->
        {{:error, :already_added}, state}
    end
  end

  defp deregister_keyspace(%__MODULE__{keyspaces: keyspaces} = state, ks_name) do
    if Map.has_key?(keyspaces, ks_name) do
      {:ok, enqueue_deregistration_of_keyspace(state, ks_name)}
    else
      {{:error, :no_such_keyspace}, state}
    end
  end

  defp enqueue_deregistration_of_keyspace(%__MODULE__{keyspaces: keyspaces,
                                                      pending_deregistrations_queue: q,
                                                      split_candidates: scs,
                                                      merge_candidates: mcs} = state,
                                          ks_name) do
    new_keyspaces = Map.delete(keyspaces, ks_name)
    new_scs = SplitCandidates.remove_entries_of_keyspace(scs, ks_name)
    new_mcs = MergeCandidates.remove_entries_of_keyspace(mcs, ks_name)
    %__MODULE__{state |
      keyspaces:                     new_keyspaces,
      pending_deregistrations_queue: :queue.in(ks_name, q),
      split_candidates:              new_scs,
      merge_candidates:              new_mcs,
    }
  end

  defp set_keyspace_policy(%__MODULE__{keyspaces: keyspaces,
                                       split_candidates: scs,
                                       merge_candidates: mcs} = state,
                           ks_name,
                           policy) do
    case Map.get(keyspaces, ks_name) do
      nil     -> {{:error, :no_such_keyspace}, state}
      ks_info ->
        new_keyspaces = Map.put(keyspaces, ks_name, %KeyspaceInfo{ks_info | policy: policy})
        new_scs = SplitCandidates.remove_entries_of_keyspace(scs, ks_name)
        new_mcs = MergeCandidates.remove_entries_of_keyspace(mcs, ks_name)
        new_state = %__MODULE__{state | keyspaces: new_keyspaces, split_candidates: new_scs, merge_candidates: new_mcs}
        {:ok, new_state}
    end
  end

  defp store_stats(%__MODULE__{keyspaces:        keyspaces,
                               split_candidates: split_candidates,
                               merge_candidates: merge_candidates} = state,
                   stats_map,
                   threshold_time) do
    {new_keyspaces, new_split_candidates, new_merge_candidates} =
      Enum.reduce(keyspaces, {%{}, [], []}, fn({ks_name, ks_info}, {m, scs1, mcs1}) ->
        {new_ks_info, scs2, mcs2} =
          case Map.get(stats_map, ks_name) do
            nil    -> {ks_info, []}
            submap -> KeyspaceInfo.store(ks_info, submap, threshold_time)
          end
        scs3 = Enum.map(scs2, fn {usage, start} -> {usage, ks_name, start} end) ++ scs1
        mcs3 = Enum.map(mcs2, fn {usage, start1, start2} -> {usage, ks_name, start1, start2} end) ++ mcs1
        {Map.put(m, ks_name, new_ks_info), scs3, mcs3}
      end)
    %__MODULE__{state |
      keyspaces:        new_keyspaces,
      split_candidates: SplitCandidates.add(split_candidates, new_split_candidates),
      merge_candidates: MergeCandidates.add(merge_candidates, new_merge_candidates),
    }
  end

  defp workflow_fetch(%__MODULE__{ongoing_workflow: ongoing} = state, node, time, lock_period) do
    case ongoing do
      nil                                                       -> find_next_workflow(state, node, time)
      {_task, n , t } when n != node and time < t + lock_period -> {:locked, state}
      {task , _n, _t}                                           -> {task, %__MODULE__{state | ongoing_workflow: {task, node, time}}}
    end
  end

  defp find_next_workflow(state1, node, time) do
    case find_next_deregistration(state1) || find_next_shard_split(state1) || find_next_shard_merge(state1) do
      nil            -> {nil, state1}
      {pair, state2} -> {pair, %__MODULE__{state2 | ongoing_workflow: {pair, node, time}}}
    end
  end

  defp find_next_deregistration(%__MODULE__{pending_deregistrations_queue: q1} = state) do
    case :queue.out(q1) do
      {:empty, _q} ->
        nil
      {{:value, ks_name}, q2} ->
        pair = {Workflow.DeregisterKeyspace, Workflow.DeregisterKeyspace.first(ks_name)}
        {pair, %__MODULE__{state | pending_deregistrations_queue: q2}}
    end
  end

  defp find_next_shard_split(%__MODULE__{keyspaces: keyspaces, split_candidates: split_candidates} = state1) do
    case split_candidates do
      [] ->
        nil
      [{_usage_ratio, ks_name, range_start} | scs] ->
        state2 = %__MODULE__{state1 | split_candidates: scs}
        with {:ok, ks_info1}                  <- Map.fetch(keyspaces, ks_name),
             {:ok, ks_info2, new_range_start} <- KeyspaceInfo.check_if_splittable(ks_info1, range_start) do
          pair = {Workflow.SplitShard, Workflow.SplitShard.first(ks_name, range_start, new_range_start)}
          state3 = %__MODULE__{state2 | keyspaces: Map.put(keyspaces, ks_name, ks_info2)}
          {pair, state3}
        else
          :error -> find_next_shard_split(state2)
        end
    end
  end

  defp find_next_shard_merge(%__MODULE__{keyspaces: keyspaces, merge_candidates: merge_candidates} = state) do
    case merge_candidates do
      [] ->
        nil
      [{_usage_ratio, ks_name, range_start1, range_start2} | mcs] ->
        with {:ok, ks_info1} <- Map.fetch(keyspaces, ks_name),
             {:ok, ks_info2} <- KeyspaceInfo.check_if_mergeable(ks_info1, range_start1, range_start2) do
          pair = {Workflow.MergeShards, Workflow.MergeShards.first(ks_name, range_start1, range_start2)}
          new_mcs = MergeCandidates.remove_overlapping_entries(mcs, ks_name, range_start1, range_start2)
          new_state = %__MODULE__{state | keyspaces: Map.put(keyspaces, ks_name, ks_info2), merge_candidates: new_mcs}
          {pair, new_state}
        else
          :error -> find_next_shard_merge(%__MODULE__{state | merge_candidates: mcs})
        end
    end
  end

  defp workflow_proceed(%__MODULE__{ongoing_workflow: ongoing} = state1, {mod, workflow_data} = pair, node, time) do
    case ongoing do
      {^pair, _n, _t} ->
        {next_pair, state2} =
          case mod.next(workflow_data) do
            nil       -> {nil, %__MODULE__{state1 | ongoing_workflow: nil}}
            next_step ->
              next = {mod, next_step}
              {next, %__MODULE__{state1 | ongoing_workflow: {next, node, time}}}
          end
        {next_pair, modify_state_on_completion_of_workflow_step(state2, pair, time)}
      _ ->
        {nil, state1}
    end
  end

  defp modify_state_on_completion_of_workflow_step(%__MODULE__{keyspaces: keyspaces} = state, pair, time) do
    case pair do
      {Workflow.SplitShard, {:create_consensus_group, ks_name, _range_start1, range_start2}} ->
        new_keyspaces = Map.update!(keyspaces, ks_name, &KeyspaceInfo.add_shard(&1, range_start2))
        %__MODULE__{state | keyspaces: new_keyspaces}
      {Workflow.SplitShard, {:transfer_latter_half, ks_name, range_start1, range_start2}} ->
        new_keyspaces = Map.update!(keyspaces, ks_name, &KeyspaceInfo.touch_both(&1, range_start1, range_start2, time))
        %__MODULE__{state | keyspaces: new_keyspaces}
      {Workflow.MergeShards, {:transfer_latter_half, ks_name, range_start1, range_start2}} ->
        new_keyspaces = Map.update!(keyspaces, ks_name, &KeyspaceInfo.touch_and_delete(&1, range_start1, range_start2, time))
        %__MODULE__{state | keyspaces: new_keyspaces}
      _ ->
        state
    end
  end

  defp workflow_abort(%__MODULE__{ongoing_workflow: ongoing} = state, pair) do
    case ongoing do
      {^pair, _n, _t} -> %__MODULE__{state | ongoing_workflow: nil}
      _               -> state
    end
  end

  #
  # query
  #
  @impl true
  defun query(%__MODULE__{keyspaces: keyspaces}, arg :: RVData.query_arg) :: RVData.query_ret do
    case arg do
      :keyspace_names        -> Map.keys(keyspaces)
      :all_keyspace_shards   -> all_keyspace_shards(keyspaces)
      {:get_policy, ks_name} -> policy_of(keyspaces, ks_name)
      _                      -> :ok
    end
  end

  defp all_keyspace_shards(keyspaces) do
    Enum.map(keyspaces, fn {ks_name, ks_info} ->
      {ks_name, KeyspaceInfo.shard_range_start_positions(ks_info)}
    end)
  end

  defp policy_of(keyspaces, ks_name) do
    case Map.get(keyspaces, ks_name) do
      nil                      -> nil
      %KeyspaceInfo{policy: p} -> p
    end
  end

  #
  # hook
  #
  defmodule Hook do
    alias RaftedValue.Data, as: RVData
    alias RaftKV.Keyspaces

    @behaviour RaftedValue.LeaderHook

    @impl true
    defun on_command_committed(_data_before_command :: Keyspaces.t,
                               command_arg          :: RVData.command_arg,
                               command_ret          :: RVData.command_ret,
                               _data_after_command  :: Keyspaces.t) :: any do
      case {command_arg, command_ret} do
        {{:register, ks_name, _policy}, :ok} -> EtsRecordManager.broadcast_new_keyspace(ks_name)
        _                                    -> :ok
      end
    end

    @impl true
    defun on_query_answered(_data      :: Keyspaces.t,
                            _query_arg :: RVData.query_arg,
                            _query_ret :: RVData.query_ret) :: any do
      :ok
    end

    @impl true
    defun on_follower_added(_data :: Keyspaces.t, _pid :: pid) :: any do
      :ok
    end

    @impl true
    defun on_follower_removed(_data :: Keyspaces.t, _pid :: pid) :: any do
      :ok
    end

    @impl true
    defun on_elected(_data :: Keyspaces.t) :: any do
      :ok
    end

    @impl true
    defun on_restored_from_files(_data :: Keyspaces.t) :: any do
      :ok
    end
  end

  #
  # API
  #
  defun add_consensus_group() :: :ok | {:error, :already_added} do
    rv_options = [
      heartbeat_timeout:                   1_000,
      election_timeout:                    2_000,
      election_timeout_clock_drift_margin: 1_000,
      leader_hook_module:                  Hook,
    ]
    rv_config = RaftedValue.make_config(__MODULE__, rv_options)
    RaftFleet.add_consensus_group(__MODULE__, 3, rv_config)
  end

  defun register(keyspace_name :: v[atom], policy :: v[SplitMergePolicy.t]) :: :ok | {:error, :already_added} do
    {:ok, r} = RaftFleet.command(__MODULE__, {:register, keyspace_name, policy})
    r
  end

  defun submit_stats(map :: %{atom => %{Hash.t => {non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer}}}) :: :ok do
    if not Enum.empty?(map) do
      threshold_time = System.system_time(:millisecond) - Config.shard_ineligible_period_after_split_or_merge()
      {:ok, _} = RaftFleet.command(__MODULE__, {:stats, map, threshold_time})
    end
    :ok
  end

  defun workflow_fetch_from_local_leader() :: v[nil | Workflow.t] do
    now = System.system_time(:millisecond)
    lock_period = Config.workflow_lock_period()
    case RaftedValue.command(__MODULE__, {:workflow_fetch, Node.self(), now, lock_period}) do
      {:ok, :locked}                  -> nil
      {:ok, w}                        -> w
      {:error, _not_leader_or_noproc} -> nil
    end
  end

  defun workflow_proceed(pair :: v[Workflow.t]) :: v[nil | Workflow.t] do
    {:ok, w} = RaftFleet.command(__MODULE__, {:workflow_proceed, pair, Node.self(), System.system_time(:millisecond)})
    w
  end

  defun workflow_abort(pair :: v[Workflow.t]) :: :ok do
    {:ok, :ok} = RaftFleet.command(__MODULE__, {:workflow_abort, pair})
    :ok
  end
end
