use Croma

defmodule RaftKV do
  @moduledoc """
  Documentation for `RaftKV`.
  """

  alias Croma.Result, as: R
  alias RaftKV.{Hash, Table, Keyspaces, Shard, SplitMergePolicy, ValuePerKey, EtsRecordManager, Config}

  @doc """
  """
  defun init() :: :ok do
    case Keyspaces.add_consensus_group() do
      :ok                      -> :ok
      {:error, :already_added} ->
        {:ok, kss} = RaftFleet.query(Keyspaces, :all_keyspace_shards)
        Enum.each(kss, fn {ks_name, range_starts} ->
          Enum.each(range_starts, &Table.insert(ks_name, &1))
        end)
    end
  end

  @doc """
  """
  defun register_keyspace(keyspace_name     :: g[atom],
                          rv_config_options :: Keyword.t,
                          data_module       :: g[module],
                          hook_module       :: v[nil | module],
                          policy            :: SplitMergePolicy.t) :: :ok | {:error, :invalid_policy | :already_registered} do
    with true <- SplitMergePolicy.valid?(policy),
         :ok  <- Keyspaces.register(keyspace_name, policy),
         :ok  <- add_1st_consensus_group(keyspace_name, rv_config_options),
         :ok  <- Shard.initialize_1st_shard(keyspace_name, data_module, hook_module, 0, Hash.upper_bound()) do
      EtsRecordManager.ensure_record_created(keyspace_name, 0)
    else
      false                                                        -> {:error, :invalid_policy}
      {:error, a} when a in [:already_added, :already_initialized] -> {:error, :already_registered}
    end
  end

  defpt add_1st_consensus_group(keyspace_name, rv_config_options) do
    rv_config =
      RaftedValue.make_config(Shard, rv_config_options)
      |> Map.put(:leader_hook_module, Shard.Hook)
    RaftFleet.add_consensus_group(Shard.consensus_group_name(keyspace_name, 0), 3, rv_config)
  end

  @doc """
  """
  defun deregister_keyspace(keyspace_name :: g[atom]) :: :ok | {:error, :no_such_keyspace} do
    {:ok, r} = RaftFleet.command(Keyspaces, {:deregister, keyspace_name})
    r
  end

  @doc """
  """
  defun list_keyspaces() :: [atom] do
    {:ok, ks_names} = RaftFleet.query(Keyspaces, :keyspace_names)
    ks_names
  end

  @doc """
  """
  defun get_keyspace_policy(keyspace_name :: g[atom]) :: v[SplitMergePolicy.t] do
    {:ok, policy} = RaftFleet.query(Keyspaces, {:get_policy, keyspace_name})
    policy
  end

  @doc """
  """
  defun set_keyspace_policy(keyspace_name :: g[atom], policy :: v[SplitMergePolicy.t]) :: :ok | {:error, :no_such_keyspace} do
    {:ok, r} = RaftFleet.command(Keyspaces, {:set_policy, keyspace_name, policy})
    r
  end

  @doc """
  """
  defun command(keyspace_name :: g[atom], key :: ValuePerKey.key, command_arg :: ValuePerKey.command_arg) :: R.t(ValuePerKey.command_ret) do
    call_impl(keyspace_name, key, fn cg_name ->
      RaftFleet.command(cg_name, {:c, key, command_arg})
    end)
  end

  @doc """
  """
  defun query(keyspace_name :: g[atom], key :: ValuePerKey.key, query_arg :: ValuePerKey.query_arg) :: R.t(ValuePerKey.query_ret) do
    call_impl(keyspace_name, key, fn cg_name ->
      RaftFleet.query(cg_name, {:q, key, query_arg})
    end)
  end

  defp call_impl(keyspace_name, key, f, attempts \\ 0) do
    range_start = Table.lookup(keyspace_name, key)
    cg_name = Shard.consensus_group_name(keyspace_name, range_start)
    case f.(cg_name) do
      {:error, _reason} = e -> e
      {:ok, result}   ->
        case result do
          {:ok, ret, _load} ->
            {:ok, ret}
          {:error, {:below_range, _start}} ->
            # The shard should have already been merged; the marker in ETS is already stale.
            Table.delete(keyspace_name, range_start)
            call_impl(keyspace_name, key, f)
          {:error, {:above_range, range_end}} ->
            # The shard should have already been split; we have to make a new marker for the newly-created shard.
            Table.insert(keyspace_name, range_end)
            call_impl(keyspace_name, key, f)
          {:error, :will_own_the_key_retry_afterward} ->
            # The shard has not yet shift its range; retry after a sleep
            if attempts >= Config.max_retries() do
              {:error, {:timeout_waiting_for_completion_of_split_or_merge, cg_name}}
            else
              :timer.sleep(Config.sleep_duration_before_retry)
              call_impl(keyspace_name, key, f, attempts + 1)
            end
          {:error, :key_not_found} = e ->
            e
        end
    end
  end
end
