use Croma

defmodule RaftKV do
  @moduledoc File.read!(Path.join([__DIR__, "..", "README.md"])) |> String.replace_prefix("# RaftKV\n\n", "")

  alias RaftKV.{Hash, Table, Keyspaces, Shard, SplitMergePolicy, ValuePerKey, EtsRecordManager}

  @doc """
  Initializes `:raft_kv` so that processes in `Node.self()` can interact with appropriate shard(s) for command/query.

  `:raft_kv` heavily depends on `:raft_fleet` and thus it's necessary to call `RaftFleet.activate/1`
  before calling this function.
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
    EtsRecordManager.init()
  end

  #
  # manipulating keyspaces
  #
  @doc """
  Registers a keyspace with the given arguments.

  Parameters:
  - `keyspace_name`:
    An atom that identifies the new keyspace.
  - `rv_config_options`:
    An optional keyword list of options passed to `RaftKV.Shard.make_rv_config/1`.
    Regardless of this argument is given or not, if `:rafted_value_config_maker` option for `:raft_fleet`
    is provided, `:raft_kv` uses it to create a `t:RaftedValue.Config.t/0` (in this case the argument is not used).
    If `:rafted_value_config_maker` option does not exist, then falls back to `RaftKV.Shard.make_rv_config/1`.
    The created `t:RaftedValue.Config.t/0` is used by all consensus groups in the newly-registered keyspace.
    It's recommended that you omit this argument and provide your implementation of `RaftFleet.RaftedValueConfigMaker` behaviour.
  - `data_module`:
    A callback module that implements `RaftKV.ValuePerKey` behaviour.
  - `hook_module`:
    A callback module that implements `RaftKV.LeaderHook` behaviour (or `nil` if you don't use hook).
  - `policy`:
    `t:RaftKV.SplitMergePolicy.t/0` that specifies the conditions on which shards are split/merged.
    See also `RaftKV.SplitMergePolicy`.
  """
  defun register_keyspace(keyspace_name     :: g[atom],
                          rv_config_options :: nil | Keyword.t \\ nil,
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
    name = Shard.consensus_group_name(keyspace_name, 0)
    rv_config =
      case RaftFleet.Config.rafted_value_config_maker() do
        nil ->
          case rv_config_options do
            nil  -> Shard.make_rv_config()
            opts -> Shard.make_rv_config(opts)
          end
        mod -> mod.make(name)
      end
    RaftFleet.add_consensus_group(name, 3, rv_config)
  end

  @doc """
  Removes an existing keyspace.

  Associated resources (processes, ETS records, etc.) for the keyspace are not removed immediately;
  they are removed by a background worker process.
  """
  defun deregister_keyspace(keyspace_name :: g[atom]) :: :ok | {:error, :no_such_keyspace} do
    {:ok, r} = RaftFleet.command(Keyspaces, {:deregister, keyspace_name})
    r
  end

  @doc """
  List all registered keyspace names.
  """
  defun list_keyspaces() :: [atom] do
    {:ok, ks_names} = RaftFleet.query(Keyspaces, :keyspace_names)
    ks_names
  end

  @doc """
  Retrieves the current value of `t:RaftKV.SplitMergePolicy.t/0` used for the specified keyspace.
  """
  defun get_keyspace_policy(keyspace_name :: g[atom]) :: v[nil | SplitMergePolicy.t] do
    {:ok, policy} = RaftFleet.query(Keyspaces, {:get_policy, keyspace_name})
    policy
  end

  @doc """
  Replaces the current `t:RaftKV.SplitMergePolicy.t/0` of a keyspace with the specified one.
  """
  defun set_keyspace_policy(keyspace_name :: g[atom], policy :: SplitMergePolicy.t) :: :ok | {:error, :invalid_policy | :no_such_keyspace} do
    if SplitMergePolicy.valid?(policy) do
      {:ok, r} = RaftFleet.command(Keyspaces, {:set_policy, keyspace_name, policy})
      r
    else
      {:error, :invalid_policy}
    end
  end

  #
  # command & query
  #
  @default_timeout                   500
  @default_retry                     3
  @default_retry_interval            1_000
  @default_call_module               :gen_statem
  @default_shard_lock_retry          3
  @default_shard_lock_retry_interval 200

  @typedoc """
  Options for `command/4`, `query/4` and `command_on_all_keys_in_shard/3`.

  - `:timeout`, `:retry`, `:retry_interval`, `:call_module` are directly passed to `RaftFleet.command/5` or `RaftFleet.query/5`.
  - `:shard_lock_retry` and `:shard_lock_retry_interval` are intended to mask temporary unavailability of a shard
    due to an ongoing splitting/merging.

  Default values:

  - `:timeout`                   : `#{@default_timeout}`
  - `:retry`                     : `#{@default_retry}`
  - `:retry_interval`            : `#{@default_retry_interval}`
  - `:call_module`               : `#{inspect(@default_call_module)}`
  - `:shard_lock_retry`          : `#{@default_shard_lock_retry}`
  - `:shard_lock_retry_interval` : `#{@default_shard_lock_retry_interval}`
  """
  @type option :: {:timeout                   , pos_integer    }
                | {:retry                     , non_neg_integer}
                | {:retry_interval            , pos_integer    }
                | {:call_module               , module         }
                | {:range_shift_retry         , non_neg_integer}
                | {:range_shift_retry_interval, pos_integer    }

  @typedoc """
  Error reason returned by `command/4` and `query/4` when a shard is being
  initialized/split/merged and, after some retries, the call failed due to
  the temporary unavailability.

  This should be a rare error case.
  Note that you can change number of retries and retry intervals
  by `:shard_lock_retry` and `:shard_lock_retry_interval` of `t:option/0`.
  """
  @type shard_timeout :: {:timeout, :waiting_for_shard_state_transition, consensus_group_name :: atom}

  @doc """
  Executes a command on the replicated value identified by `keyspace_name` and `key`.

  See also `RaftKV.ValuePerKey`, `RaftFleet.command/6` and `RaftedValue.command/5`.
  """
  defun command(keyspace_name :: g[atom],
                key           :: ValuePerKey.key,
                command_arg   :: ValuePerKey.command_arg,
                options       :: [option] \\ []) :: {:ok, ValuePerKey.command_ret} | {:error, :no_leader | shard_timeout} do
    timeout        = Keyword.get(options, :timeout       , @default_timeout       )
    retry          = Keyword.get(options, :retry         , @default_retry         )
    retry_interval = Keyword.get(options, :retry_interval, @default_retry_interval)
    call_module    = Keyword.get(options, :call_module   , @default_call_module   )
    call_impl(keyspace_name, key, options, fn cg_name ->
      RaftFleet.command(cg_name, {:c, key, command_arg}, timeout, retry, retry_interval, call_module)
    end)
  end

  @doc """
  Executes a read-only query on the replicated value identified by `keyspace_name` and `key`.

  See also `RaftKV.ValuePerKey`, `RaftFleet.query/6` and `RaftedValue.query/4`.
  """
  defun query(keyspace_name :: g[atom],
              key           :: ValuePerKey.key,
              query_arg     :: ValuePerKey.query_arg,
              options       :: [option] \\ []) :: {:ok, ValuePerKey.query_ret} | {:error, :key_not_found | :no_leader | shard_timeout} do
    timeout        = Keyword.get(options, :timeout       , @default_timeout       )
    retry          = Keyword.get(options, :retry         , @default_retry         )
    retry_interval = Keyword.get(options, :retry_interval, @default_retry_interval)
    call_module    = Keyword.get(options, :call_module   , @default_call_module   )
    call_impl(keyspace_name, key, options, fn cg_name ->
      RaftFleet.query(cg_name, {:q, key, query_arg}, timeout, retry, retry_interval, call_module)
    end)
  end

  defp call_impl(ks_name, key, options, f, attempts \\ 0) do
    range_start = Table.lookup(ks_name, key)
    cg_name = Shard.consensus_group_name(ks_name, range_start)
    case f.(cg_name) do
      {:error, _reason} = e   -> e
      {:ok, {:ok, _ret} = ok} -> ok
      {:ok, {:error, reason}} ->
        case reason do
          :key_not_found                    -> {:error, :key_not_found} # Only for queries
          {:below_range, _start}            -> delete_record_and_retry(ks_name, range_start, key, options, f)
          {:above_range, range_end}         -> insert_record_and_retry(ks_name, range_end, key, options, f)
          :uninitialized                    -> retry_after_sleep(ks_name, cg_name, key, options, f, attempts)
          :will_own_the_key_retry_afterward -> retry_after_sleep(ks_name, cg_name, key, options, f, attempts)
        end
    end
  end

  defp delete_record_and_retry(keyspace_name, range_start, key, options, f) do
    # The shard should have already been merged; the marker in ETS is already stale.
    Table.delete(keyspace_name, range_start)
    call_impl(keyspace_name, key, options, f)
  end

  defp insert_record_and_retry(keyspace_name, range_end, key, options, f) do
    # The shard should have already been split; we have to make a new marker for the newly-created shard.
    Table.insert(keyspace_name, range_end)
    call_impl(keyspace_name, key, options, f)
  end

  defp retry_after_sleep(ks_name, cg_name, key, options, f, attempts) do
    # The shard has not yet initialized or not yet shifted its range; retry after a sleep
    max_retries = Keyword.get(options, :shard_lock_retry, @default_shard_lock_retry)
    if attempts >= max_retries do
      {:error, {:timeout, :waiting_for_shard_state_transition, cg_name}}
    else
      interval = Keyword.get(options, :shard_lock_retry_interval, @default_shard_lock_retry_interval)
      :timer.sleep(interval)
      call_impl(ks_name, key, options, f, attempts + 1)
    end
  end

  #
  # shard-aware API
  #
  @doc """
  (shard-aware API) Traverses all shards in the specified keyspace.
  """
  defun reduce_keyspace_shard_names(keyspace_name :: g[atom], acc :: a, f :: ((atom, a) -> a)) :: a when a: any do
    Table.traverse_keyspace_shards(keyspace_name, acc, fn({_ks_name, range_start}, a) ->
      cg_name = Shard.consensus_group_name(keyspace_name, range_start)
      f.(cg_name, a)
    end)
  end

  @doc """
  (shard-aware API) Fetches all keys in the specified shard.
  """
  defun list_keys_in_shard(shard_name :: g[atom]) :: [ValuePerKey.key] do
    {:ok, {keys1, keys2}} = RaftFleet.query(shard_name, :list_keys)
    keys1 ++ keys2
  end

  @doc """
  (shard-aware API) Executes a command on all existing keys in the specified shard.

  Note that values of `RaftKV.ValuePerKey.command_ret` that were returned by existing keys' command
  are not returned to the caller of this function.
  """
  defun command_on_all_keys_in_shard(shard_name  :: g[atom],
                                     command_arg :: ValuePerKey.command_arg,
                                     options     :: [option] \\ []) :: :ok | {:error, :no_leader} do
    timeout        = Keyword.get(options, :timeout       , @default_timeout       )
    retry          = Keyword.get(options, :retry         , @default_retry         )
    retry_interval = Keyword.get(options, :retry_interval, @default_retry_interval)
    call_module    = Keyword.get(options, :call_module   , @default_call_module   )
    case RaftFleet.command(shard_name, {:all_keys_command, command_arg}, timeout, retry, retry_interval, call_module) do
      {:ok, :ok} -> :ok
      e          -> e
    end
  end
end
