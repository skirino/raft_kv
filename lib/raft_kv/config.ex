use Croma

defmodule RaftKV.Config do
  @default_stats_collection_interval                    (if Mix.env() == :test, do: 2_000, else: 60_000)
  @default_workflow_execution_interval                  (if Mix.env() == :test, do: 2_000, else: 60_000)
  @default_workflow_lock_period                         (if Mix.env() == :test, do: 2_000, else: 30_000)
  @default_shard_ineligible_period_after_split_or_merge 2 * @default_stats_collection_interval

  @moduledoc """
  `:raft_kv` defines the following application configs:

  - `:stats_collection_interval`:
      - Interval (in milliseconds) between collections of the following metrics of all shards:
          - Number of keys in a shard
          - Aggregated size of all keys in a shard
          - Aggregated load which a shard has experienced since the last stats collection
      - By using smaller value you can adjust (split/merge) number of shards more quickly, with higher overhead.
        Defaults to `#{@default_stats_collection_interval}`.
  - `:workflow_execution_interval`:
      - Interval (in milliseconds) between executions of workflow tasks.
        By using smaller value you can execute workflow tasks more quickly, with higher overhead.
        Defaults to `#{@default_workflow_execution_interval}`.
        Workflow task here means:
          - removing a keyspace
          - splitting 1 shard into 2
          - merging 2 consecutive shards into 1
  - `:workflow_lock_period`:
      - When executing a workflow task it is locked for this period (in milliseconds) in order to avoid running the same task simultaneously.
        Defaults to `#{@default_workflow_lock_period}`.
  - `:shard_ineligible_period_after_split_or_merge`:
      - When a shard has just been split/merged, stats of the affected shard(s) become stale.
        To prevent from incorrectly splitting/merging based on the stale stats, shards that have been split/merged within
        this period (in milliseconds) are excluded from next split/merge candidates.
        Defaults to `#{@default_shard_ineligible_period_after_split_or_merge}`.

  Note that each `raft_kv` process uses application configs stored in the local node.
  If you want to configure the options above you must set them on all nodes in your cluster.

  In addition to the configurations above, the following configurations defined by the underlying libraries are also available:

  - `RaftedValue.make_config/2` and `RaftedValue.change_config/2`.
  - `RaftFleet.Config`.

  ## About `:rafted_value_config_maker` option for `:raft_fleet`

  `:raft_fleet` provides a way to configure each consensus group by setting an implementation of `RaftFleet.RaftedValueConfigMaker` behaviour
  as `:rafted_value_config_maker` option.
  `:raft_kv` respects this option; it uses (if any) the provided callback module when creating a `t:RaftedValue.Config.t/0`.
  `:raft_kv` defines the following consensus groups:

  - `RaftKV.Keyspaces`
  - Each shard is impelemented as a Raft consensus group whose name is `:"\#{keyspace_name}_\#{hash_integer}"`

  To construct `t:RaftedValue.Config.t/0`s for these consensus groups in your implementation of
  `RaftFleet.RaftedValueConfigMaker` behaviour, you can use `RaftKV.Keyspaces.make_rv_config/1` and `RaftKV.Shard.make_rv_config/1`.

  As a result implementations of `RaftFleet.RaftedValueConfigMaker` behaviour should look like the following:

      defmodule ConfigMaker do
        @behaviour RaftFleet.RaftedValueConfigMaker

        @options [heartbeat_timeout: 500]

        @impl true
        def make(name) do
          case Atom.to_string(name) do
            "Elixir.RaftFleet.Cluster"   -> RaftFleet.Cluster.make_rv_config(@options) # consensus group defined by `:raft_fleet` itself
            "Elixir.RaftKV.Keyspaces"    -> RaftKV.Keyspaces.make_rv_config(@options)
            "some_keyspace_"    <> _hash -> RaftKV.Shard.make_rv_config(@options)
            "another_keyspace_" <> _hash -> RaftKV.Shard.make_rv_config(@options)
          end
        end
      end
  """

  defun stats_collection_interval() :: pos_integer do
    Application.get_env(:raft_kv, :stats_collection_interval, @default_stats_collection_interval)
  end

  defun workflow_execution_interval() :: pos_integer do
    Application.get_env(:raft_kv, :workflow_execution_interval, @default_workflow_execution_interval)
  end

  defun workflow_lock_period() :: pos_integer do
    Application.get_env(:raft_kv, :workflow_lock_period, @default_workflow_lock_period)
  end

  defun shard_ineligible_period_after_split_or_merge() :: pos_integer do
    Application.get_env(:raft_kv, :shard_ineligible_period_after_split_or_merge, @default_shard_ineligible_period_after_split_or_merge)
  end
end
