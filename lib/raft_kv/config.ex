use Croma

defmodule RaftKV.Config do
  @default_max_retries                            3
  @default_sleep_duration_before_retry            100
  @default_stats_collection_interval              (if Mix.env() == :test, do: 2_000, else: 60_000)
  @default_workflow_execution_interval            (if Mix.env() == :test, do: 2_000, else: 60_000)
  @default_workflow_lock_period                   (if Mix.env() == :test, do: 2_000, else: 30_000)
  @default_shard_lock_period_after_split_or_merge @default_stats_collection_interval

  @moduledoc """
  `RaftKV` defines the following application configs:

  - `:max_retries`
  - `:sleep_duration_before_retry`
  - `:stats_collection_interval`
  - `:workflow_execution_interval`
  - `:workflow_lock_period`
  - `:shard_lock_period_after_split_or_merge`

  Note that each `raft_kv` processes uses application configs stored in the local node.
  If you want to configure the options above you must set them on all nodes in your cluster.

  In addition to the configurations above, the following configurations defined by the underlying libraries are also available:

  - `RaftedValue.make_config/2` and `RaftedValue.change_config/2`.
  - `RaftFleet.Config`.
  """

  defun max_retries() :: pos_integer do
    Application.get_env(:raft_kv, :max_retries, @default_max_retries)
  end

  defun sleep_duration_before_retry() :: pos_integer do
    Application.get_env(:raft_kv, :sleep_duration_before_retry, @default_sleep_duration_before_retry)
  end

  defun stats_collection_interval() :: pos_integer do
    Application.get_env(:raft_kv, :stats_collection_interval, @default_stats_collection_interval)
  end

  defun workflow_execution_interval() :: pos_integer do
    Application.get_env(:raft_kv, :workflow_execution_interval, @default_workflow_execution_interval)
  end

  defun workflow_lock_period() :: pos_integer do
    Application.get_env(:raft_kv, :workflow_lock_period, @default_workflow_lock_period)
  end

  defun shard_lock_period_after_split_or_merge() :: pos_integer do
    Application.get_env(:raft_kv, :shard_lock_period_after_split_or_merge, @default_shard_lock_period_after_split_or_merge)
  end
end
