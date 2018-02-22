use Croma

defmodule RaftKV.Config do
  @default_stats_collection_interval   5_000
  @default_workflow_execution_interval 5_000

  @moduledoc """
  """

  defun stats_collection_interval() :: pos_integer do
    Application.get_env(:raft_kv, :stats_collection_interval, @default_stats_collection_interval)
  end

  defun workflow_execution_interval() :: pos_integer do
    Application.get_env(:raft_kv, :workflow_execution_interval, @default_workflow_execution_interval)
  end
end
