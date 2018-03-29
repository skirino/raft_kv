use Croma

defmodule RaftKV.Application do
  use Application
  alias RaftKV.Table

  def start(_type, _args) do
    Table.create()
    children = [
      RaftKV.EtsRecordManager,
      RaftKV.WorkflowExecutor,
      RaftKV.StatsCollector,
      RaftKV.StatsReporter,
    ]
    Supervisor.start_link(children, [strategy: :one_for_one])
  end
end
