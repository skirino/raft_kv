use Croma

defmodule RaftKV.WorkflowExecutor do
  use GenServer
  alias RaftKV.{Keyspaces, Workflow, Config}

  defun start_link([]) :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  @impl true
  def init(:ok) do
    start_timer()
    {:ok, nil}
  end

  @impl true
  def handle_info(:timeout, nil) do
    start_timer()
    case Keyspaces.workflow_fetch_from_local_leader() do
      nil  -> :ok
      task -> Workflow.execute(task)
    end
    {:noreply, nil}
  end
  def handle_info(_delayed_reply, nil) do
    {:noreply, nil}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, Config.workflow_execution_interval)
  end
end
