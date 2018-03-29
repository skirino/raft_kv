use Croma

defmodule RaftKV.StatsReporter do
  use GenServer
  alias RaftKV.{Config, StatsCollector, Keyspaces}

  defun start_link([]) :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    start_timer()
    {:ok, nil}
  end

  @impl true
  def handle_info(:timeout, _state) do
    start_timer()
    Keyspaces.submit_stats(StatsCollector.get_all())
    {:noreply, nil}
  end
  def handle_info(_delayed_reply, state) do
    {:noreply, state}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, Config.stats_collection_interval())
  end
end
