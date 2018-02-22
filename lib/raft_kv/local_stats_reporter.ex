use Croma

defmodule RaftKV.LocalStatsReporter do
  use GenServer
  alias RaftKV.{Config, SizeCollector, LoadAccumulator, Keyspaces}

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
    size_map = SizeCollector.get_all()
    load_map = LoadAccumulator.get_all()
    Keyspaces.submit_stats(size_map, load_map)
    {:noreply, nil}
  end
  def handle_info(_delayed_reply, state) do
    {:noreply, state}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, Config.stats_collection_interval())
  end
end
