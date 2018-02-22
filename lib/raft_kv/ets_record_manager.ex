use Croma

defmodule RaftKV.EtsRecordManager do
  use GenServer
  alias RaftKV.{Hash, Table}

  @interval 5 * 60_000

  defun start_link([]) :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  @impl true
  def init(:ok) do
    {:ok, nil}
  end

  @impl true
  def handle_call(msg, _from, nil) do
    handle(msg)
    {:reply, :ok, nil}
  end

  @impl true
  def handle_cast(msg, nil) do
    handle(msg)
    {:noreply, nil}
  end

  defp handle({:ensure_record_created, ks_name, range_start}) do
    Table.ensure_record_created(ks_name, range_start)
  end
  defp handle({:ensure_record_removed, ks_name, range_start}) do
    Table.delete(ks_name, range_start)
  end
  defp handle({:delete_all, ks_name}) do
    Table.traverse_keyspace_ranges(ks_name, [], &[&1 | &2])
    |> Enum.each(fn {^ks_name, range_start} -> Table.delete(ks_name, range_start) end)
  end

  @impl true
  def handle_info(:timeout, nil) do
    # Periodically fetches all keyspace names and ensures that, for each keyspace name, at least one record exists in the local ETS table.
    # This is not necessary as long as no message is lost; however we need this because
    # notifications from `Keyspaces` leader hook may be lost due to e.g. temporary network issue.
    start_timer()
    RaftKV.list_keyspaces() |> Enum.each(&Table.ensure_record_created(&1, 0))
    {:noreply, nil}
  end
  def handle_info(_delayed_reply, nil) do
    {:noreply, nil}
  end

  defp start_timer() do
    Process.send_after(self(), :timeout, @interval)
  end

  #
  # API
  #
  defun ensure_record_created(ks_name :: v[atom], range_start :: v[Hash.t]) :: :ok do
    GenServer.call(__MODULE__, {:ensure_record_created, ks_name, range_start})
  end

  defun broadcast_new_keyspace(ks_name :: v[atom]) :: :ok do
    # Best effort messaging; we don't have to fetch `RaftFleet.active_nodes()` here.
    GenServer.abcast(__MODULE__, {:ensure_record_created, ks_name, 0})
    :ok
  end

  def ensure_record_changed_in_all_relevant_nodes!(msg) do
    active_nodes = RaftFleet.active_nodes() |> Enum.flat_map(fn {_z, ns} -> ns end)
    relevant_nodes = Enum.uniq([Node.self() | Node.list()] ++ active_nodes)
    {_replies, []} = GenServer.multi_call(relevant_nodes, __MODULE__, msg, 5_000)
    :ok
  end
end
