use Croma

defmodule RaftKV.SizeCollector do
  use GenServer
  alias RaftKV.{Hash, Table, Shard}

  defmodule Fetcher do
    def run() do
      size_map = Table.traverse_all_keyspace_shards(%{}, &collect_from_local_member/2)
      exit({:shutdown, size_map})
    end

    defp collect_from_local_member({ks_name, range_start}, acc) do
      cg_name = Shard.consensus_group_name(ks_name, range_start)
      case RaftedValue.query(cg_name, :get_total_size) do
        {:ok, {:ok, n_keys, total_size}} ->
          submap = Map.get(acc, ks_name, %{}) |> Map.put(range_start, {n_keys, total_size})
          Map.put(acc, ks_name, submap)
        {:ok, {:error, _uninitialized_or_post_merge_latter}} ->
          acc
        {:error, _not_leader_or_noproc} ->
          acc
      end
    end
  end

  defun start_link([]) :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  @impl true
  def init(:ok) do
    {:ok, %{pid: nil, map: %{}}}
  end

  @impl true
  def handle_call(:get_all, _from, %{pid: pid, map: map} = state) do
    new_pid =
      case pid do
        nil -> {p, _ref} = spawn_monitor(Fetcher, :run, []); p
        _   -> pid
      end
    {:reply, map, %{state | pid: new_pid}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, {:shutdown, map}}, %{pid: pid} = state) do
    {:noreply, %{state | pid: nil, map: map}}
  end

  #
  # API
  #
  defun get_all() :: %{atom => %{Hash.t => {non_neg_integer, non_neg_integer}}} do
    GenServer.call(__MODULE__, :get_all)
  end
end
