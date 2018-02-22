use Croma

defmodule RaftKV.LoadAccumulator do
  use GenServer
  alias RaftKV.Hash

  defun start_link([]) :: GenServer.on_start do
    GenServer.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  @impl true
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call(:get_all, _from, map) do
    {:reply, map, %{}}
  end

  @impl true
  def handle_cast({ks_name, range_start, load_or_knf}, nested_map) do
    pair =
      case load_or_knf do
        :key_not_found -> {0   , 1}
        load           -> {load, 0}
      end
    submap =
      case Map.get(nested_map, ks_name) do
        nil -> %{range_start => pair}
        m   -> Map.update(m, range_start, pair, &pair_add(&1, pair))
      end
    {:noreply, Map.put(nested_map, ks_name, submap)}
  end

  defp pair_add({l1, knf1}, {l2, knf2}), do: {l1 + l2, knf1 + knf2}

  #
  # API
  #
  defun get_all() :: %{atom => {non_neg_integer, non_neg_integer}} do
    GenServer.call(__MODULE__, :get_all)
  end

  defun send_load(ks_name :: v[atom], range_start :: v[Hash.t], load_or_knf :: v[pos_integer | :key_not_found]) :: :ok do
    GenServer.cast(__MODULE__, {ks_name, range_start, load_or_knf})
  end
end
