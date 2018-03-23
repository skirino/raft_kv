ExUnit.start()
case RaftFleet.activate("zone") do
  :ok                     -> :timer.sleep(100)
  {:error, :not_inactive} -> :ok
end

defmodule KV do
  alias RaftKV.ValuePerKey
  @behaviour ValuePerKey

  @impl true
  def command(_previous_value, _size, _key, {:set, value}) do
    {:ok, 5, value, byte_size(value)}
  end
  def command(_previous_value, _size, _key, :unset) do
    {:ok, 5, nil, 0}
  end
  def command(value, _size, _key, :append_zero) do
    new_value = value <> "0"
    {:ok, 5, new_value, byte_size(new_value)}
  end

  @impl true
  def query(value, _size, _key, :get) do
    {value, 1}
  end

  #
  # API
  #
  def get(k) do
    case RaftKV.query(:kv, k, :get) do
      {:ok, v}                 -> v
      {:error, :key_not_found} -> nil
    end
  end

  def set(k, v) do
    {:ok, :ok} = RaftKV.command(:kv, k, {:set, v})
    :ok
  end

  def unset(k) do
    {:ok, :ok} = RaftKV.command(:kv, k, :unset)
    :ok
  end
end

defmodule Hook do
  @behaviour RaftKV.LeaderHook

  @impl true
  def on_command_committed(_data_before, _size_before, _key, _arg, _ret, _data_after, _size_after) do
  end

  @impl true
  def on_query_answered(_data, _size, _key, _arg, _ret) do
  end
end
