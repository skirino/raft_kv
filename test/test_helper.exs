ExUnit.start()
case RaftFleet.activate("zone") do
  :ok                     -> :timer.sleep(100)
  {:error, :not_inactive} -> :ok
end

defmodule KVBase do
  defmacro __using__([keyspace_name: ks]) do
    quote bind_quoted: [ks: ks] do
      @ks ks

      alias RaftKV.ValuePerKey
      @behaviour ValuePerKey

      @impl true
      def command(_previous_value, _size, _key, {:set, value}) do
        {:ok, 5, value, 1}
      end
      def command(_previous_value, _size, _key, :unset) do
        {:ok, 5, nil, 0}
      end
      def command(value, _size, _key, :inc) do
        {:ok, 5, value + 1, 1}
      end

      @impl true
      def query(value, _size, _key, :get) do
        {value, 1}
      end

      #
      # API
      #
      def get(k) do
        case RaftKV.query(@ks, k, :get) do
          {:ok, v}                 -> v
          {:error, :key_not_found} -> nil
        end
      end

      def set(k, v) do
        {:ok, :ok} = RaftKV.command(@ks, k, {:set, v})
        :ok
      end

      def unset(k) do
        {:ok, :ok} = RaftKV.command(@ks, k, :unset)
        :ok
      end

      def inc(k) do
        {:ok, :ok} = RaftKV.command(@ks, k, :inc)
        :ok
      end
    end
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
