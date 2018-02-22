use Croma

defmodule RaftKV.ValuePerKey do
  @type key         :: any
  @type value       :: any
  @type command_arg :: any
  @type command_ret :: any
  @type query_arg   :: any
  @type query_ret   :: any
  @type load        :: non_neg_integer
  @type size        :: non_neg_integer

  @callback command(value, size, key, command_arg) :: {command_ret, load, size, nil | value}
  @callback query(value, size, key, query_arg) :: {query_ret, load}
end

if Mix.env() in [:dev, :test] do
  defmodule KV do
    alias RaftKV.ValuePerKey
    @behaviour ValuePerKey

    @impl true
    def command(_previous_value, _size, _key, :unset) do
      {:ok, 5, 0, nil}
    end
    def command(_previous_value, _size, _key, {:set, value}) do
      {:ok, 5, byte_size(value), value}
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
end
