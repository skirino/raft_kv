use Croma

defmodule RaftKV.LeaderHook do
  @moduledoc """
  Behaviour module for hooks that are invoked in leader of a consensus group.

  Note that there are cases where hooks are invoked multiple times for a single event due to leader change.
  """

  alias RaftKV.ValuePerKey

  @doc """
  Hook to be called when a command given by `RaftKV.command/4` or `RaftKV.command_on_all_keys_in_shard/3` is executed.
  """
  @callback on_command_committed(value_before :: nil | ValuePerKey.value,
                                 size_before  :: ValuePerKey.size,
                                 key          :: ValuePerKey.key,
                                 arg          :: ValuePerKey.command_arg,
                                 ret          :: ValuePerKey.command_ret,
                                 value_after  :: nil | ValuePerKey.value,
                                 size_after   :: ValuePerKey.size) :: any

  @doc """
  Hook to be called when a query given by `RaftKV.query/4` is executed.
  """
  @callback on_query_answered(value :: ValuePerKey.value,
                              size  :: ValuePerKey.size,
                              key   :: ValuePerKey.key,
                              arg   :: ValuePerKey.query_arg,
                              ret   :: ValuePerKey.query_ret) :: any
end

if Mix.env() in [:dev, :test] do
  defmodule Hook do
    @behaviour RaftKV.LeaderHook

    @impl true
    def on_command_committed(_data_before, _size_before, _key, _arg, _ret, _data_after, _size_after) do
    end

    @impl true
    def on_query_answered(_data, _size, _key, _arg, _ret) do
    end
  end
end



