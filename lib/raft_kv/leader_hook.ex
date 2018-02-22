use Croma

defmodule RaftKV.LeaderHook do
  alias RaftKV.ValuePerKey

  @callback on_command_committed(value_before :: nil | ValuePerKey.value,
                                 size_before  :: ValuePerKey.size,
                                 key          :: ValuePerKey.key,
                                 arg          :: ValuePerKey.command_arg,
                                 ret          :: ValuePerKey.command_ret,
                                 value_after  :: nil | ValuePerKey.value,
                                 size_after   :: ValuePerKey.size) :: any
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



