use Croma

defmodule RaftKV.Table do
  alias RaftKV.Hash

  @table :raft_kv_table

  defun create() :: :ok do
    :ets.new(@table, [:ordered_set, :public, :named_table, {:read_concurrency, true}])
    :ok
  end

  defun insert(ks_name :: v[atom], range_start :: v[Hash.t]) :: :ok do
    :ets.insert(@table, {{ks_name, range_start}})
    :ok
  end

  defun delete(ks_name :: v[atom], range_start :: v[Hash.t]) :: :ok do
    :ets.delete(@table, {ks_name, range_start})
    :ok
  end

  defun lookup(ks_name :: v[atom], key :: any) :: v[Hash.t] do
    case :ets.prev(@table, {ks_name, Hash.from_key(ks_name, key) + 1}) do
      {^ks_name, range_start} -> range_start
      :"$end_of_table"        -> raise "Keyspace #{ks_name} is not initialized"
    end
  end

  defun ensure_record_created(ks_name :: v[atom], range_start :: v[Hash.t]) :: :ok do
    case :ets.lookup(@table, {ks_name, range_start}) do
      []  -> insert(ks_name, range_start)
      [_] -> :ok
    end
  end

  defun traverse_keyspace_shards(ks_name :: v[atom], acc :: a, f :: (({atom, Hash.t}, a) -> a)) :: a when a: any do
    traverse_keyspace_shards_impl(ks_name, {ks_name, -1}, acc, f)
  end

  defp traverse_keyspace_shards_impl(ks_name, ets_key1, acc, f) do
    case :ets.next(@table, ets_key1) do
      :"$end_of_table"         -> acc
      {^ks_name, _} = ets_key2 -> traverse_keyspace_shards_impl(ks_name, ets_key2, f.(ets_key2, acc), f)
      {_other_ks, _}           -> acc
    end
  end

  defun traverse_all_keyspace_shards(acc :: a, f :: (({atom, Hash.t}, a) -> a)) :: a when a: any do
    # Note taht all ETS keys are `{ks_name, range_start}` and `nil < {x, y}` always holds in Erlang term order.
    traverse_all_keyspace_shards_impl(nil, acc, f)
  end

  defp traverse_all_keyspace_shards_impl(ets_key1, acc, f) do
    case :ets.next(@table, ets_key1) do
      :"$end_of_table" -> acc
      ets_key2         -> traverse_all_keyspace_shards_impl(ets_key2, f.(ets_key2, acc), f)
    end
  end
end
