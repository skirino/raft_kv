use Croma

defmodule RaftKV.ValuePerKey do
  @moduledoc """
  Behaviour module to define interface functions to manipulate stored value for each key.

  The implementations of `c:command/4` and `c:query/4` must be pure (i.e., they must consist of only deterministic computations).
  To introduce side effects for your key-value operations see `RaftKV.LeaderHook`.

  See also `RaftedValue.Data`.
  """

  @type key         :: any
  @type value       :: any
  @type command_arg :: any
  @type command_ret :: any
  @type query_arg   :: any
  @type query_ret   :: any
  @type load        :: non_neg_integer
  @type size        :: non_neg_integer

  @doc """
  Generic read/write operation on the stored value.

  This callback function is invoked by `RaftKV.command/4` or `RaftKV.command_on_all_keys_in_shard/3`.
  Commands are replicated across members of the consensus group and executed in all members
  in order to reproduce the same value in all nodes.

  The callback function must return a 4-tuple.

  - 0th element : Return value for the caller of `RaftKV.command/4`.
  - 1st element : Approximate load (in an arbitrary unit) required by execution of the command.
  - 2nd element : The next version of the value after the command. If you return `nil` the key is removed.
  - 3rd element : Size of the next version of the value (in an arbitrary unit). Neglected if you specify `nil` for the 3rd element.
  """
  @callback command(nil | value, size, key, command_arg) :: {command_ret, load, nil | value, size}

  @doc """
  Read-only operation on the stored value.

  This callback function is invoked by `RaftKV.query/4`.
  This function must return a 2-tuple.

  - 0th element : Return value for the caller of `RaftKV.query/4`.
  - 1st element : Approximate load (in an arbitrary unit) required by execution of the query.
    Note that (most of the time) read-only queries can bypass the Raft log replication (which is necessary in the case of commands),
    thanks to leader leases in the Raft protocol.
    Load values to return in `c:command/4` and `c:query/4` should reflect this difference.
  """
  @callback query(value, size, key, query_arg) :: {query_ret, load}
end
