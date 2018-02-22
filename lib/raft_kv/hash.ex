use Croma

defmodule RaftKV.Hash do
  @upper_bound 134217728 # 2^27
  defun upper_bound() :: pos_integer, do: @upper_bound

  use Croma.SubtypeOfInt, min: 0, max: (@upper_bound - 1)

  defun from_key(name :: v[atom], key :: any) :: t do
    :erlang.phash2({name, key}, @upper_bound)
  end
end
