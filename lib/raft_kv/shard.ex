use Croma

defmodule RaftKV.Shard do
  alias RaftedValue.Data, as: RVData
  alias RaftKV.Hash

  defmodule Status do
    # This represents state machine state of each shard in the context of splitting/merging protocol.
    #
    # Statuses:
    # - Right after creation, consensus group state is `:uninitialized` instead of a `Shard.t`.
    #   `:uninitialized` consensus group soon becomes either
    #   `:normal` (if it's the 1st shard) or `:pre_split_latter` (if it's created for a shard split)
    # - `:pre_split_former` and `:pre_merge_latter` are the sender side of data;
    #   they must remember all commands that have been applied to data to be transferred.
    # - `:post_split_former` and `:post_merge_latter` don't add new commands but they should keep commands
    #   that had been accumulated during `:pre_split_former` and `:pre_merge_latter`, respectively, in order to facilitate retrying.
    #     - In the case of `:post_split_former` those commands are discarded once it becomes `:normal` (i.e., on completion of splitting).
    #     - A shard (consensus group) in `:post_merge_latter` is simply to be removed.
    #
    # Client interactions:
    # - `:pre_split_latter` and `:post_merge_latter` return errors on receipt of commands/queries.
    #     - `:pre_split_latter` doesn't own any key range yet (will soon start to own the latter half).
    #     - `:post_merge_latter` no longer owns any key range.
    #     - The error handling is implemented by adjusting `range_start` and `range_end` in `Shard.t`, not by looking at `status`.
    # - `:pre_split_latter` and `:pre_merge_former` are the receiver side of data;
    #   on receipt of commands/queries, they return errors that indicate
    #   "I'm not yet allowed to handle your command/query but will soon become the owner of the key. Please retry!".

    alias RaftKV.ValuePerKey
    @type single_key_command :: {:key, key :: ValuePerKey.key, command_arg :: ValuePerKey.command_arg}
    @type all_keys_command   :: {:all_keys, command_arg :: ValuePerKey.command_arg}
    @type command_info       :: single_key_command | all_keys_command

    @type t :: :normal
             | {:pre_split_former , commands :: [command_info]}
             | {:pre_split_latter , range_start_after_split :: Hash.t}
             | {:post_split_former, commands :: [command_info]}
             | {:pre_merge_former , next_range_data :: map}
             | {:pre_merge_latter , commands :: [command_info]}
             | {:post_merge_latter, commands :: [command_info]}

    defun valid?(t :: any) :: boolean do
      :normal                                -> true
      {:pre_split_former , _commands}        -> true
      {:pre_split_latter , _range_start}     -> true
      {:post_split_former, _commands}        -> true
      {:pre_merge_former , _next_range_data} -> true
      {:pre_merge_latter , _commands}        -> true
      {:post_merge_latter, _commands}        -> true
      _                                      -> false
    end

    defun remember_single_key_command(t :: t, which :: :former | :latter, pair :: {ValuePerKey.key, ValuePerKey.command_arg}) :: t do
      ({:pre_split_former, cs}, :latter, {key, arg}) -> {:pre_split_former, [{:key, key, arg} | cs]}
      ({:pre_merge_latter, cs}, _which , {key, arg}) -> {:pre_merge_latter, [{:key, key, arg} | cs]}
      (status                 , _which , _         ) -> status
    end

    defun remember_all_keys_command(t :: t, command_arg :: ValuePerKey.command_arg) :: t do
      ({:pre_split_former, cs}, arg) -> {:pre_split_former, [{:all_keys, arg} | cs]}
      ({:pre_merge_latter, cs}, arg) -> {:pre_merge_latter, [{:all_keys, arg} | cs]}
      (status                 , _  ) -> status
    end
  end

  defmodule KeysMap do
    # %{key => {keyed_value, size}}
    use Croma.SubtypeOfMap, key_module: Croma.Any, value_module: Croma.Tuple
  end

  use Croma.Struct, fields: [
    keyspace_name:    Croma.Atom,
    data_module:      Croma.Atom,
    hook_module:      Croma.TypeGen.nilable(Croma.Atom),
    status:           Status,
    range_start:      Croma.NonNegInteger,
    range_middle:     Croma.NonNegInteger,
    range_end:        Croma.NonNegInteger,
    keys_former_half: KeysMap,
    keys_latter_half: KeysMap,
    size_former_half: Croma.NonNegInteger,
    size_latter_half: Croma.NonNegInteger,
  ]

  def valid?(v) do
    super(v) and check?(v)
  end

  defp check?(%__MODULE__{status:       status,
                          range_start:  r_start,
                          range_middle: r_middle,
                          range_end:    r_end}) do
    # Checking all keys may take too long, so we omit it.
    case status do
      {:pre_split_latter, new_range_start} -> r_start == r_end and (new_range_start + r_end == 2 * r_middle)
      {:post_merge_latter, _}              -> r_start == r_end
      _                                    -> r_start < r_middle and r_middle < r_end and (r_start + r_end == 2 * r_middle)
    end
  end

  @behaviour RVData

  @impl true
  defun new() :: :uninitialized do
    :uninitialized
  end

  #
  # command
  #
  @impl true
  defun command(state0 :: v[:uninitialized | t], arg :: RVData.command_arg) :: {RVData.command_ret, t} do
    case state0 do
      :uninitialized ->
        case arg do
          {:split_install, latter_range_start, data}               -> {:ok, split_install(latter_range_start, data)}
          {:init_1st, ks_name, data_mod, hook_mod, r_start, r_end} -> {:ok, make_struct(ks_name, data_mod, hook_mod, r_start, r_end)}
          _                                                        -> {{:error, :uninitialized}, :uninitialized}
        end
      state ->
        case arg do
          {:c, key, command_arg}                             -> command_impl(state, key, command_arg)
          {:all_keys_command, command_arg}                   -> all_keys_command_impl(state, command_arg)
          {:split_prepare, latter_range_start}               -> split_prepare(state, latter_range_start)
          {:split_install, _latter_range_start, _data}       -> {{:error, :already_initialized}, state}
          {:split_shift_range_end, latter_range_start}       -> split_shift_range_end(state, latter_range_start)
          {:split_shift_range_start, cmds}                   -> split_shift_range_start(state, cmds)
          :split_discard_commands                            -> {:ok, split_discard_commands(state)}
          :merge_prepare                                     -> merge_prepare(state)
          {:merge_install, data}                             -> merge_install(state, data)
          :merge_shift_range_start                           -> merge_shift_range_start(state)
          {:merge_shift_range_end, latter_range_start, cmds} -> merge_shift_range_end(state, latter_range_start, cmds)
          {:init_1st, _n, _d, _h, _s, _e}                    -> {{:error, :already_initialized}, state}
          _                                                  -> {:ok, state}
        end
    end
  end

  defp make_struct(ks_name, data_mod, hook_mod, r_start, r_end) do
    %__MODULE__{
      keyspace_name:    ks_name,
      data_module:      data_mod,
      hook_module:      hook_mod,
      status:           :normal,
      range_start:      r_start,
      range_middle:     div(r_start + r_end, 2),
      range_end:        r_end,
      keys_former_half: %{},
      keys_latter_half: %{},
      size_former_half: 0,
      size_latter_half: 0,
    }
  end

  defp split_install(r_start,
                     %{keyspace_name:    ks_name,
                       data_module:      d,
                       hook_module:      h,
                       range_end:        r_end,
                       keys_latter_half: keys}) do
    r_middle = div(r_start + r_end, 2)
    {keys1, size1, keys2, size2} = split_keys_map(ks_name, r_middle, keys)
    %__MODULE__{
      keyspace_name:    ks_name,
      data_module:      d,
      hook_module:      h,
      status:           {:pre_split_latter, r_start},
      range_start:      r_end, # temporarily become an empty range
      range_middle:     r_middle,
      range_end:        r_end,
      keys_former_half: keys1,
      keys_latter_half: keys2,
      size_former_half: size1,
      size_latter_half: size2,
    }
  end

  defp split_keys_map(ks_name, r_middle, keys) do
    Enum.reduce(keys, {%{}, 0, %{}, 0}, fn({k, {_data, s} = pair}, {m1, s1, m2, s2}) ->
      case Hash.from_key(ks_name, k) do
        hash when hash < r_middle -> {Map.put(m1, k, pair), s1 + s, m2                  , s2    }
        _                         -> {m1                  , s1    , Map.put(m2, k, pair), s2 + s}
      end
    end)
  end

  defp command_impl(state, key, command_arg) do
    case check_key_position(state, key) do
      {:error, _} = e -> {e, state}
      :former         -> apply_command_on_former_half(state, key, command_arg)
      :latter         -> apply_command_on_latter_half(state, key, command_arg)
    end
  end

  defp check_key_position(%__MODULE__{keyspace_name: ks_name, status: status, range_start: s, range_middle: m, range_end: e}, key) do
    case Hash.from_key(ks_name, key) do
      hash when hash < s ->
        case status do
          {:pre_split_latter, new_range_start} when new_range_start <= hash -> {:error, :will_own_the_key_retry_afterward}
          _                                                                 -> {:error, {:below_range, s}}
        end
      hash when e <= hash ->
        case status do
          {:pre_merge_former, %{range_end: new_range_end}} when hash < new_range_end -> {:error, :will_own_the_key_retry_afterward}
          _                                                                          -> {:error, {:above_range, e}}
        end
      hash when hash < m -> :former
      _                  -> :latter
    end
  end

  Enum.each([:former, :latter], fn which ->
    fun_name   = :"apply_command_on_#{which}_half"
    keys_field = :"keys_#{which}_half"
    size_field = :"size_#{which}_half"
    defp unquote(fun_name)(%__MODULE__{:data_module        => d,
                                       :status             => status,
                                       unquote(keys_field) => keys,
                                       unquote(size_field) => total} = state,
                           key,
                           command_arg) do
      {ret, load, new_keys, new_total} = apply_command_to_half(d, keys, total, key, command_arg)
      new_status = Status.remember_single_key_command(status, unquote(which), {key, command_arg})
      new_state = %__MODULE__{state | :status => new_status, unquote(keys_field) => new_keys, unquote(size_field) => new_total}
      add_load(load)
      {{:ok, ret}, new_state}
    end
  end)

  defp apply_command_to_half(d, keys, total, key, command_arg) do
    {data, size} = Map.get(keys, key, {nil, 0})
    {ret, load, new_data, new_size} = d.command(data, size, key, command_arg)
    case new_data do
      nil -> {ret, load, Map.delete(keys, key)                   , total            - size}
      _   -> {ret, load, Map.put(keys, key, {new_data, new_size}), total + new_size - size}
    end
  end

  defp apply_command_to_both(ks_name, d, r_middle, {keys1, size1, keys2, size2}, command) do
    case command do
      {:key, key, arg} ->
        case Hash.from_key(ks_name, key) do
          hash when hash < r_middle ->
            {_ret, _load, new_keys1, new_size1} = apply_command_to_half(d, keys1, size1, key, arg)
            {new_keys1, new_size1, keys2, size2}
          _ ->
            {_ret, _load, new_keys2, new_size2} = apply_command_to_half(d, keys2, size2, key, arg)
            {keys1, size1, new_keys2, new_size2}
        end
      {:all_keys, arg} ->
        {_load1, new_keys1, new_size1} = apply_all_keys_command_to_half(d, keys1, arg)
        {_load2, new_keys2, new_size2} = apply_all_keys_command_to_half(d, keys2, arg)
        {new_keys1, new_size1, new_keys2, new_size2}
    end
  end

  defp apply_all_keys_command_to_half(d, keys, command_arg) do
    Enum.reduce(keys, {0, %{}, 0}, fn({key, {data, size}}, {load_acc, map, size_acc}) ->
      {_ret, load, new_data, new_size} = d.command(data, size, key, command_arg)
      case new_data do
        nil -> {load_acc + load, map                                    , size_acc           }
        _   -> {load_acc + load, Map.put(map, key, {new_data, new_size}), size_acc + new_size}
      end
    end)
  end

  defp all_keys_command_impl(%__MODULE__{data_module:      d,
                                         status:           status,
                                         keys_former_half: keys1,
                                         keys_latter_half: keys2} = state,
                             command_arg) do
    case status do
      {:pre_split_latter, _} ->
        # This shard hasn't yet acquire ownership of the keys. Nothing to do.
        {0, state}
      _ ->
        {load1, new_keys1, new_size1} = apply_all_keys_command_to_half(d, keys1, command_arg)
        {load2, new_keys2, new_size2} = apply_all_keys_command_to_half(d, keys2, command_arg)
        new_state = %__MODULE__{state |
          status:           Status.remember_all_keys_command(status, command_arg),
          keys_former_half: new_keys1,
          keys_latter_half: new_keys2,
          size_former_half: new_size1,
          size_latter_half: new_size2,
        }
        add_load(load1 + load2)
        {:ok, new_state}
    end
  end

  #
  # split
  #
  defp split_prepare(%__MODULE__{status:       status,
                                 range_middle: r_middle,
                                 range_end:    r_end} = state,
                     latter_range_start) do
    case status do
      :normal                    when r_middle == latter_range_start -> become_pre_split_former(state)
      {:pre_split_former, _cmds} when r_middle == latter_range_start -> become_pre_split_former(state)
      {:post_split_former, cmds} when r_end    == latter_range_start -> {{:error, {:range_already_shifted, cmds}}, state}
      _                                                              -> {error_status_unmatch(state), state}
    end
  end

  defp become_pre_split_former(state) do
    map_to_return = Map.take(state, [:keyspace_name, :data_module, :hook_module, :range_end, :keys_latter_half])
    new_state = %__MODULE__{state | status: {:pre_split_former, []}}
    {{:ok, map_to_return}, new_state}
  end

  defp split_shift_range_end(%__MODULE__{status:       status,
                                         range_middle: r_middle,
                                         range_end:    r_end} = state,
                             latter_range_start) do
    case status do
      {:pre_split_former , cmds} when r_middle == latter_range_start -> {{:ok, cmds}, become_post_split_former(state, cmds)}
      {:post_split_former, cmds} when r_end    == latter_range_start -> {{:ok, cmds}, state}
      _                                                              -> {error_status_unmatch(state), state}
    end
  end

  defp become_post_split_former(%__MODULE__{keyspace_name:    ks_name,
                                            range_start:      r_start,
                                            range_middle:     r_middle,
                                            keys_former_half: keys} = state,
                                cmds) do
    new_r_middle = div(r_start + r_middle, 2)
    {keys1, size1, keys2, size2} = split_keys_map(ks_name, new_r_middle, keys)
    %__MODULE__{state |
      status:           {:post_split_former, cmds},
      range_middle:     new_r_middle,
      range_end:        r_middle,
      keys_former_half: keys1,
      keys_latter_half: keys2,
      size_former_half: size1,
      size_latter_half: size2,
    }
  end

  defp split_shift_range_start(%__MODULE__{status: status} = state, cmds) do
    case status do
      {:pre_split_latter, new_range_start} -> {:ok, apply_commands_and_shift_range_start(state, new_range_start, cmds)}
      _                                    -> {error_status_unmatch(state), state}
    end
  end

  defp apply_commands_and_shift_range_start(%__MODULE__{keyspace_name:    ks_name,
                                                        data_module:      d,
                                                        range_middle:     r_middle,
                                                        keys_former_half: keys1,
                                                        keys_latter_half: keys2,
                                                        size_former_half: size1,
                                                        size_latter_half: size2} = state,
                                            new_range_start,
                                            cmds) do
    {new_keys1, new_size1, new_keys2, new_size2} =
      Enum.reduce(cmds, {keys1, size1, keys2, size2}, fn(cmd, tuple4) ->
        apply_command_to_both(ks_name, d, r_middle, tuple4, cmd)
      end)
    %__MODULE__{state |
      status:           :normal,
      range_start:      new_range_start,
      keys_former_half: new_keys1,
      keys_latter_half: new_keys2,
      size_former_half: new_size1,
      size_latter_half: new_size2,
    }
  end

  defp split_discard_commands(%__MODULE__{status: status} = state) do
    case status do
      {:post_split_former, _cmds} -> %__MODULE__{state | status: :normal}
      _                           -> state
    end
  end

  defp error_status_unmatch(%__MODULE__{status: status, range_start: r_start, range_end: r_end}) do
    label =
      case status do
        :normal -> :normal
        {l, _}  -> l
      end
    {:error, {:status_unmatch, label, r_start, r_end}}
  end

  #
  # merge
  #
  defp merge_prepare(%__MODULE__{status: status} = state) do
    case status do
      :normal                    -> become_pre_merge_latter(state)
      {:pre_merge_latter, _cmds} -> become_pre_merge_latter(state)
      {:post_merge_latter, cmds} -> {{:error, {:range_already_shifted, cmds}}, state}
      _                          -> {error_status_unmatch(state), state}
    end
  end

  defp become_pre_merge_latter(state) do
    map_to_return = Map.take(state, [:range_start, :range_middle, :range_end, :keys_former_half, :keys_latter_half, :size_former_half, :size_latter_half])
    new_state = %__MODULE__{state | status: {:pre_merge_latter, []}}
    {{:ok, map_to_return}, new_state}
  end

  defp merge_install(%__MODULE__{status:    status,
                                 range_end: r_end} = state,
                     %{range_start: next_range_start} = data) do
    case status do
      :normal                               when r_end == next_range_start -> {:ok, %__MODULE__{state | status: {:pre_merge_former, data}}}
      {:pre_merge_former, _next_range_data} when r_end == next_range_start -> {:ok, %__MODULE__{state | status: {:pre_merge_former, data}}}
      _                                                                    -> {error_status_unmatch(state), state}
    end
  end

  defp merge_shift_range_start(%__MODULE__{status: status} = state) do
    case status do
      {:pre_merge_latter , cmds} -> {{:ok, cmds}, become_post_merge_latter(state, cmds)}
      {:post_merge_latter, cmds} -> {{:ok, cmds}, state}
      _                          -> {error_status_unmatch(state), state}
    end
  end

  defp become_post_merge_latter(%__MODULE__{range_end: r_end} = state, cmds) do
    %__MODULE__{state |
      status:           {:post_merge_latter, cmds},
      range_start:      r_end,
      keys_former_half: %{}, # relinquish all keys
      keys_latter_half: %{},
      size_former_half: 0,
      size_latter_half: 0
    }
  end

  defp merge_shift_range_end(%__MODULE__{status:    status,
                                         range_end: r_end} = state,
                             latter_range_start,
                             cmds) do
    case status do
      {:pre_merge_former, next_range_data}    -> {:ok, apply_commands_and_shift_range_end(state, next_range_data, cmds)}
      :normal when latter_range_start < r_end -> {:ok, state}
      _                                       -> {error_status_unmatch(state), state}
    end
  end

  defp apply_commands_and_shift_range_end(%__MODULE__{keyspace_name:    ks_name,
                                                      data_module:      d,
                                                      range_start:      r_start,
                                                      range_end:        r_end,
                                                      keys_former_half: keys1,
                                                      keys_latter_half: keys2,
                                                      size_former_half: size1,
                                                      size_latter_half: size2} = state1,
                                          %{range_middle:     next_r_middle,
                                            range_end:        next_r_end,
                                            keys_former_half: keys3,
                                            keys_latter_half: keys4,
                                            size_former_half: size3,
                                            size_latter_half: size4},
                                          cmds) do
    {new_keys3, new_size3, new_keys4, new_size4} =
      Enum.reduce(cmds, {keys3, size3, keys4, size4}, fn(cmd, tuple4) ->
        apply_command_to_both(ks_name, d, next_r_middle, tuple4, cmd)
      end)
    {new_r_middle, keys_former, size_former, keys_latter, size_latter} =
      merge_keys_maps(ks_name, r_start, r_end, next_r_end, keys1, keys2, new_keys3, new_keys4, size1, size2, new_size3, new_size4)
    %__MODULE__{state1 |
      status:           :normal,
      range_middle:     new_r_middle,
      range_end:        next_r_end,
      keys_former_half: keys_former,
      keys_latter_half: keys_latter,
      size_former_half: size_former,
      size_latter_half: size_latter,
    }
  end

  defp merge_keys_maps(ks_name,
                       r_start, r_end, next_r_end,
                       keys1, keys2, keys3, keys4,
                       size1, size2, size3, size4) do
    case div(r_start + next_r_end, 2) do
      ^r_end ->
        # the 2 ranges have the same value of `r_end - r_start`
        {r_end, Map.merge(keys1, keys2), size1 + size2, Map.merge(keys3, keys4), size3 + size4}
      new_r_middle when new_r_middle < r_end ->
        # All keys in `keys1` belong to the former half of the newly-merged range.
        # All keys in `keys3` and `keys4` belong to the latter half of the newly-merged range.
        # We have to inspect each key in `keys2`.
        divide_keys_map(ks_name, new_r_middle, keys1, size1, keys2, Map.merge(keys3, keys4), size3 + size4)
      new_r_middle ->
        # All keys in `keys1` and `keys2` belong to the former half of the newly-merged range.
        # All keys in `keys4` belong to the latter half of the newly-merged range.
        # We have to inspect each key in `keys3`.
        divide_keys_map(ks_name, new_r_middle, Map.merge(keys1, keys2), size1 + size2, keys3, keys4, size4)
    end
  end

  defp divide_keys_map(ks_name, r_middle, keys_former, size_former, keys_to_divide, keys_latter, size_latter) do
    {keys1, size1, keys2, size2} =
      Enum.reduce(keys_to_divide, {keys_former, size_former, keys_latter, size_latter}, fn({k, {_data, s} = pair}, {m1, s1, m2, s2}) ->
        case Hash.from_key(ks_name, k) do
          hash when hash < r_middle -> {Map.put(m1, k, pair), s1 + s, m2                  , s2    }
          _                         -> {m1                  , s1    , Map.put(m2, k, pair), s2 + s}
        end
      end)
    {r_middle, keys1, size1, keys2, size2}
  end

  #
  # query
  #
  @impl true
  defun query(state0 :: v[:uninitialized | t], arg :: RVData.query_arg) :: RVData.query_ret do
    case state0 do
      :uninitialized -> {:error, :uninitialized}
      state          ->
        case arg do
          {:q, key, query_arg} -> query_impl(state, key, query_arg)
          :get_stats           -> get_stats(state)
          :list_keys           -> list_keys(state)
          _                    -> :ok
        end
    end
  end

  defp query_impl(%__MODULE__{data_module:      d,
                              keys_former_half: keys_former,
                              keys_latter_half: keys_latter} = state,
                  key,
                  query_arg) do
    case check_key_position(state, key) do
      {:error, _} = e -> e
      :former         -> run_query(d, keys_former, key, query_arg)
      :latter         -> run_query(d, keys_latter, key, query_arg)
    end
  end

  defp run_query(d, keys, key, query_arg) do
    case Map.get(keys, key) do
      nil ->
        increment_knf()
        {:error, :key_not_found}
      {data, size} ->
        {ret, load} = d.query(data, size, key, query_arg)
        add_load(load)
        {:ok, ret}
    end
  end

  defp get_stats(%__MODULE__{status:           status,
                             keys_former_half: keys_former,
                             keys_latter_half: keys_latter,
                             size_former_half: s1,
                             size_latter_half: s2}) do
    case status do
      {:post_merge_latter, _} -> {:error, :post_merge_latter}
      {:pre_split_latter , _} -> {:ok, 0, 0, 0, 0} # This shard hasn't yet acquire ownership of the keys.
      _                       ->
        n_keys = map_size(keys_former) + map_size(keys_latter)
        {load, knf} = reset_load_and_knf()
        {:ok, n_keys, s1 + s2, load, knf}
    end
  end

  defp list_keys(%__MODULE__{status:           status,
                             keys_former_half: keys1,
                             keys_latter_half: keys2}) do
    case status do
      {:pre_split_latter, _} -> {[]             , []             } # This shard hasn't yet acquire ownership of the keys.
      _                      -> {Map.keys(keys1), Map.keys(keys2)}
    end
  end

  #
  # recording "load" and "count of :key_not_found" in process dictionary
  #
  defp add_load(0), do: 0
  defp add_load(load) do
    new_load = Process.get(:raft_kv_load, 0) + load
    Process.put(:raft_kv_load, new_load)
  end

  defp increment_knf() do
    new_load = Process.get(:raft_kv_knf, 0) + 1
    Process.put(:raft_kv_knf, new_load)
  end

  defp reset_load_and_knf() do
    {Process.put(:raft_kv_load, 0) || 0, Process.put(:raft_kv_knf, 0) || 0}
  end

  #
  # hook
  #
  defmodule Hook do
    alias RaftedValue.Data, as: RVData
    alias RaftKV.Shard

    @behaviour RaftedValue.LeaderHook

    @impl true
    defun on_command_committed(data_before :: v[:uninitialized | Shard.t],
                               command_arg :: RVData.command_arg,
                               command_ret :: RVData.command_ret,
                               data_after  :: v[:uninitialized | Shard.t]) :: any do
      case {command_arg, command_ret} do
        {{:c, key, arg}, {:ok, ret}}    -> run_on_command_hook(data_before, key, arg, ret, data_after)
        {{:all_keys_command, arg}, :ok} -> run_on_command_hook_for_all_keys(data_before, arg, data_after)
        _                               -> :ok
      end
    end

    defp run_on_command_hook(%Shard{keys_former_half: keys1, keys_latter_half: keys2},
                             key,
                             arg,
                             ret,
                             %Shard{keyspace_name: ks_name, hook_module: h, range_middle: r_middle, keys_former_half: keys3, keys_latter_half: keys4}) do
      case h do
        nil -> :ok
        _   ->
          {keys_before, keys_after} =
            case Hash.from_key(ks_name, key) do
              hash when hash < r_middle -> {keys1, keys3}
              _                         -> {keys2, keys4}
            end
          {data_before, size_before} = Map.get(keys_before, key, {nil, 0})
          {data_after , size_after } = Map.get(keys_after , key, {nil, 0})
          debug_assert(data_before || data_after)
          h.on_command_committed(data_before, size_before, key, arg, ret, data_after, size_after)
      end
    end

    defp run_on_command_hook_for_all_keys(%Shard{keys_former_half: keys1, keys_latter_half: keys2},
                                          arg,
                                          %Shard{hook_module: h, keys_former_half: keys3, keys_latter_half: keys4}) do
      case h do
        nil -> :ok
        _   ->
          run_on_command_hook_for_all_keys_in_half(h, keys1, keys3, arg)
          run_on_command_hook_for_all_keys_in_half(h, keys2, keys4, arg)
      end
    end

    defp run_on_command_hook_for_all_keys_in_half(h, keys_before, keys_after, arg) do
      Enum.each(keys_before, fn {key, {data_before, size_before}} ->
        {data_after, size_after} = Map.get(keys_after, key, {nil, 0})
        h.on_command_committed(data_before, size_before, key, arg, nil, data_after, size_after)
      end)
    end

    @impl true
    defun on_query_answered(data      :: v[:uninitialized | Shard.t],
                            query_arg :: RVData.query_arg,
                            query_ret :: RVData.query_ret) :: any do
      case query_arg do
        {:q, key, arg} ->
          case query_ret do
            {:ok, ret} -> run_on_query_hook(data, key, arg, ret)
            _          -> :ok
          end
        _ -> :ok
      end
    end

    defp run_on_query_hook(%Shard{hook_module: h, keys_former_half: keys1, keys_latter_half: keys2}, key, arg, ret) do
      case h do
        nil -> :ok
        _   ->
          {data, size} = Map.get(keys1, key) || Map.get(keys2, key)
          h.on_query_answered(data, size, key, arg, ret)
      end
    end

    @impl true
    defun on_follower_added(_data :: :uninitialized | Shard.t, _pid :: pid) :: any do
      :ok
    end

    @impl true
    defun on_follower_removed(_data :: :uninitialized | Shard.t, _pid :: pid) :: any do
      :ok
    end

    @impl true
    defun on_elected(_data :: :uninitialized | Shard.t) :: any do
      Process.put(:raft_kv_load, 0)
      Process.put(:raft_kv_knf , 0)
    end

    @impl true
    defun on_restored_from_files(_data :: :uninitialized | Shard.t) :: any do
      :ok
    end
  end

  #
  # API
  #
  defun consensus_group_name(keyspace_name :: v[atom], range_start :: v[non_neg_integer]) :: atom do
    :"#{keyspace_name}_#{range_start}" # inevitable dynamic atom creation
  end

  defun initialize_1st_shard(keyspace_name :: v[atom],
                             data_module   :: v[module],
                             hook_module   :: v[nil | module],
                             range_start   :: v[non_neg_integer],
                             range_end     :: v[pos_integer]) :: :ok | {:error, :already_initialized} do
    cg_name = consensus_group_name(keyspace_name, range_start)
    {:ok, r} = RaftFleet.command(cg_name, {:init_1st, keyspace_name, data_module, hook_module, range_start, range_end})
    r
  end

  @default_rv_config_options [
    heartbeat_timeout:                   1_000,
    election_timeout:                    2_000,
    election_timeout_clock_drift_margin: 1_000,
    leader_hook_module:                  Hook,
  ]

  defun make_rv_config(rv_config_options :: Keyword.t \\ @default_rv_config_options) :: RaftedValue.Config.t do
    opts = Keyword.put(rv_config_options, :leader_hook_module, Hook) # :leader_hook_module must not be changed
    RaftedValue.make_config(__MODULE__, opts)
  end
end
