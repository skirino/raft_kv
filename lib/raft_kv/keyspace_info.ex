use Croma

defmodule RaftKV.KeyspaceInfo do
  alias RaftKV.{Hash, SplitMergePolicy}

  use Croma.Struct, fields: [
    policy: SplitMergePolicy,
    shards: Croma.Tuple, # :gb_trees.tree(nil | :locked | last_split_merge_time, range_start, {n_keys, size, load})
  ]

  defun make(policy :: v[SplitMergePolicy.t]) :: v[t] do
    shards_with_1st = :gb_trees.insert(0, {nil, nil, nil, nil}, :gb_trees.empty())
    %__MODULE__{policy: policy, shards: shards_with_1st}
  end

  defun shard_range_start_positions(%__MODULE__{shards: shards}) :: [Hash.t] do
    :gb_trees.keys(shards)
  end

  defun add_shard(%__MODULE__{shards: shards} = info, new_range_start :: v[Hash.t]) :: v[t] do
    new_shards = :gb_trees.insert(new_range_start, {:locked, nil, nil, nil}, shards)
    %__MODULE__{info | shards: new_shards}
  end

  defun touch_both(%__MODULE__{shards: shards} = info, range_start1 :: v[Hash.t], range_start2 :: v[Hash.t], time :: v[pos_integer]) :: v[t] do
    new_shards = shards |> touch1(range_start1, time) |> touch1(range_start2, time)
    %__MODULE__{info | shards: new_shards}
  end

  defun touch_and_delete(%__MODULE__{shards: shards} = info,
                         range_start1 :: v[Hash.t],
                         range_start2 :: v[Hash.t],
                         time         :: v[pos_integer]) :: v[t] do
    new_shards = :gb_trees.delete(range_start2, shards) |> touch1(range_start1, time)
    %__MODULE__{info | shards: new_shards}
  end

  defp touch1(shards, range_start, time) do
    {_t, n, s, l} = :gb_trees.get(range_start, shards)
    :gb_trees.update(range_start, {time, n, s, l}, shards)
  end

  @typep pair_or_nil :: nil | {non_neg_integer, non_neg_integer}

  defun store(%__MODULE__{policy: %SplitMergePolicy{load_per_query_to_missing_key: load_per_knf} = policy,
                          shards: shards} = info,
              map            :: %{Hash.t => {pair_or_nil, pair_or_nil}},
              threshold_time :: v[pos_integer]) :: {t, [{float, Hash.t}], [{float, Hash.t, Hash.t}]} do
    {new_shards, split_candidates, merge_candidates} =
      Enum.reduce(map, {shards, [], []}, fn({range_start, pair_of_pairs}, {r1, scs1, mcs1}) ->
        debug_assert(pair_of_pairs != {nil, nil})
        case get_quad_and_update_shard(r1, load_per_knf, range_start, pair_of_pairs) do
          nil ->
            {r1, scs1, mcs1}
          {r2, quad} ->
            {scs2, mcs2} =
              case compute_split_merge_demand(policy, r2, threshold_time, range_start, quad) do
                nil          -> {scs1       , mcs1       }
                {:split, sc} -> {[sc | scs1], mcs1       }
                {:merge, mc} -> {scs1       , [mc | mcs1]}
              end
            {r2, scs2, mcs2}
        end
      end)
    {%__MODULE__{info | shards: new_shards}, split_candidates, merge_candidates}
  end

  defp get_quad_and_update_shard(shards, load_per_knf, range_start, pair_of_pairs) do
    case :gb_trees.lookup(range_start, shards) do
      :none ->
        nil
      {:value, {t, _n, _s, _l}} ->
        {n, s, l} =
          case pair_of_pairs do
            {{n_keys, size}, nil        } -> {n_keys, size, nil                      }
            {nil           , {load, knf}} -> {nil   , nil , load + load_per_knf * knf}
            {{n_keys, size}, {load, knf}} -> {n_keys, size, load + load_per_knf * knf}
          end
        q = {t, n, s, l}
        {:gb_trees.update(range_start, q, shards), q}
    end
  end

  defp compute_split_merge_demand(%SplitMergePolicy{max_keys_per_shard:    max_keys,
                                                    max_size_per_shard:    max_size,
                                                    max_load_per_shard:    max_load,
                                                    merge_threshold_ratio: merge_threshold_ratio},
                                  shards,
                                  threshold_time,
                                  range_start,
                                  {last_split_merge_time, n_keys, size, load}) do
    if eligible_for_split_or_merge?(last_split_merge_time, threshold_time) do
      max_ratio = ratio(n_keys, max_keys) |> max(ratio(size, max_size)) |> max(ratio(load, max_load))
      # Avoid splitting shard with `n_keys == 1`, as (probably) splitting won't help in that case.
      if n_keys > 1 and max_ratio > 1.0 do
        {:split, {max_ratio, range_start}}
      else
        # Merge shards only when all `n_keys` and `size` are filled (i.e., don't merge if in doubt); `load == nil` is regarded as no load
        if n_keys && size && max_ratio < merge_threshold_ratio do
          case tree_next(shards, range_start) do
            nil ->
              nil
            {next_range_start, {last_split_merge_time2, n_keys2, size2, load2}} ->
              if eligible_for_split_or_merge?(last_split_merge_time2, threshold_time) and n_keys2 && size2 do
                ratio_if_merged = ratio(n_keys + n_keys2, max_keys) |> max(ratio(size + size2, max_size)) |> max(ratio((load || 0) + (load2 || 0), max_load))
                if ratio_if_merged < merge_threshold_ratio do
                  {:merge, {ratio_if_merged, range_start, next_range_start}}
                end
              end
          end
        end
      end
    end
  end

  defp eligible_for_split_or_merge?(nil                  , _t            ), do: true
  defp eligible_for_split_or_merge?(:locked              , _t            ), do: false
  defp eligible_for_split_or_merge?(last_split_merge_time, threshold_time), do: last_split_merge_time < threshold_time

  defp ratio(nil  , _  ), do: 0.0
  defp ratio(_    , nil), do: 0.0
  defp ratio(value, max), do: value / max

  defun check_if_splittable(%__MODULE__{policy: %SplitMergePolicy{max_shards: max_shards},
                                        shards: shards} = info,
                            range_start :: v[Hash.t]) :: {:ok, t, Hash.t} | :error do
    if :gb_trees.size(shards) < max_shards do
      case :gb_trees.lookup(range_start, shards) do
        :none ->
          :error
        {:value, {_t, n, s, l}} ->
          new_shards = :gb_trees.update(range_start, {:locked, n, s, l}, shards)
          new_info = %__MODULE__{info | shards: new_shards}
          range_end =
            case tree_next(shards, range_start) do
              nil            -> Hash.upper_bound()
              {r_end, _quad} -> r_end
            end
          {:ok, new_info, div(range_start + range_end, 2)}
      end
    else
      :error
    end
  end

  defun check_if_mergeable(%__MODULE__{policy: %SplitMergePolicy{min_shards: min_shards},
                                       shards: shards} = info,
                           range_start1 :: v[Hash.t],
                           range_start2 :: v[Hash.t]) :: {:ok, t} | :error do
    if :gb_trees.size(shards) > min_shards do
      case {:gb_trees.lookup(range_start1, shards), :gb_trees.lookup(range_start2, shards)} do
        {:none                      , _                          } -> :error
        {_                          , :none                      } -> :error
        {{:value, {_t1, n1, s1, l1}}, {:value, {_t2, n2, s2, l2}}} ->
          new_shards1 = :gb_trees.update(range_start1, {:locked, n1, s1, l1}, shards)
          new_shards2 = :gb_trees.update(range_start2, {:locked, n2, s2, l2}, new_shards1)
          new_info = %__MODULE__{info | shards: new_shards2}
          {:ok, new_info}
      end
    else
      :error
    end
  end

  defp tree_next(shards, range_start) do
    case :gb_trees.iterator_from(range_start + 1, shards) |> :gb_trees.next() do
      :none                     -> nil
      {next_start, quad, _iter} -> {next_start, quad}
    end
  end
end
