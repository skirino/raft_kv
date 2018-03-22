use Croma

defmodule RaftKV.SplitMergePolicy do
  defmodule MergeThresholdRatio do
    use Croma.SubtypeOfFloat, min: 0.0, max: 1.0, default: 0.5
  end

  @moduledoc """
  An Elixir struct to specify when to split/merge shards in a keyspace.

  `:raft_kv` uses the following 3 stats to judge whether splitting/merging should be done:

  1. number of keys in a shard
  2. aggregated data size (in an arbitrary unit) in a shard
  3. current load (computing resource required, in an arbitrary unit) of a shard

  Fields:

  - `:max_shards`
    Maximum number of shards.
    If number of shards is the same as this value, no further split occurs.
  - `:min_shards`
    Minimum number of shards.
    If number of shards is the same as this value, no further merge occurs.
  - `:max_keys_per_shard`
    Threshold number of keys for shard split.
    Shards that contains more keys than this value become candidates for split.
    If `nil`, shards are not split due to number of keys.
    Defaults to `nil`.
  - `:max_size_per_shard`
    Threshold size for shard split.
    Shards whose aggregated size exceeds this value become candidates for split.
    If `nil`, shards are not split due to size.
    Defaults to `nil`.
  - `:max_load_per_shard`
    Threshold load for shard split.
    Shards that have been experiencing load above this value become candidates for split.
    If `nil`, shards are not split due to load.
    Defaults to `nil`.
  - `:load_per_query_to_missing_key`
    `RaftKV.query/4` returns `{:error, :key_not_found}` if target `key` does not exist.
    This value is used as the "load" for such queries.
    Defaults to `0`.
  - `:merge_threshold_ratio`:
    For each of the 3 types of split thresholds above (`:max_keys_per_shard`, `:max_size_per_shard` and `:max_load_per_shard`),
    merge thresholds are calculated by multiplying this ratio.
    Consecutive 2 shards become candidates for merge if they together
    (1) contain less keys than the threshold,
    (2) contain smaller size than the threshold, and
    (3) experience less load than the threshold.
    Defaults to `#{MergeThresholdRatio.default()}`.
  """

  use Croma.Struct, fields: [
    max_shards:                    Croma.PosInteger,
    min_shards:                    {Croma.PosInteger, [default: 1]},
    max_keys_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    max_size_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    max_load_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    load_per_query_to_missing_key: {Croma.NonNegInteger, [default: 0]},
    merge_threshold_ratio:         MergeThresholdRatio,
  ]

  def valid?(p) do
    super(p) and check?(p)
  end

  defun check?(%__MODULE__{min_shards: min, max_shards: max}) :: boolean do
    min <= max
  end
end
