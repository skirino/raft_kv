use Croma

defmodule RaftKV.SplitMergePolicy do
  defmodule MergeThresholdRatio do
    use Croma.SubtypeOfFloat, min: 0.0, max: 1.0
  end

  use Croma.Struct, fields: [
    min_shards:                    Croma.PosInteger,
    max_shards:                    Croma.PosInteger,
    max_keys_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    max_size_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    max_load_per_shard:            Croma.TypeGen.nilable(Croma.PosInteger),
    load_per_query_to_missing_key: Croma.NonNegInteger,
    merge_threshold_ratio:         MergeThresholdRatio,
  ]

  def valid?(p) do
    super(p) and check?(p)
  end

  defun check?(%__MODULE__{min_shards: min, max_shards: max}) :: boolean do
    min <= max
  end
end
