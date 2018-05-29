# RaftKV

An Elixir library to store key-value pairs in a distributed, fault-tolerant, self-adjusting data structure.

- [API Documentation](https://hexdocs.pm/raft_kv/)
- [Hex package information](https://hex.pm/packages/raft_kv)

[![Hex.pm](http://img.shields.io/hexpm/v/raft_kv.svg)](https://hex.pm/packages/raft_kv)
[![Build Status](https://travis-ci.org/skirino/raft_kv.svg)](https://travis-ci.org/skirino/raft_kv)
[![Coverage Status](https://coveralls.io/repos/github/skirino/raft_kv/badge.svg?branch=master)](https://coveralls.io/github/skirino/raft_kv?branch=master)

## Feature & Design

- Each value can be arbitrary data structure.
    - Operation on a stored value must be given as an implementation of `RaftKV.ValuePerKey` behaviour.
- Built on top of [raft_fleet](https://github.com/skirino/raft_fleet).
    - Key-value pairs are sharded into multiple Raft consensus groups by hash partitioning.
- Based on number of keys, data size and current load, shards are automatically split/merged in a transparent manner.
- Designed for many key-value pairs and throughput.

## Usage

Suppose we have the following callback module for simple key-value store.
```ex
defmodule KV do
  alias RaftKV.ValuePerKey
  @behaviour ValuePerKey

  @impl true
  def command(_previous_value, _size, _key, {:set, value}) do
    {:ok, 5, value, 0}
  end
  def command(_previous_value, _size, _key, :unset) do
    {:ok, 5, nil, 0}
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
```

Let's initialize `:raft_kv` and then register a keyspace.
```ex
RaftFleet.activate("some_zone")
RaftKV.init()
RaftKV.register_keyspace(:kv, [], KV, nil, %RaftKV.SplitMergePolicy{max_shards: 16, max_keys_per_shard: 100})
```

Now we can get/set values with arbitrary keys.
```ex
KV.get("foo")        # => nil
KV.set("foo", "bar") # => :ok
KV.get("foo")        # => "bar"
```

Initially there's only one shard.
```ex
RaftKV.reduce_keyspace_shard_names(:kv, [], &[&1 | &2]) # => [:kv_0]
```

When the number of key-value pairs exceeds the limit per shard (`100`) as follows...
```ex
Enum.each(1..200, fn i -> KV.set("#{i}", i) end)
```

then shards are automatically split.
(Depending on the configurations it may take several minutes. See `RaftKV.Config` for more detail.)
```ex
RaftKV.reduce_keyspace_shard_names(:kv, [], &[&1 | &2]) # => [:kv_100663296, :kv_67108864, :kv_0]
```

Similarly, when you remove key-value pairs...
```ex
Enum.each(1..200, fn i -> KV.unset("#{i}") end)
```

the shards are automatically merged.
```ex
RaftKV.reduce_keyspace_shard_names(:kv, [], &[&1 | &2]) # => [:kv_0]
```

Shard splits and merges are transparent from client processes interacting with key-value pairs.
