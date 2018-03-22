# RaftKV

An Elixir library to store key-value pairs in a distributed, fault-tolerant, self-adjusting data structure.

- [API Documentation](http://hexdocs.pm/raft_kv/)
- [Hex package information](https://hex.pm/packages/raft_kv)

[![Hex.pm](http://img.shields.io/hexpm/v/raft_kv.svg)](https://hex.pm/packages/raft_kv)
[![Build Status](https://travis-ci.org/skirino/raft_kv.svg)](https://travis-ci.org/skirino/raft_kv)
[![Coverage Status](https://coveralls.io/repos/github/skirino/raft_kv/badge.svg?branch=master)](https://coveralls.io/github/skirino/raft_kv?branch=master)

## Feature & Design

- Each value can be arbitrary data structure.
    - Operation on a stored value must be given as an implementation of `RaftKV.ValuePerKey` behaviour.
- Built on top of [raft_fleet](https://github.com/skirino/raft_fleet).
    - Key-value pairs are sharded into Raft consensus groups by hash-based partitioning.
- Shards are automatically split/merged according to number of keys, data size and current load.
- Designed for many key-value pairs and throughput.
