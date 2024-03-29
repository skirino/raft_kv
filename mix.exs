defmodule RaftKV.MixProject do
  use Mix.Project

  @github_url "https://github.com/skirino/raft_kv"

  def project() do
    [
      app:               :raft_kv,
      version:           "0.2.5",
      elixir:            "~> 1.6",
      build_embedded:    Mix.env() == :prod,
      start_permanent:   Mix.env() == :prod,
      deps:              deps(),
      description:       "An Elixir library to store key-value pairs in a distributed, fault-tolerant, self-adjusting data structure.",
      package:           package(),
      source_url:        @github_url,
      homepage_url:      @github_url,
      test_coverage:     [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test],
    ]
  end

  def application() do
    [
      extra_applications: [:logger],
      mod: {RaftKV.Application, []},
    ]
  end

  defp deps() do
    [
      {:croma       , "~> 0.9"},
      {:rafted_value, "~> 0.9"},
      {:raft_fleet  , "~> 0.8"},
      {:dialyxir    , "~> 1.3" , [only: :dev , runtime: false]},
      {:ex_doc      , "~> 0.29", [only: :dev , runtime: false]},
      {:excoveralls , "~> 0.16", [only: :test, runtime: false]},
    ]
  end

  defp package() do
    [
      files:       ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Shunsuke Kirino"],
      licenses:    ["MIT"],
      links:       %{"GitHub repository" => @github_url},
    ]
  end
end
