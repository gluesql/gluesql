defmodule GlueSQL.MixProject do
  use Mix.Project

  @version "0.15.0"
  @repo_url "https://github.com/gluesql/gluesql/tree/main/pkg/elixir"

  def project do
    [
      app: :gluesql,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      description: description()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.30.0"},
      {:jason, "~> 1.4.1"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:earmark, "~> 1.4", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "GlueSQL",
      extras: ["CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      source_ref: "v#{@version}",
      source_url: @repo_url,
      groups_for_modules: [
        Storages: [
          GlueSQL.Storages,
          GlueSQL.Storages.MemoryStorage
        ]
      ]
    ]
  end

  defp description do
    """
    GlueSQL is a SQL database library for Elixir.  
    This repository is an Elixir binding of the original Rust library `gluesql-rs`.
    """
  end

  defp package do
    [
      files: [
        "lib",
        "native",
        "mix.exs",
        "README.md",
        "CHANGELOG.md"
      ],
      maintainers: ["Hoon Wee"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @repo_url}
    ]
  end
end
