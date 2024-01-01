defmodule GlueSQL.MixProject do
  use Mix.Project

  def project do
    [
      app: :gluesql,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
end
