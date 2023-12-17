defmodule GlueSQL do
  @moduledoc """
  Documentation for `GlueSQL`.
  """

  @doc """
  Create a glue object.
  It accepts storage engine, and enables query/mutation features.

  ## Example

      iex> Html5ever.parse("<!doctype html><html><body><h1>Hello world</h1></body></html>")
      {:ok,
       [
         {:doctype, "html", "", ""},
         {"html", [], [{"head", [], []}, {"body", [], [{"h1", [], ["Hello world"]}]}]}
       ]}

  """
  def glue(storage) do
    GlueSQL.Native.glue(storage)
  end
end
