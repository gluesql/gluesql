defmodule GlueSQL do
  @moduledoc """
  Documentation for `GlueSQL`.
  """

  @doc """
  Create a glue object.
  It accepts storage engine, and enables query/mutation features.
  """
  def glue(storage) do
    GlueSQL.Native.glue(storage)
  end
end
