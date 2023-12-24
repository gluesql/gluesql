defmodule GlueSQL do
  @moduledoc """
  Documentation for `GlueSQL`.
  """

  @doc """
  Create a GlueSQL database, using memory storage.
  """
  def glue_new(storage) do
    GlueSQL.Native.glue_new(storage)
  end
end
