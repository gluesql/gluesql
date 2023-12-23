defmodule GlueSQL do
  @moduledoc """
  Documentation for `GlueSQL`.
  """

  @doc """
  Create a GlueSQL database, using memory storage.
  """
  def glue_memory_storage() do
    GlueSQL.Native.glue_memory_storage()
  end
end
