defmodule GlueSQL.Storages.MemoryStorage do
  @moduledoc """
  Methods for creating and using memory storage.
  """

  @doc """
  Create in-memory storage instance.
  """
  def new() do
    GlueSQL.Native.memory_storage_new()
  end
end
