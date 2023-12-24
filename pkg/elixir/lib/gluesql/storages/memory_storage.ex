defmodule GlueSQL.Storages.MemoryStorage do
  @moduledoc false

  def new() do
    GlueSQL.Native.memory_storage_new()
  end
end
