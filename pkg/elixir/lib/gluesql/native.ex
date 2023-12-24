defmodule GlueSQL.Native do
  use Rustler,
    otp_app: :gluesql,
    crate: :gluesql

  def glue_new(_storage), do: err()
  def memory_storage_new(), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
