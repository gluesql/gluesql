defmodule GlueSQL.Native do
  use Rustler,
    otp_app: :gluesql,
    crate: :gluesql_nif

  def glue(_attrs_as_maps), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
