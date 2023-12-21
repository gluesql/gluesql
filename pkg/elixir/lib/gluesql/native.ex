defmodule GlueSQL.Native do
  use Rustler,
    otp_app: :gluesql,
    crate: :"gluesql-ex"

  def glue(_storage), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
