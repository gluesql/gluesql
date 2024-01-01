defmodule GlueSQL do
  @external_resource "README.md"

  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC -->")
             |> Enum.fetch!(1)

  alias Jason

  @doc """
  Create a GlueSQL database with given storage engine.

  ## Example: Create a SQL database that runs on in-memory storage.

      alias GlueSQL.Storages.MemoryStorage

      db =
        MemoryStorage.new()
        |> GlueSQL.glue_new()

  """
  def glue_new(storage) do
    GlueSQL.Native.glue_new(storage)
  end

  @doc """
  Execute given query for database.
  SQL query should be provided as string format.

  ## Example: Create tables (with-or-without schema). 

       GlueSQL.query(db, \"""
           CREATE TABLE Foo (id INTEGER); # schema
           CREATE TABLE Bar; #schema-less
           \""")
  """
  def query(glue_db, sql) do
    case GlueSQL.Native.glue_query(glue_db, sql) do
      {:ok, payload} ->
        decoded_payload = decode_payload(payload)
        {:ok, decoded_payload}

      result ->
        result
    end
  end

  defp decode_payload(payload) when is_list(payload) do
    payload
    |> Enum.map(&Jason.decode!(&1))
  end

  defp decode_payload(payload) do
    Jason.decode!(payload)
  end
end
