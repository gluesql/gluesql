defmodule GlueSQLTest do
  use ExUnit.Case
  doctest GlueSQL

  # test "create table" do
  #   db = Glue
  #
  #   assert db.query("CREATE TABLE Foo (id INTEGER);") == %{type: "CREATE TABLE"}
  #   assert db.query("CREATE TABLE Bar;") == %{type: "CREATE TABLE"}
  # end
end
