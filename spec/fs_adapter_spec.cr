require "./spec_helper"
require "../src/**"

ENV["FSDB_PATH"] = "#{__DIR__}/../test_db"

module FSDB
  module Driver
    describe "self.open_db" do
      it "opens a db" do
        open_db ENV["FSDB_PATH"], &.inspect
        Dir.exists?(ENV["FSDB_PATH"]).should be_true
      end
    end
  end

  class Adapter
    describe "self.build" do
      it "initializes the adapter and the db" do
        build "test", "", {"name" => String, "age" => Int}
      end
    end

    describe "#all, create" do
      it "let you add a row and get all the rows" do
        adapter = build "test", "", {"name" => String, "age" => Int}
        adapter.all.should eq [] of Hash(String, ActiveRecord::SupportedType)
        adapter.create({"name" => "Mario", "age" => 32})
        adapter.all.should eq [{"id" => 0, "name" => "Mario", "age" => 32}]
      end
    end
  end
end

`rm -rf #{ENV["FSDB_PATH"]}`
