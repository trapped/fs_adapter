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
        adapter.all.should eq [] of Hash(String, ActiveRecord::SupportedType) unless adapter.all.size
        id = adapter.create({"name" => "Mario", "age" => 32}).not_nil!.id
        adapter.all.includes?({"id" => id, "name" => "Mario", "age" => 32}).should be_true
      end
    end

    describe "#find" do
      it "gets a single row by id" do
        adapter = build "test", "", {"name" => String, "age" => Int}
        id = adapter.create({"name" => "Mario", "age" => 32}).not_nil!.id
        adapter.find(id).should eq({"id" => id, "name" => "Mario", "age" => 32})
      end
    end

    describe "#update" do
      it "updates a single row by id" do
        adapter = build "test", "", {"name" => String, "age" => Int}
        id = adapter.create({"name" => "Mario", "age" => 32}).not_nil!.id
        adapter.update(id, {"age" => 33})
        adapter.find(id)["age"].should eq 33
      end
    end
  end
end

`rm -rf #{ENV["FSDB_PATH"]}`
