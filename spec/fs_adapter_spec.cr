require "./spec_helper"
require "../src/**"

DB_PATH = "#{__DIR__}/../test_db"

module FSDB
  module Driver
    describe "self.open_db" do
      it "opens a db" do
        open_db DB_PATH, &.inspect
        File.exists?(DB_PATH).should be_true
        Dir.rmdir DB_PATH
      end
    end
  end

  class Adapter
    describe "self.build" do
      it "initializes the adapter and the db" do
        build "test", "", {"name" => String, "age" => Int}
      end
    end
  end
end
