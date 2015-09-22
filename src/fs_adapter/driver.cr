require "file"
require "dir"

module FSDB
  class LockError < Exception; end

  def self.lock_write path, &block
    object = File.join File.dirname(path), File.basename(path)
    File.open "#{object}.lock", "w+" do |f|
      f.flock_exclusive do
        yield
      end
    end
    File.delete "#{object}.lock"
  end

  def self.lock_read path, &block
    object = File.join File.dirname(path), File.basename(path)
    File.open "#{object}.lock", "rw" do |f|
      f.flock_shared do
        yield
      end
    end
    File.delete "#{object}.lock"
  end

  class Row
    private macro read_int64 io, storage
      %buffer = Slice(UInt8).new(8)
      %n = {{io.id}}.read(%buffer)
      break if %n < 8
      {{storage.id}} = (%buffer.pointer(0) as Pointer(Int64)).value
    end

    private macro read_string io, storage
      %string_length = 0i64
      read_int64 {{io}}, %string_length
      %buffer = Slice(UInt8).new(%string_length)
      %n = {{io.id}}.read(%buffer)
      break if %n < %string_length
      {{storage.id}} = String.new(s.pointer(0))
    end

    def initialize @path, @fields
      FSDB.lock_read @path do
        File.open @path, "rb" do |f|
          @data["id"] = File.basename @path
          @data = Hash(String,String|Int64).new
          @fields.each do |k, v|
            case v
            when "int64"
              read_int64 f, @data[k]
            when "string"
              read_string f, @data[k]
            else
              puts "FSDB: Unsupported field type #{v}"
            end
          end
        end
      end
    end

    def fields
      @fields
    end

    def [] field
      @data[field]
    end

    def []= field, value
      @data[field] = value
    end

    def to_h
      @data
    end
  end

  class Table
    def initialize @path
    end

    def initialize @path, fields
      FSDB.lock_write @path do
        metadata_path = File.join @path, "metadata"
        FSDB.lock_write(metadata_path) do
          File.open metadata_path, "w" do |f|
            f.puts fields.to_a.map(&.join ":").join ","
          end
        end
      end
    end

    def id
      File.basename(@path).split("_")[0].to_i
    end

    def name
      File.basename(@path).split("_")[1]
    end

    def fields
      FSDB.lock_read @path do
        metadata_path = File.join @path, "metadata"
        FSDB.lock_read metadata_path do
          File.open metadata_path, "r" do |f|
            f.gets.split(",").map(&.split(":")).to_h
          end
        end
      end
    end

    private def rows_
      FSDB.lock_read @path do
        Dir.entries(@path)
          .reject!(&.== "metadata")
          .reject!(&.=~ /(.*)\.lock/i)
          .map do |id|
            [id.to_i, Row.new File.join(@path, id), fields]
          end || Array(Int|Row|Hash(String,String)).new
      end
    end

    def rows
      rows_.not_nil!.map &.[1]
    end

    def row id : Int
      rows_.select(&.[0].== id)[0][1]
    end
  end

  class Database
    def initialize @path
      FSDB.lock_write @path do
        Dir.mkdir_p @path unless Dir.exists? @path
      end
    end

    def add_table table_name, fields
      FSDB.lock_write @path do
        table_path = File.join @path, "#{tables.size}_#{table_name}"
        FSDB.lock_write(table_path) do
          Dir.mkdir_p table_path unless Dir.exists? table_path
        end
        Table.new table_path, fields
      end
    end

    private def tables_
      FSDB.lock_read @path do
        Dir.entries(@path)
          .reject!(&.== "metadata")
          .reject!(&.=~ /(.*)\.lock/i)
          .map do |full_name|
            name = full_name.split("_")
            [name[0].to_i64, name[1], Table.new File.join(@path, full_name)]
          end || Array(Int64|String|Table).new
      end
    end

    def tables
      tables_.not_nil!.map(&.[2])
    end

    def table id : Int
      tables_.select(&.[0].== id)[0][2]
    end

    def table name : String
      tables_.select(&.[1].== name)[0][1]
    end
  end

  module Driver
    def self.open_db path, &block
      yield Database.new path
    end
  end
end
