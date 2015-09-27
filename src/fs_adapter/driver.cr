require "file"
require "dir"

module FSDB
  class LockError < Exception; end

  def self.lock_write path, &block
    object = File.join File.dirname(path), File.basename(path)
    File.open "#{object}.lock", "w+" do |f|
      f.flock_exclusive do
        begin
          yield
        end
      end
    end
  ensure
    begin
      File.delete "#{object}.lock"
    rescue
    end
  end

  def self.lock_read path, &block
    object = File.join File.dirname(path), File.basename(path)
    File.open "#{object}.lock", "w+" do |f|
      f.flock_shared do
        begin
          yield
        end
      end
    end
  ensure
    begin
      File.delete "#{object}.lock"
    rescue
    end
  end

  class Row
    private def bytes(n : Int64)
      sz = sizeof(typeof(n))
      shift = sz * 8
      Array(UInt8).new(sz) { shift -= 8; (n >> shift).to_u8 }.reverse
    end

    macro read_int64 io, storage
      %buffer = Slice(UInt8).new(8)
      %n = {{io.id}}.read(%buffer)
      %new_buf = Slice(UInt8).new(8)
      %buffer.each_with_index { |v, i| %new_buf[%buffer.size - i - 1] = v }
      {{storage.id}} = (%buffer.pointer(0) as Pointer(Int64)).value
    end

    macro write_int64 io, storage
      {{io.id}}.write bytes ({{storage.id}} as Int64)
    end

    macro read_string io, storage
      %string_length = 0i64
      read_int64 {{io.id}}, %string_length
      %buffer = Slice(UInt8).new(%string_length)
      %n = {{io.id}}.read(%buffer)
      {{storage.id}} = String.new(%buffer)
    end

    macro write_string io, storage
      write_int64 {{io.id}}, ({{storage.id}} as String).size.to_i64
      {{io.id}}.write ({{storage.id}} as String).bytes
    end

    private def read
      FSDB.lock_read @path do
        File.open @path, "r+" do |f|
          @data.try &.["id"] = File.basename(@path).to_i64
          @fields.not_nil!.each do |k, v|
            case v
            when "int64"
              read_int64 f, @data.not_nil![k]
            when "string"
              read_string f, @data.not_nil![k]
            else
              puts "FSDB: Unsupported field type #{v}"
            end
          end
        end
      end
    end

    private def write
      FSDB.lock_write @path do
        File.open @path, "w+" do |f|
          @data.not_nil!["id"] = File.basename(@path).to_i64
          @fields.not_nil!.each do |k, v|
            case v
            when "int64"
              write_int64 f, @data.not_nil![k]
            when "string"
              write_string f, @data.not_nil![k]
            else
              puts "FSDB: Unsupported field type #{v}"
            end
          end
        end
      end
    end

    def initialize @path, @fields = Hash(String, String).new, @data = nil : Hash(String, String|Int64)?
      @data = Hash(String, String|Int64).new unless @data
      if File.exists? @path
        read
      else
        write
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
      @data.not_nil!
    end
  end

  class Table
    def initialize @path
    end

    def initialize @path, fields : Hash(String, String)
      FSDB.lock_write @path do
        metadata_path = File.join @path, "metadata"
        FSDB.lock_write(metadata_path) do
          File.open metadata_path, "w+" do |f|
            f.puts fields.to_a.map(&.not_nil!.join ":").not_nil!.join ","
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
          File.open metadata_path, "r+" do |f|
            f.gets.try(
              &.chomp).try(
              &.split ",").try(
              &.map(&.split ":")).try(
              &.to_h)
          end
        end
      end
    end

    def add_row data : Hash(String, String|Int64)
      Row.new File.join(@path, "#{rows.size}"), fields, data
    end

    private def rows_ : Array(Array(Int64|Row))
      ff = fields
      FSDB.lock_read @path do
        return (Dir.entries(@path).try(
          &.-(["metadata"])).try(
          &.reject(&.=~ /(.*)\.lock/i)).try(
          &.-([".", ".."])).try(
          &.map do |id|
            [id.to_i64, Row.new(File.join(@path, id), ff)]
          end).try(
          &.sort_by(&.[0] as Int64)) || Array(Array(Int64|Row)).new).not_nil!
      end
    end

    def rows
      rows_.try &.map &.[1] as Row || Array(Row).new
    end

    def row id : Int
      rows_.select(&.[0].== id)[0]?.try &.[1] as Row
    end
  end

  class Database
    def initialize @path
      FSDB.lock_write @path do
        Dir.mkdir_p @path unless Dir.exists? @path
      end
    end

    def add_table table_name, fields : Hash(String, String)
      table_path = File.join @path, "#{tables.size}_#{table_name}"
      FSDB.lock_write table_path do
        Dir.mkdir_p table_path
      end
      Table.new table_path, fields
    end

    private def tables_
      FSDB.lock_read @path do
        listed = Dir.entries(@path).try(
          &.-(["metadata"])).try(
          &.reject(&.=~ /(.*)\.lock/i)).try(
          &.-([".", ".."])).try(
          &.map do |full_name|
            name = full_name.split("_")
            [name[0].to_i64, name[1], Table.new File.join(@path, full_name)]
          end).try(
          &.sort_by(&.[0] as Int64)) || Array(Array(Int64|Row|Hash(String,String))).new
        return listed
      end
    end

    def tables
      tables_.not_nil!.map(&.[2]) || Array(Table).new
    end

    def table id : Int
      tables_.not_nil!.select(&.[0].== id)[0]?.try &.[2] as Table
    end

    def table name : String
      tables_.not_nil!.select(&.[1].== name)[0]?.try &.[2] as Table
    end
  end

  module Driver
    def self.open_db path, &block
      yield Database.new path
    end
  end
end
