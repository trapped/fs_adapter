require "./fs_adapter/**"
require "active_record"
require "dir"
require "thread/mutex"

module FSDB
  class Adapter < ActiveRecord::Adapter
    def self.build table_name, primary_field, fields, register = true
      new(table_name, primary_field, fields, register)
    end

    def initialize @table_name, primary_field, @fields, register = true
      Driver.open_db db_path, do |db|
        db.table(@table_name) || db.add_table(@table_name, convert_fields @fields)
      end
    end

    def create fields
      merged = merge_fields fields
      Driver.open_db db_path, &.table(@table_name).try &.add_row merged
    end

    def find id
      extract_fields Driver.open_db db_path, &.table(@table_name).try &.row id
    end

    def all
      extract_rows Driver.open_db db_path, &.table(@table_name).try &.rows
    end

    def where query_hash : Hash
      puts "WHERE #{query_hash.inspect}"
    end

    def where query : Query
      puts "WHERE #{query.inspect}"
    end

    def update id, fields
      puts "UPDATE #{id} FIELDS #{fields.inspect}"
    end

    def delete id
      puts "DELETE #{id}"
    end

    private def extract_rows rows
      return [] of Hash(String, ActiveRecord::SupportedType) unless rows
      rows.map { |row| extract_fields row }
    end

    private def extract_fields row
      fields = Hash(String, ActiveRecord::SupportedType).new
      return fields unless row
      row.to_h.each do |k, v|
        if v.is_a? ActiveRecord::SupportedType
          fields[k] = v
        else
          puts "Encountered unsupported type: #{v.class}/#{typeof(v)}"
        end
      end
      fields
    end

    private def db_path
      ENV["FSDB_PATH"]? || "#{`pwd`.chomp}/fsdb/"
    end

    private def merge_fields fields
      result = Hash(String, String|Int64).new
      fields.each do |k, v|
        if @fields.has_key? k
          v = v.to_i64 if v.is_a? Int
          result[k] = v
        end
      end
      return result
    end

    private def convert_fields fields
      converted_fields = Hash(String, String).new
      fields.each do |k, v|
        type_ = nil
        case v.inspect
        when "Int"
          type_ = "int64"
        when "String"
          type_ = "string"
        else next
        end
        converted_fields[k] = type_
      end
      converted_fields
    end
  end
end
