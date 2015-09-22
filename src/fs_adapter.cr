require "./fs_adapter/**"
require "active_record"
require "dir"
require "thread/mutex"

module FSDB
  class Adapter < ActiveRecord::Adapter
    def self.build table_name, primary_field, fields, register = true
      new(table_name, primary_field, fields, register)
    end

    def initialize @table_name, primary_field, fields, register = true
      puts "BUILD #{@table_name} #{fields.inspect}"
      Driver.open_db db_path, &.add_table @table_name, convert_fields fields
    end

    def create fields
      puts "CREATE #{fields.inspect}"
      Driver.open_db db_path, &.tables[@table_name].add_row fields
    end

    def find id
      puts "FIND #{id}"
      extract_fields Driver.open_db db_path, &.tables[@table_name].rows[id]
    end

    def all
      puts "ALL"
      extract_rows Driver.open_db db_path, &.tables[@table_name].rows
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
      rows.map { |row| extract_fields row }
    end

    private def extract_fields row
      fields = Hash(String, ActiveRecord::SupportedType).new
      row.each do |k, v|
        if v.is_a? ActiveRecord::SupportedType
          fields[k] = v
        else
          puts "Encountered unsupported type: #{v.class}/#{typeof(value)}"
        end
      end
      fields
    end

    private def db_path
      ENV["FSDB_PATH"] || "./fsdb/"
    end

    private def convert_fields fields
      fields.map { |k, v|
        case v.class
        when Int
          v = "int64"
        when String
          v = "string"
        end
      }
    end
  end
end
