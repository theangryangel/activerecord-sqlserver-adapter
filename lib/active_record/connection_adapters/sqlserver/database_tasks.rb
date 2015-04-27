module ActiveRecord
  module ConnectionAdapters
    module SQLServer
      module DatabaseTasks

        def create_database(database, options = {})
          name = quote_database_name(database)
          options = {collation: @connection_options[:collation]}.merge!(options.symbolize_keys)
          options = options.select { |_, v| v.present? }
          option_string = options.inject("") do |memo, (key, value)|
            memo += case key
            when :collation
              " COLLATE #{value}"
            else
              ""
            end
          end
          do_execute "CREATE DATABASE #{name}#{option_string}"
        end

        def drop_database(database)
          name = quote_database_name(database)
          do_execute "DROP DATABASE #{name}"
        end

        def current_database
          select_value 'SELECT DB_NAME()'
        end

        def charset
          select_value "SELECT DATABASEPROPERTYEX(DB_NAME(), 'SqlCharSetName')"
        end

        def collation
          select_value "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation')"
        end

      end
    end
  end
end
