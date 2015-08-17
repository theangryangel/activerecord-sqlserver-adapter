require 'active_record/tasks/database_tasks'
require 'shellwords'
require 'ipaddr'
require 'socket'

module ActiveRecord
  module Tasks

    class SQLServerDatabaseTasks

      DEFAULT_COLLATION = 'SQL_Latin1_General_CP1_CI_AS'

      delegate :connection, :establish_connection, :clear_active_connections!,
               to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      def create(master_established = false)
        establish_master_connection unless master_established
        connection.create_database configuration['database'], configuration.merge('collation' => default_collation)
        establish_connection configuration
      rescue ActiveRecord::StatementInvalid => error
        if /database .* already exists/i === error.message
          raise DatabaseAlreadyExists
        else
          raise
        end
      end

      def drop
        establish_master_connection
        connection.drop_database configuration['database']
      end

      def charset
        connection.charset
      end

      def collation
        connection.collation
      end

      def purge
        clear_active_connections!
        drop
        create true
      end

      def structure_dump(filename)
        File.open(filename, 'w') { |file|
          # Generate DDL for tables
          connection.select_all("select 
  'create table [' + so.name + '] (' + o.list + ')' + CASE WHEN tc.Constraint_Name IS NULL THEN '' ELSE 'ALTER TABLE ' + so.Name + ' ADD CONSTRAINT ' + tc.Constraint_Name  + ' PRIMARY KEY ' + ' (' + LEFT(j.List, Len(j.List)-1) + ')' END as definition
from sysobjects so
cross apply
    (SELECT 
        '  ['+column_name+'] ' + 
        data_type + case data_type
            when 'sql_variant' then ''
            when 'text' then ''
            when 'ntext' then ''
            when 'xml' then ''
            when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')'
            else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end + ' ' +
        case when exists ( 
        select id from syscolumns
        where object_name(id)=so.name
        and name=column_name
        and columnproperty(id,name,'IsIdentity') = 1 
        ) then
        'IDENTITY(' + 
        cast(ident_seed(so.name) as varchar) + ',' + 
        cast(ident_incr(so.name) as varchar) + ')'
        else ''
        end + ' ' +
         (case when IS_NULLABLE = 'No' then 'NOT ' else '' end ) + 'NULL ' + 
          case when information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT '+ information_schema.columns.COLUMN_DEFAULT ELSE '' END + ', ' 

     from information_schema.columns where table_name = so.name
     order by ordinal_position
    FOR XML PATH('')) o (list)
left join
    information_schema.table_constraints tc
on  tc.Table_name       = so.Name
AND tc.Constraint_Type  = 'PRIMARY KEY'
cross apply
    (select '[' + Column_Name + '], '
     FROM   information_schema.key_column_usage kcu
     WHERE  kcu.Constraint_Name = tc.Constraint_Name
     ORDER BY
        ORDINAL_POSITION
     FOR XML PATH('')) j (list)
where   xtype = 'U'
AND name NOT IN ('dtproperties')").each do |row|
            file.puts "#{row['definition']}\r\nGO\r\n"
          end

          # Export views definitions
          connection.select_all("select definition, o.type from sys.objects as o join sys.sql_modules as m on m.object_id = o.object_id where o.type = 'V'").each do |row|
            file.puts "#{row['definition']}\r\nGO\r\n"
          end

          # Export stored procedures definitions
          connection.select_all("select object_definition(object_id) as routine_definition from sys.all_objects where type = 'P' and is_ms_shipped = 0").each do |row|
            file.puts "#{row['definition']}\r\nGO\r\n"
          end
        }
      end

      def structure_load(filename)
        structure = File.read(filename)
        # Split by GO so that operations that must be in separate batches are in
        # separate batches
        structure.split(/^GO/).each { |s|
          connection.execute s
        }
      end


      private

      def configuration
        @configuration
      end

      def default_collation
        configuration['collation'] || DEFAULT_COLLATION
      end

      def establish_master_connection
        establish_connection configuration.merge('database' => 'master')
      end

    end

    module DatabaseTasksSQLServer

      extend ActiveSupport::Concern

      module ClassMethods

        LOCAL_IPADDR = [
          IPAddr.new('192.168.0.0/16'),
          IPAddr.new('10.0.0.0/8'),
          IPAddr.new('172.16.0.0/12')
        ]

        private

        def local_database?(configuration)
          super || local_ipaddr?(configuration_host_ip(configuration))
        end

        def configuration_host_ip(configuration)
          return nil unless configuration['host']
          Socket::getaddrinfo(configuration['host'], 'echo', Socket::AF_INET)[0][3]
        end

        def local_ipaddr?(host_ip)
          return false unless host_ip
          LOCAL_IPADDR.any? { |ip| ip.include?(host_ip) }
        end

      end

    end

    DatabaseTasks.register_task %r{sqlserver}, SQLServerDatabaseTasks
    DatabaseTasks.send :include, DatabaseTasksSQLServer

  end
end
