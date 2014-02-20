require 'java';
require 'jdbc-helper';
require 'mysql-connector-java-5.1.17-bin.jar';
require 'io/console';

module Gddb
  class Db
    def initialize ()
      
    end

    def get_interactive()
      db_user = ENV['USER'];
      # Like get_conn but getting username & password from stdin.
      print "\nPassword for user #{db_user}: >>";
      db_pw = STDIN.noecho(&:gets).strip;
      print "\n";
      conn = JDBCHelper::Connection.new(
                                        :driver           => 'com.mysql.jdbc.Driver',
                                        :url              => 'jdbc:mysql://mysql-sdr/ht_repository',
                                        :user             => db_user,
                                        :password         => db_pw,
                                        :useCursorFetch   => 'true', 
                                        :defaultFetchSize => 10000,
                                        );
      return conn;
    end

  end	
end
