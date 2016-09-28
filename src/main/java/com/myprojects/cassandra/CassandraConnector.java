package com.myprojects.cassandra;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static java.lang.System.out;

/*
Read readme.txt under your Books/Cassandra-DB/Documents
Tutorial:
http://www.tutorialspoint.com/cassandra/

Java API Tutorial:
http://planetcassandra.org/getting-started-with-apache-cassandra-and-java-part-1/
http://planetcassandra.org/getting-started-with-apache-cassandra-and-java-part-2/

how to start and stop cassandra in local
https://docs.datastax.com/en/cassandra/2.0/cassandra/reference/referenceStartCprocess_t.html
CQL (Cassandra Query Language)
https://docs.datastax.com/en/cql/3.0/cql/cql_using/use_query_system_tables_t.html
After starting cassandra, you do
    cassandra installed location/bin>sh cqlsh
to give you a prompt where you can write query in terminal.


Connecting to a Cassandra cluster
http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/quick_start/qsSimpleClientCreate_t.html
Session to execute queries
http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/quick_start/qsSimpleClientAddSession_t.html
Prepared and bound statements
http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/quick_start/qsSimpleClientBoundStatements_t.html
Query Builder (like Criteria in Hibernate)
http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/reference/queryBuilder_r.html
CQL Commands
http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/cqlCommandsTOC.html

Required dependencies to set up cassandra project in IDE
http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/reference/settingUpJavaProgEnv_r.html
*/


/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnector {
    /**
     * Cassandra Cluster.
     */
    private Cluster cluster;

    /**
     * Cassandra Session.
     */
    private Session session;

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param node Cluster node IP address.
     * @param port Port of cluster host.
     */
    public void connect(final String node, final int port) {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts()) {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public Session getSession() {
        return this.session;
    }

    /**
     * Close cluster.
     */
    public void close() {
        cluster.close();
    }


    public void createSchema() {
        // Create keyspace (database schema)
        session.execute("CREATE KEYSPACE simplex WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':4};");

        // create tables
        session.execute(
                "CREATE TABLE simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                        ");");
        session.execute(
                "CREATE TABLE simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
    }
    public void loadData() {
        // insert records
        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");
        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                        "VALUES (" +
                        "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'" +
                        ");");
    }

    public void printData() {
        // get records
        ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
                "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d and title='La Petite Tonkinoise';");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
                    row.getString("album"),  row.getString("artist")));
        }
        System.out.println();

    }
    public void dropSchema() {
        session.execute("DROP KEYSPACE simplex");
    }
    /**
     * Main function for demonstrating connecting to Cassandra with host and port.
     *
     * @param args Command-line arguments; first argument, if provided, is the
     *    host and second argument, if provided, is the port.
     */
    public static void main(final String[] args)
    {
        final CassandraConnector client = new CassandraConnector();
        final String ipAddress = args.length > 0 ? args[0] : "localhost";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
        out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
        client.connect(ipAddress, port);

        //client.createSchema();
        client.loadData();
        client.printData();
        //client.dropSchema();

        client.close();
    }
}