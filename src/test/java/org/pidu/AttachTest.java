package org.pidu;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.*;

@Testcontainers
public class AttachTest {
    @Container
    public PostgreSQLContainer psql = new PostgreSQLContainer("postgres:14");

    DataSource dataSource;

    @BeforeEach
    public void setup() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName(psql.getHost());
        ds.setDatabaseName(psql.getDatabaseName());
        ds.setUser(psql.getUsername());
        ds.setPassword(psql.getPassword());
        ds.setPortNumber(psql.getFirstMappedPort());
        dataSource = ds;
    }

    @Test
    public void runAttachWithoutMidSteps() throws SQLException {
        prepareTables();

        Connection con = dataSource.getConnection();
        Statement st = con.createStatement();
        st.execute("insert into films_partition values (1, 'dr', 'musician',5)");
        st.execute("alter table films_partition add constraint check_code check (code = 'dr');");
        con.close();

        Connection con2 = dataSource.getConnection();
        Statement st2 = con2.createStatement();
        st2.execute("""
                BEGIN;
                """);
        st2.execute("ALTER TABLE films ATTACH PARTITION films_partition for values in ('dr')");

        printLocks();
    }

    @Test
    public void runAttachWithAdditionalConstraints() throws SQLException {
        prepareTables();

        Connection con = dataSource.getConnection();
        Statement st = con.createStatement();
        st.execute("insert into films_partition values (1, 'dr', 'musician', 5)");
        st.execute("alter table films_partition add constraint check_code check (code = 'dr');");
        st.execute("alter table films_partition ADD CONSTRAINT fk_did FOREIGN KEY (did) REFERENCES refs (id);");
        con.close();

        Connection con2 = dataSource.getConnection();
        Statement st2 = con2.createStatement();
        st2.execute("""
                BEGIN;
                """);
        st2.execute("ALTER TABLE films ATTACH PARTITION films_partition for values in ('dr')");

        printLocks();
    }

    private void printLocks() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("""
                    select relname, mode
                    from pg_locks l
                        join pg_class c on (relation = c.oid)
                        join pg_namespace nsp on (c.relnamespace = nsp.oid);
                    """);
            System.out.println(convertResultSet(resultSet));
        }
    }

    private void prepareTables() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement st = connection.createStatement();
            st.execute("""
                    CREATE TABLE refs (
                        id          integer primary key,
                        did         integer
                    );
                    """);
            st.execute("""
                    CREATE TABLE films (
                        id          integer,
                        code        char(5) ,
                        title       varchar(40) NOT NULL,
                        did         integer NOT NULL  references refs(id)
                        )
                        partition by list (code);
                    """);
            st.execute("insert into refs values (5, 5)");
            st.execute("create table films_partition (LIKE films INCLUDING ALL)");
        }
    }

    private String convertResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        StringBuilder builder = new StringBuilder();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) builder.append(",  ");
                String columnValue = resultSet.getString(i);
                builder.append(columnValue + " " + rsmd.getColumnName(i));
            }
            builder.append("\n");
        }
        return builder.toString();
    }
}
