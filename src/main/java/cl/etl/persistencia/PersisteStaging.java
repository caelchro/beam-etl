package cl.etl.persistencia;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.ConexionBD;
import cl.etl.ConsultasSQL;
import cl.etl.model.StagingClientes;

public class PersisteStaging extends DoFn<StagingClientes, StagingClientes> {

    private static final Logger log = LoggerFactory.getLogger(PersisteStaging.class);
private long procesados;
    private transient Connection conn;
    private transient PreparedStatement stmt;
    private long procesoId;

    public PersisteStaging(long procesoId) {
        this.procesoId = procesoId;
    }

    @Setup
    public void setup() throws SQLException {
        conn = DriverManager.getConnection(
            ConexionBD.PROPERTIES.getProperty("db.url"),
            ConexionBD.PROPERTIES.getProperty("db.username"),
            System.getenv("DB_PASSWORD")
        );

        stmt = conn.prepareStatement(ConsultasSQL.obtener("staging.insertar"));
    }

    @ProcessElement
    public void procesar(@Element StagingClientes cliente) throws SQLException {
        stmt.setLong(1, this.procesoId);
        stmt.setString(2, cliente.getLinea());
        stmt.setLong(3, cliente.getIdCliente());
        stmt.setString(4, cliente.getNombre());
        stmt.setString(5, cliente.getEmail());
        stmt.setString(6, cliente.getEstado());
        stmt.setString(7, cliente.getMotivoRechazo());
        stmt.addBatch();
    procesados++;
    }

    @Teardown
    public void teardown() throws SQLException {
        log.info("procesados:" + procesados);
        if (stmt != null) {
            stmt.executeBatch();
            stmt.close();
        }

        if (conn != null) {
            conn.close();
        }
    }
}
