
package cl.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.persistencia.ConsultasSQL;

public class ProcesoETL {
    private static final Logger log = LoggerFactory.getLogger(ProcesoETL.class);

    public long iniciarProceso() throws SQLException {
        String sql = ConsultasSQL.obtener("proceso.iniciar");

        try (Connection conn = Propiedades.obtenerConexion();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException(
                        "No se pudo obtener el ID del proceso");
            }

            long procesoId = rs.getLong("proceso_id");
            log.info("Proceso ETL iniciado. ID: {}", procesoId);

            return procesoId;
        }
    }

    public void actualizarContadores(
            long procesoId,
            int totalLeidos,
            int totalValidos,
            int totalRechazados) throws SQLException {

        String sql = ConsultasSQL.obtener("proceso.actualizarContadores");

        try (Connection conn = Propiedades.obtenerConexion();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, totalLeidos);
            stmt.setInt(2, totalValidos);
            stmt.setInt(3, totalRechazados);
            stmt.setLong(4, procesoId);

            int filas = stmt.executeUpdate();

            if (filas != 1) {
                throw new SQLException(
                        "No se encontró el proceso " + procesoId);
            }

            log.info(
                    "Contadores actualizados. procesoId={}, leídos={}, válidos={}, rechazados={}",
                    procesoId, totalLeidos, totalValidos, totalRechazados);
        }
    }

    public void finalizarProceso(
            long procesoId,
            String estado,
            String mensajeError) throws SQLException {

        if (!"COMPLETADO".equals(estado)
                && !"ERROR".equals(estado)) {
            throw new IllegalArgumentException(
                    "Estado de cierre no válido: " + estado);
        }

        String sql = ConsultasSQL.obtener("proceso.finalizar");

        try (Connection conn = Propiedades.obtenerConexion();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, estado);
            stmt.setString(2, mensajeError);
            stmt.setLong(3, procesoId);

            int filas = stmt.executeUpdate();

            if (filas != 1) {
                throw new SQLException(
                        "No se encontró el proceso " + procesoId);
            }

            if ("ERROR".equals(estado)) {
                log.error(
                        "Proceso ETL finalizado con error. ID: {}, detalle: {}",
                        procesoId, mensajeError);
            } else {
                log.info("Proceso ETL completado. ID: {}", procesoId);
            }
        }
    }
}
