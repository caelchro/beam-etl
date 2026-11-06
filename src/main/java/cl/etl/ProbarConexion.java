
package cl.etl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ProbarConexion {

    private static final Logger log = LoggerFactory.getLogger(ProbarConexion.class);

    public static void main(String[] args) {
        try (Connection conn = ConexionBD.obtenerConexion()) {
            DatabaseMetaData meta = conn.getMetaData();

            log.info("Conexión exitosa");
            log.info("Base de datos: " + conn.getCatalog());
            log.info("Driver: " + meta.getDriverVersion());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
