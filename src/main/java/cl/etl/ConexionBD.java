
package cl.etl;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConexionBD {

    public static final Properties PROPERTIES = cargarPropiedades();

    private ConexionBD() {
        // Clase utilitaria: no se instancia.
    }

    private static Properties cargarPropiedades() {
        Properties properties = new Properties();

        try (InputStream input = ConexionBD.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new IllegalStateException(
                        "No se encontró application.properties");
            }

            properties.load(input);
            return properties;

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Error al cargar application.properties", e);
        }
    }

    public static Connection obtenerConexion() throws SQLException {
        String url = PROPERTIES.getProperty("db.url");
        String usuario = PROPERTIES.getProperty("db.username");
        String password = System.getenv("DB_PASSWORD");

        if (password == null || password.isBlank()) {
            throw new IllegalStateException(
                    "Debes configurar la variable de entorno DB_PASSWORD");
        }

        return DriverManager.getConnection(url, usuario, password);
    }
}
