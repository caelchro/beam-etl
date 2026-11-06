
package cl.etl;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.util.Map;

public final class ConsultasSQL {

    private static final Map<String, Object> CONSULTAS = cargar();

    private ConsultasSQL() {
    }

    // @SuppressWarnings("unchecked")
    private static Map<String, Object> cargar() {
        try (InputStream input = ConsultasSQL.class
                .getClassLoader()
                .getResourceAsStream("sql/queries.yml")) {

            if (input == null) {
                throw new IllegalStateException(
                        "No se encontró queries.yml");
            }

            Yaml yaml = new Yaml(
                    new SafeConstructor(new LoaderOptions()));

            return yaml.load(input);

        } catch (Exception e) {
            throw new IllegalStateException(
                    "Error al cargar queries.yml", e);
        }
    }

    public static String obtener(String clave) {
        String[] partes = clave.split("\\.");
        Object actual = CONSULTAS;

        for (String parte : partes) {
            if (!(actual instanceof Map<?, ?> mapa)
                    || !mapa.containsKey(parte)) {
                throw new IllegalArgumentException(
                        "No existe la consulta SQL: " + clave);
            }

            actual = mapa.get(parte);
        }

        if (!(actual instanceof String sql)) {
            throw new IllegalArgumentException(
                    "La clave no corresponde a una consulta SQL: "
                            + clave);
        }

        return sql.strip();
    }
}
