package cl.etl;

import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.model.ResumenETL;
import cl.etl.persistencia.ConsultasSQL;

/**
 * Hello world!
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws SQLException {
        String sql = ConsultasSQL.obtener("proceso.iniciar");
        ProcesoETL procesoETL = new ProcesoETL();
        long procesoId = -1;
        try {
            procesoId = procesoETL.iniciarProceso();

            PipelineClientes pipelineClientes = new PipelineClientes();

            ResumenETL resumen = pipelineClientes.procesar(procesoId);
            procesoETL.actualizarContadores(
                    procesoId,
                    resumen.getTotalLeidos(),
                    resumen.getTotalValidos(),
                    resumen.getTotalRechazados());

            procesoETL.finalizarProceso(
                    procesoId,
                    "COMPLETADO",
                    null);
        } catch (Exception e) {

            log.error("Error ejecutando proceso ETL {}", procesoId, e);

            try {
                procesoETL.finalizarProceso(
                        procesoId,
                        "ERROR",
                        e.getMessage());
            } catch (SQLException errorCierre) {
                log.error("No se pudo cerrar el proceso {}", procesoId, errorCierre);
            }

            throw new RuntimeException("Falló la ejecución ETL", e);
        }

        /**************************************
         * pruebas raras de Milo
         ************************************/
        /*
         * try {
         * // Simulamos los resultados de una ejecución.
         * // Todavía no procesamos el CSV real.
         * int totalLeidos = 2;
         * int totalValidos = 1;
         * int totalRechazados = 1;
         *
         * procesoETL.actualizarContadores(
         * procesoId,
         * totalLeidos,
         * totalValidos,
         * totalRechazados);
         *
         * procesoETL.finalizarProceso(
         * procesoId,
         * "COMPLETADO",
         * null);
         *
         * } catch (Exception e) {
         * try {
         * procesoETL.finalizarProceso(
         * procesoId,
         * "ERROR",
         * e.getMessage());
         * } catch (SQLException errorCierre) {
         * log.error(
         * "No se pudo registrar el error del proceso {}",
         * procesoId,
         * errorCierre);
         * }
         *
         * throw new RuntimeException(
         * "Falló la ejecución ETL", e);
         * }
         */
        log.info("sale:");
    }
}
