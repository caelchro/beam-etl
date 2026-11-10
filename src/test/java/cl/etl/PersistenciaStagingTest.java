package cl.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import cl.etl.coder.StagingCoder;
import cl.etl.model.StagingClientes;
import cl.etl.persistencia.ConsultasSQL;
import cl.etl.persistencia.PersisteStaging;

public class PersistenciaStagingTest {
    
    @Test
    void datosOK() throws Exception {
   
        // Arrange
        List<StagingClientes> clientes = new ArrayList<>();
        clientes.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana", "ana@ejemplo", "1,Ana,ana@ejemplo"));
        clientes.add(new StagingClientes(Constantes.ESTADO_VALIDO, 2L, "Ana2", "ana2@ejemplo", "2,Ana2,ana2@ejemplo"));
        clientes.add(new StagingClientes(Constantes.ESTADO_VALIDO, 3L, "Ana3", "ana3@ejemplo", "3,Ana3,ana3@ejemplo"));

        ProcesoETL procesoETL = new ProcesoETL();
        // Act
        long procesoId = procesoETL.iniciarProceso();
        
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
                .apply("CrearCliente", Create.of(clientes))
                .setCoder(new StagingCoder());
        lista.apply("PersistenciaStaging",
                        ParDo.of(new PersisteStaging(procesoId)));
        pipeline.run().waitUntilFinish();

        // Assert
        // Verificar que 3 clientes salgan con estado VALIDO
        String sqlConsulta = ConsultasSQL.obtener("staging.getAllByProceso");
        try (Connection conn = Propiedades.obtenerConexion();
            PreparedStatement stmt = conn.prepareStatement(sqlConsulta)) {
            stmt.setLong(1, procesoId);
           try (var rs = stmt.executeQuery()) {

                int cantidad = 0;
                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    cantidad++;
                    ids.add(rs.getLong("cliente_id"));

                    assertEquals(Constantes.ESTADO_VALIDO, rs.getString("estado"));
                    assertEquals("", rs.getString("motivo_rechazo"));
                }
                ids.sort(Long::compareTo);
                assertEquals(List.of(1L, 2L, 3L), ids);

                assertEquals(3, cantidad);
            }
        } finally {
            String sql3 = ConsultasSQL.obtener("staging.borrarByProceso");
            String sql4 = ConsultasSQL.obtener("proceso.borrarProceso");
            try (Connection conn = Propiedades.obtenerConexion();
                PreparedStatement stmt = conn.prepareStatement(sql3);
                PreparedStatement stmt4 = conn.prepareStatement(sql4);) {
                stmt.setLong(1, procesoId);
                stmt.executeUpdate();

                stmt4.setLong(1, procesoId);
                stmt4.executeUpdate();
            }
        }
       
    }

    @Test
    void datosRechazados() throws Exception {
   
        // Arrange
        List<StagingClientes> clientes = new ArrayList<>();
        clientes.add(new StagingClientes(1L, "Ana", "ana@ejemplo.cl", Constantes.ESTADO_VALIDO, "", "1,Ana,ana@ejemplo.cl"));
        clientes.add(new StagingClientes(2L, "Ana2", "ana2@@@ejemplo.cl", Constantes.ESTADO_RECHAZADO, Constantes.MR_EMAIL_INVALIDO, "2,Ana2,ana2@@@ejemplo.cl"));
        clientes.add(new StagingClientes(-1L, "Ana3", "ana3@ejemplo.cl", Constantes.ESTADO_RECHAZADO, Constantes.MR_ID_INVALIDO, "a,Ana3,ana3@ejemplo.cl"));

        ProcesoETL procesoETL = new ProcesoETL();
        // Act
        long procesoId = procesoETL.iniciarProceso();
        
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
                .apply("CrearCliente", Create.of(clientes))
                .setCoder(new StagingCoder());
        lista.apply("PersistenciaStaging",
                        ParDo.of(new PersisteStaging(procesoId)));
        pipeline.run().waitUntilFinish();

        // Assert
        // Verificar que 3 clientes salgan
        String sqlConsulta = ConsultasSQL.obtener("staging.getAllByProceso");
        try (Connection conn = Propiedades.obtenerConexion();
            PreparedStatement stmt = conn.prepareStatement(sqlConsulta)) {
            stmt.setLong(1, procesoId);
           try (var rs = stmt.executeQuery()) {

                while (rs.next()) {
                    if (rs.getLong("cliente_id") == -1L) {
                        assertEquals(Constantes.MR_ID_INVALIDO, rs.getString("motivo_rechazo"));
                        assertEquals(Constantes.ESTADO_RECHAZADO, rs.getString("estado"));
                    } else if (rs.getLong("cliente_id") == 2L) {
                        assertEquals(Constantes.MR_EMAIL_INVALIDO, rs.getString("motivo_rechazo"));
                        assertEquals(Constantes.ESTADO_RECHAZADO, rs.getString("estado"));
                    } else if (rs.getLong("cliente_id") == 1L) {
                        assertEquals("", rs.getString("motivo_rechazo"));
                        assertEquals(Constantes.ESTADO_VALIDO, rs.getString("estado"));
                    } else {
                        throw new AssertionError("Cliente inesperado: " + rs.getLong("cliente_id"));
                    }
                }
            }
        } finally {
            String sql3 = ConsultasSQL.obtener("staging.borrarByProceso");
            String sql4 = ConsultasSQL.obtener("proceso.borrarProceso");
            try (Connection conn = Propiedades.obtenerConexion();
                PreparedStatement stmt = conn.prepareStatement(sql3);
                PreparedStatement stmt4 = conn.prepareStatement(sql4);) {
                stmt.setLong(1, procesoId);
                stmt.executeUpdate();

                stmt4.setLong(1, procesoId);
                stmt4.executeUpdate();
            }
        }
       
    }
}
