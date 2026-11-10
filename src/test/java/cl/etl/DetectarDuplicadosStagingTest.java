package cl.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import cl.etl.coder.StagingCoder;
import cl.etl.model.StagingClientes;
import cl.etl.transform.DetectarDuplicadosStaging;

public class DetectarDuplicadosStagingTest {
    @Test
    void debeMantenerClienteValido() {
        // Arrange
        StagingClientes cliente = new StagingClientes(
                Constantes.ESTADO_VALIDO,
                "1,Ana,ana@ejemplo.cl");
        cliente.setIdCliente(1L);
        cliente.setNombre("Ana");
        cliente.setEmail("ana@ejemplo.cl");

        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
                .apply("CrearCliente", Create.of(cliente))
                .setCoder(new StagingCoder());

        PCollection<KV<Long, StagingClientes>> clientesPorId = lista
                .apply("AsignarIdComoClave",
                        WithKeys.of(StagingClientes::getIdCliente))
                .setCoder(KvCoder.of(
                        VarLongCoder.of(),
                        new StagingCoder()));
        PCollection<KV<Long, Iterable<StagingClientes>>> gruposPorId = clientesPorId.apply(
                "AgruparPorId",
                GroupByKey.create());
        PCollection<StagingClientes> revisionDuplicados = gruposPorId.apply(
                "DetectarDuplicados",
                ParDo.of(new DetectarDuplicadosStaging()));

        // Assert
        // Verificar que el cliente salga con estado VALIDO
        PAssert.that(revisionDuplicados)
                .satisfies(clientes -> {
                    List<StagingClientes> resultado = new ArrayList<>();
                    clientes.forEach(resultado::add);

                    assertEquals(1, resultado.size());
                    assertEquals(
                            Constantes.ESTADO_VALIDO,
                            resultado.get(0).getEstado());
                    assertEquals(
                            1,
                            resultado.get(0).getIdCliente());
                    assertEquals(
                            "Ana",
                            resultado.get(0).getNombre());
                    assertEquals(
                            "ana@ejemplo.cl",
                            resultado.get(0).getEmail());

                    return null;
                });

        pipeline.run().waitUntilFinish();
    }

    @Test
    void debeRechazarClienteIdDuplicado() {
        // Arrange
        List<StagingClientes> inicia = new ArrayList<>();
        inicia.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana", "ana@@ejemplo", "1,Ana,ana@@ejemplo"));
        inicia.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana2", "ana2@@ejemplo", "1,Ana2,ana2@@ejemplo"));

        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
                .apply("CrearCliente", Create.of(inicia))
                .setCoder(new StagingCoder());
        PCollection<KV<Long, StagingClientes>> clientesPorId = lista
                .apply("AsignarIdComoClave",
                        WithKeys.of(StagingClientes::getIdCliente))
                .setCoder(KvCoder.of(
                        VarLongCoder.of(),
                        new StagingCoder()));
        PCollection<KV<Long, Iterable<StagingClientes>>> gruposPorId = clientesPorId.apply(
                "AgruparPorId",
                GroupByKey.create());
        PCollection<StagingClientes> revisionDuplicados = gruposPorId.apply(
                "DetectarDuplicados",
                ParDo.of(new DetectarDuplicadosStaging()));
        // Assert
        // Verificar que el cliente salga con estado ESTADO_RECHAZADO
        PAssert.that(revisionDuplicados)
                .satisfies(clientes -> {
                    List<StagingClientes> resultado = new ArrayList<>();
                    clientes.forEach(resultado::add);

                    assertEquals(2, resultado.size());
                    assertEquals(
                            Constantes.ESTADO_RECHAZADO,
                            resultado.get(0).getEstado());
                    assertEquals(
                            Constantes.MR_DUPLICADO,
                            resultado.get(0).getMotivoRechazo());
                    assertEquals(
                            1,
                            resultado.get(0).getIdCliente());
                    assertEquals(
                            Constantes.ESTADO_RECHAZADO,
                            resultado.get(1).getEstado());
                    assertEquals(
                            Constantes.MR_DUPLICADO,
                            resultado.get(1).getMotivoRechazo());
                    assertEquals(
                            1,
                            resultado.get(1).getIdCliente());

                    return null;
                });

        pipeline.run().waitUntilFinish();
    }

    @Test
    void debeRechazarClienteIdTriplicado() {
        // Arrange
        List<StagingClientes> inicia = new ArrayList<>();
        inicia.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana", "ana@@ejemplo", "1,Ana,ana@@ejemplo"));
        inicia.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana2", "ana2@@ejemplo", "1,Ana2,ana2@@ejemplo"));
        inicia.add(new StagingClientes(Constantes.ESTADO_VALIDO, 1L, "Ana3", "ana3@@ejemplo", "1,Ana3,ana3@@ejemplo"));

        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
                .apply("CrearCliente", Create.of(inicia))
                .setCoder(new StagingCoder());
        PCollection<KV<Long, StagingClientes>> clientesPorId = lista
                .apply("AsignarIdComoClave",
                        WithKeys.of(StagingClientes::getIdCliente))
                .setCoder(KvCoder.of(
                        VarLongCoder.of(),
                        new StagingCoder()));
        PCollection<KV<Long, Iterable<StagingClientes>>> gruposPorId = clientesPorId.apply(
                "AgruparPorId",
                GroupByKey.create());
        PCollection<StagingClientes> revisionDuplicados = gruposPorId.apply(
                "DetectarDuplicados",
                ParDo.of(new DetectarDuplicadosStaging()));
        // Assert
        // Verificar que el cliente salga con estado ESTADO_RECHAZADO
        PAssert.that(revisionDuplicados)
                .satisfies(clientes -> {
                    List<StagingClientes> resultado = new ArrayList<>();
                    clientes.forEach(resultado::add);

                    assertEquals(3, resultado.size());
                    assertEquals(
                            Constantes.ESTADO_RECHAZADO,
                            resultado.get(0).getEstado());
                    assertEquals(
                            Constantes.MR_DUPLICADO,
                            resultado.get(0).getMotivoRechazo());
                    assertEquals(
                            1,
                            resultado.get(0).getIdCliente());
                    assertEquals(
                            Constantes.ESTADO_RECHAZADO,
                            resultado.get(1).getEstado());
                    assertEquals(
                            Constantes.MR_DUPLICADO,
                            resultado.get(1).getMotivoRechazo());
                    assertEquals(
                            1,
                            resultado.get(1).getIdCliente());

                    assertEquals(
                            Constantes.ESTADO_RECHAZADO,
                            resultado.get(2).getEstado());
                    assertEquals(
                            Constantes.MR_DUPLICADO,
                            resultado.get(2).getMotivoRechazo());
                    assertEquals(
                            1,
                            resultado.get(2).getIdCliente());
                    return null;
                });

        pipeline.run().waitUntilFinish();
    } 
    
    @Test
    void listaVacia() {
        // Act
        Pipeline pipeline = Pipeline.create();

      PCollection<StagingClientes> lista = pipeline
        .apply("CrearCliente", Create.empty(new StagingCoder()));
        PCollection<KV<Long, StagingClientes>> clientesPorId = lista
                .apply("AsignarIdComoClave",
                        WithKeys.of(StagingClientes::getIdCliente))
                .setCoder(KvCoder.of(
                        VarLongCoder.of(),
                        new StagingCoder()));
        PCollection<KV<Long, Iterable<StagingClientes>>> gruposPorId = clientesPorId.apply(
                "AgruparPorId",
                GroupByKey.create());
        PCollection<StagingClientes> revisionDuplicados = gruposPorId.apply(
                "DetectarDuplicados",
                ParDo.of(new DetectarDuplicadosStaging()));
        // Assert
        // Verificar lista vacia
        PAssert.that(revisionDuplicados)
                .satisfies(clientes -> {
                    List<StagingClientes> resultado = new ArrayList<>();
                    clientes.forEach(resultado::add);

                    assertEquals(0, resultado.size());
                    return null;
                });

        pipeline.run().waitUntilFinish();
    }
}
