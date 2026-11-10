package cl.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import cl.etl.coder.StagingCoder;
import cl.etl.model.StagingClientes;
import cl.etl.transform.ValidarClienteStaging;

public class ValidarClienteStagingTest {

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
                PCollection<StagingClientes> revisionValidaciones = lista.apply(
                                "ValidarClientes",
                                ParDo.of(new ValidarClienteStaging()));

                // Assert
                // Verificar que el cliente salga con estado VALIDO
                PAssert.that(revisionValidaciones)
                                .satisfies(clientes -> {
                                        List<StagingClientes> resultado = new ArrayList<>();
                                        clientes.forEach(resultado::add);

                                        assertEquals(1, resultado.size());
                                        assertEquals(
                                                        Constantes.ESTADO_VALIDO,
                                                        resultado.get(0).getEstado());

                                        return null;
                                });

                pipeline.run().waitUntilFinish();
        }

        @Test
        void debeRechazarClienteConNombreVacio() {
                // Arrange
                StagingClientes cliente = new StagingClientes(
                                Constantes.ESTADO_VALIDO,
                                "1,,ana@ejemplo.cl");
                cliente.setIdCliente(1L);
                cliente.setNombre("");
                cliente.setEmail("ana@ejemplo.cl");

                // Act
                Pipeline pipeline = Pipeline.create();

                PCollection<StagingClientes> lista = pipeline
                                .apply("CrearCliente", Create.of(cliente))
                                .setCoder(new StagingCoder());
                PCollection<StagingClientes> revisionValidaciones = lista.apply(
                                "ValidarClientes",
                                ParDo.of(new ValidarClienteStaging()));

                // Assert
                // Verificar que el cliente salga con estado ESTADO_RECHAZADO
                PAssert.that(revisionValidaciones)
                                .satisfies(clientes -> {
                                        List<StagingClientes> resultado = new ArrayList<>();
                                        clientes.forEach(resultado::add);

                                        assertEquals(1, resultado.size());
                                        assertEquals(
                                                        Constantes.ESTADO_RECHAZADO,
                                                        resultado.get(0).getEstado());
                                        assertEquals(
                                                        Constantes.MR_NOMBRE_INVALIDO,
                                                        resultado.get(0).getMotivoRechazo());

                                        return null;
                                });

                pipeline.run().waitUntilFinish();
        }

        @Test
        void debeRechazarClienteConEmailVacio() {
                // Arrange
                StagingClientes cliente = new StagingClientes(
                                Constantes.ESTADO_VALIDO,
                                "1,Ana,");
                cliente.setIdCliente(1L);
                cliente.setNombre("Ana");
                cliente.setEmail("");

                // Act
                Pipeline pipeline = Pipeline.create();

                PCollection<StagingClientes> lista = pipeline
                                .apply("CrearCliente", Create.of(cliente))
                                .setCoder(new StagingCoder());
                PCollection<StagingClientes> revisionValidaciones = lista.apply(
                                "ValidarClientes",
                                ParDo.of(new ValidarClienteStaging()));

                // Assert
                // Verificar que el cliente salga con estado ESTADO_RECHAZADO
                PAssert.that(revisionValidaciones)
                                .satisfies(clientes -> {
                                        List<StagingClientes> resultado = new ArrayList<>();
                                        clientes.forEach(resultado::add);

                                        assertEquals(1, resultado.size());
                                        assertEquals(
                                                        Constantes.ESTADO_RECHAZADO,
                                                        resultado.get(0).getEstado());
                                        assertEquals(
                                                        Constantes.MR_EMAIL_INVALIDO,
                                                        resultado.get(0).getMotivoRechazo());

                                        return null;
                                });

                pipeline.run().waitUntilFinish();
        }

        @Test
        void debeRechazarClienteConEmailInvalido() {
                // Arrange
                StagingClientes cliente = new StagingClientes(
                                Constantes.ESTADO_VALIDO,
                                "1,Ana,ana@@ejemplo");
                cliente.setIdCliente(1L);
                cliente.setNombre("Ana");
                cliente.setEmail("ana@@ejemplo");

                // Act
                Pipeline pipeline = Pipeline.create();

                PCollection<StagingClientes> lista = pipeline
                                .apply("CrearCliente", Create.of(cliente))
                                .setCoder(new StagingCoder());
                PCollection<StagingClientes> revisionValidaciones = lista.apply(
                                "ValidarClientes",
                                ParDo.of(new ValidarClienteStaging()));

                // Assert
                // Verificar que el cliente salga con estado ESTADO_RECHAZADO
                PAssert.that(revisionValidaciones)
                                .satisfies(clientes -> {
                                        List<StagingClientes> resultado = new ArrayList<>();
                                        clientes.forEach(resultado::add);

                                        assertEquals(1, resultado.size());
                                        assertEquals(
                                                        Constantes.ESTADO_RECHAZADO,
                                                        resultado.get(0).getEstado());
                                        assertEquals(
                                                        Constantes.MR_EMAIL_INVALIDO,
                                                        resultado.get(0).getMotivoRechazo());

                                        return null;
                                });

                pipeline.run().waitUntilFinish();
        }
}
