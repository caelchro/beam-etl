package cl.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import cl.etl.coder.StagingCoder;
import cl.etl.model.StagingClientes;
import cl.etl.transform.ParsearClienteStaging;

public class ParsearClienteStagingTest {
    @Test
    void datosOK() {
        // Arrange
        final String input = Propiedades.PROPERTIES.getProperty("etl.inputOk");///"/datos/clientes.csv";
        
        // Act
        Pipeline pipeline = Pipeline.create();
        ParsearClienteStaging parsearCliente = new ParsearClienteStaging();

        PCollection<StagingClientes> clientesParseados = pipeline
                                .apply("ReadFromCSV", TextIO.read().from(input))
                                .apply("ExcluirEncabezado",
                                                Filter.by(linea -> !linea.equals("id,nombre,email")))
                                .apply("parsearClientes", ParDo.of(parsearCliente))
                                .setCoder(new StagingCoder());


        // Assert
        // Verificar que 3 clientes salgan con estado VALIDO
        PAssert.that(clientesParseados)
        .satisfies(clientes -> {        
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_VALIDO, cliente.getEstado());
            assertEquals("", cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
        }

        assertEquals(3, ids.size());
        assertTrue(ids.containsAll(List.of(1L, 2L, 3L)));
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
    @Test
    void cantidadColumnasIncorrecta() {
        // Arrange
        
        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
            .apply(Create.of("1,Ana"))
            .apply(ParDo.of(new ParsearClienteStaging()))
            .setCoder(new StagingCoder());


        // Assert
        // Verificar que 1 cliente salga con estado Rechazado: MR_CANTIDAD_COLUMNAS
        PAssert.that(lista)
        .satisfies(clientes -> {
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_RECHAZADO, cliente.getEstado());
            assertEquals(Constantes.MR_CANTIDAD_COLUMNAS, cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
            assertEquals(cliente.getIdCliente(), 0L);
        }

        assertEquals(1, ids.size());
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
    
    @Test
    void cantidadColumnasExcedida() {
        // Arrange
        
        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
            .apply(Create.of("1,Ana,aa@aa.cl,extra"))
            .apply(ParDo.of(new ParsearClienteStaging()))
            .setCoder(new StagingCoder());


        // Assert
        // Verificar que 1 cliente salga con estado Rechazado: MR_CANTIDAD_COLUMNAS
        PAssert.that(lista)
        .satisfies(clientes -> {
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_RECHAZADO, cliente.getEstado());
            assertEquals(Constantes.MR_CANTIDAD_COLUMNAS, cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
            assertEquals(cliente.getIdCliente(), 0L);
        }

        assertEquals(1, ids.size());
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
    
    @Test
    void idNoNumerico() {
        // Arrange
        
        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
            .apply(Create.of("a,Ana,aa@aa.cl"))
            .apply(ParDo.of(new ParsearClienteStaging()))
            .setCoder(new StagingCoder());


        // Assert
        // Verificar que 1 cliente salga con estado Rechazado: MR_ID_INVALIDO
        PAssert.that(lista)
        .satisfies(clientes -> {
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_RECHAZADO, cliente.getEstado());
            assertEquals(Constantes.MR_ID_INVALIDO, cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
            assertEquals(cliente.getIdCliente(), -1L);
        }

        assertEquals(1, ids.size());
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
    
    @Test
    void idVacio() {
        // Arrange
        
        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
            .apply(Create.of(",Ana,aa@aa.cl"))
            .apply(ParDo.of(new ParsearClienteStaging()))
            .setCoder(new StagingCoder());


        // Assert
        // Verificar que 1 cliente salga con estado Rechazado: MR_ID_INVALIDO
        PAssert.that(lista)
        .satisfies(clientes -> {
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_RECHAZADO, cliente.getEstado());
            assertEquals(Constantes.MR_ID_INVALIDO, cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
            assertEquals(cliente.getIdCliente(), -1L);
        }

        assertEquals(1, ids.size());
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
    
    @Test
    void camposConEspacios() {
        // Arrange
        
        // Act
        Pipeline pipeline = Pipeline.create();

        PCollection<StagingClientes> lista = pipeline
            .apply(Create.of("1 , Ana , ana@mail.com"))
            .apply(ParDo.of(new ParsearClienteStaging()))
            .setCoder(new StagingCoder());


        // Assert
        // Verificar que 1 cliente salga con estado valido
        PAssert.that(lista)
        .satisfies(clientes -> {
        List<Long> ids = new ArrayList<>();

        for (StagingClientes cliente : clientes) {
            assertEquals(Constantes.ESTADO_VALIDO, cliente.getEstado());
            assertEquals("", cliente.getMotivoRechazo());
            ids.add(cliente.getIdCliente());
            assertEquals(cliente.getIdCliente(), 1L);
            assertEquals("Ana", cliente.getNombre());
            assertEquals("ana@mail.com", cliente.getEmail());
        }

        assertEquals(1, ids.size());
        return null;
    });

        pipeline.run().waitUntilFinish();
    }
}
