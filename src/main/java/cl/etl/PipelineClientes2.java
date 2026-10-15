
package cl.etl;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class PipelineClientes2 {

    public static String input = "/datos/clientes.csv";

    static final TupleTag<Cliente> CLIENTES_VALIDOS = new TupleTag<Cliente>() {
    };

    static final TupleTag<ClienteRechazado> CLIENTES_RECHAZADOS = new TupleTag<ClienteRechazado>() {
    };

    static class ValidarCliente extends DoFn<Cliente, Cliente> {

        @ProcessElement
        public void procesar(
                @Element Cliente cliente,
                MultiOutputReceiver salida) {

            if (cliente.getNombre() == null || cliente.getNombre().isBlank()) {
                salida.get(CLIENTES_RECHAZADOS).output(
                        new ClienteRechazado(cliente.toString(), "Nombre vacío"));
            } else {
                salida.get(CLIENTES_VALIDOS).output(cliente);
            }
        }
    }

    static class DetectarDuplicados
            extends DoFn<KV<Long, Iterable<Cliente>>, Cliente> {

        @ProcessElement
        public void procesar(
                @Element KV<Long, Iterable<Cliente>> grupo,
                MultiOutputReceiver salida) {

            List<Cliente> clientes = new ArrayList<>();
            grupo.getValue().forEach(clientes::add);

            if (clientes.size() > 1) {
                clientes.forEach(cliente -> salida.get(CLIENTES_RECHAZADOS).output(
                        new ClienteRechazado(
                                cliente.toString(),
                                "ID duplicado: " + grupo.getKey())));
            } else {
                salida.get(CLIENTES_VALIDOS).output(clientes.get(0));
            }
        }
    }

    static class ParsearCliente extends DoFn<String, Cliente> {

        @ProcessElement
        public void procesar(
                @Element String linea,
                OutputReceiver<Cliente> salida) {

            String[] campos = linea.split(",", -1);

            long id = Long.parseLong(campos[0].trim());
            String nombre = campos[1].trim();
            String email = campos[2].trim();

            Cliente cliente = new Cliente(id, nombre, email);

            salida.output(cliente);
        }
    }

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<Cliente> clientesParseados = pipeline
                .apply("ReadFromCSV", TextIO.read().from(input))
                .apply("ExcluirEncabezado",
                        Filter.by(linea -> !linea.equals("id,nombre,email")))
                .apply("parsearClientes", ParDo.of(new ParsearCliente())).setCoder(new ClienteCoder());

        PCollection<KV<Long, Cliente>> clientesPorId = clientesParseados
                .apply("AsignarIdComoClave",
                        WithKeys.of(Cliente::getId))
                .setCoder(
                        KvCoder.of(
                                VarLongCoder.of(),
                                new ClienteCoder()));

        PCollection<KV<Long, Iterable<Cliente>>> gruposPorId = clientesPorId.apply(
                "AgruparPorId",
                GroupByKey.create());
        PCollectionTuple resultadoDuplicados = gruposPorId.apply(
                "DetectarDuplicados",
                ParDo.of(new DetectarDuplicados())
                        .withOutputTags(
                                CLIENTES_VALIDOS,
                                TupleTagList.of(CLIENTES_RECHAZADOS)));
        PCollection<Cliente> sinDuplicados = resultadoDuplicados.get(CLIENTES_VALIDOS);

        PCollectionTuple resultado = sinDuplicados.apply(
                "ValidarClientes",
                ParDo.of(new ValidarCliente())
                        .withOutputTags(
                                CLIENTES_VALIDOS,
                                TupleTagList.of(CLIENTES_RECHAZADOS)));

        PCollection<Cliente> validos = resultado.get(CLIENTES_VALIDOS);

        PCollection<ClienteRechazado> rechazados = PCollectionList.of(
                resultadoDuplicados.get(CLIENTES_RECHAZADOS)).and(
                        resultado.get(CLIENTES_RECHAZADOS))
                .apply("UnirRechazados", Flatten.pCollections());
        validos.apply("MostrarValidos", ParDo.of(
                new DoFn<Cliente, Void>() {
                    @ProcessElement
                    public void procesar(@Element Cliente cliente) {
                        System.out.println("VÁLIDO: " + cliente);
                    }
                }));

        rechazados.apply("MostrarRechazados", ParDo.of(
                new DoFn<ClienteRechazado, Void>() {
                    @ProcessElement
                    public void procesar(@Element ClienteRechazado cliente) {
                        System.out.println("RECHAZADO: " + cliente);
                    }
                }));
        pipeline.run().waitUntilFinish();
    }
}
