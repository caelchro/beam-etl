
package cl.etl;

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
import org.apache.beam.sdk.values.TupleTagList;

import cl.etl.transform.DetectarDuplicados;
import cl.etl.transform.ParsearCliente;
import cl.etl.transform.ValidarCliente;

public class PipelineClientes {

    public static String input = "/datos/clientes.csv";
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        ParsearCliente parsearCliente = new ParsearCliente();

        PCollection<Cliente> clientesParseados = pipeline
                .apply("ReadFromCSV", TextIO.read().from(input))
                .apply("ExcluirEncabezado",
                        Filter.by(linea -> !linea.equals("id,nombre,email")))
                .apply("parsearClientes", ParDo.of(parsearCliente)).setCoder(new ClienteCoder());

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
                                DetectarDuplicados.CLIENTES_VALIDOS,
                                TupleTagList.of(DetectarDuplicados.CLIENTES_RECHAZADOS)));
        PCollection<Cliente> sinDuplicados = resultadoDuplicados.get(DetectarDuplicados.CLIENTES_VALIDOS);

        PCollectionTuple resultado = sinDuplicados.apply(
                "ValidarClientes",
                ParDo.of(new ValidarCliente())
                        .withOutputTags(
                                ValidarCliente.CLIENTES_VALIDOS,
                                TupleTagList.of(ValidarCliente.CLIENTES_RECHAZADOS)));

        PCollection<Cliente> validos = resultado.get(ValidarCliente.CLIENTES_VALIDOS);

        PCollection<ClienteRechazado> rechazados = PCollectionList.of(
                resultadoDuplicados.get(DetectarDuplicados.CLIENTES_RECHAZADOS)).and(
                        resultado.get(ValidarCliente.CLIENTES_RECHAZADOS))
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
