package cl.etl.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import cl.etl.Cliente;
import cl.etl.ClienteRechazado;

public class DetectarDuplicados extends DoFn<KV<Long, Iterable<Cliente>>, Cliente> {
    public static final TupleTag<Cliente> CLIENTES_VALIDOS = new TupleTag<Cliente>() {
    };

    public static final TupleTag<ClienteRechazado> CLIENTES_RECHAZADOS = new TupleTag<ClienteRechazado>() {
    };

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
