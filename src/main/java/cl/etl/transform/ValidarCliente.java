package cl.etl.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import cl.etl.Cliente;
import cl.etl.ClienteRechazado;

public class ValidarCliente extends DoFn<Cliente, Cliente> {

    public static final TupleTag<Cliente> CLIENTES_VALIDOS = new TupleTag<Cliente>() {
    };

    public static final TupleTag<ClienteRechazado> CLIENTES_RECHAZADOS = new TupleTag<ClienteRechazado>() {
    };

    @ProcessElement
    public void procesar(
            @Element Cliente cliente,
            MultiOutputReceiver salida) {
        boolean nombreInvalido = cliente.getNombre() == null
                || cliente.getNombre().isBlank();

        boolean emailInvalido = cliente.getEmail() == null
                || !cliente.getEmail().matches(
                        "^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
        if (nombreInvalido) {
            salida.get(CLIENTES_RECHAZADOS).output(
                    new ClienteRechazado(cliente.toString(), "Nombre vacío"));
        } else if (emailInvalido) {
            salida.get(CLIENTES_RECHAZADOS).output(
                    new ClienteRechazado(cliente.toString(), "Email inválido"));
        } else {
            salida.get(CLIENTES_VALIDOS).output(cliente);
        }
    }
}
