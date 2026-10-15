package cl.etl.transform;

import org.apache.beam.sdk.transforms.DoFn;

import cl.etl.Cliente;

public class ParsearCliente extends DoFn<String, Cliente> {
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
