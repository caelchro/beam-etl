package cl.etl.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.Constantes;
import cl.etl.model.ResumenETL;
import cl.etl.model.StagingClientes;

public class ParsearClienteStaging extends DoFn<String, StagingClientes> {
    private static final Logger log = LoggerFactory.getLogger(ParsearClienteStaging.class);

    @ProcessElement
    public void procesar(
            @Element String linea,
            OutputReceiver<StagingClientes> salida) {

        ResumenETL.LEIDOS.inc();
        String[] campos = linea.split(",", -1);

        StagingClientes stagingCliente = new StagingClientes(Constantes.ESTADO_VALIDO, linea);
        if (campos.length != 3) {
            ResumenETL.RECHAZADOS.inc();
            stagingCliente.rechazar(Constantes.ESTADO_RECHAZADO, Constantes.MR_CANTIDAD_COLUMNAS);
            salida.output(stagingCliente);
            return;
        }

        long id;

        try {
            id = Long.parseLong(campos[0].trim());
            stagingCliente.setIdCliente(id);
        } catch (NumberFormatException e) {
            ResumenETL.RECHAZADOS.inc();
            stagingCliente.setId(-1);
            stagingCliente.rechazar(Constantes.ESTADO_RECHAZADO, Constantes.MR_ID_INVALIDO);
            log.info("rechazo por id invalido:" + campos[0].trim());
        }

        String nombre = campos[1].trim();
        String email = campos[2].trim();
        stagingCliente.setEmail(email);
        stagingCliente.setNombre(nombre);

        salida.output(stagingCliente);
    }
}
