package cl.etl.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.Constantes;
import cl.etl.model.ResumenETL;
import cl.etl.model.StagingClientes;

public class ValidarClienteStaging extends DoFn<StagingClientes, StagingClientes> {

    private static final Logger log = LoggerFactory.getLogger(ValidarClienteStaging.class);
    @ProcessElement
    public void procesar(
            @Element StagingClientes scliente,
            OutputReceiver<StagingClientes> salida) {
        if (Constantes.ESTADO_RECHAZADO.equals(scliente.getEstado())) {
            salida.output(scliente);
            return;
        }

        boolean nombreInvalido = scliente.getNombre() == null
                || scliente.getNombre().isBlank();

        boolean emailInvalido = scliente.getEmail() == null
                || !scliente.getEmail().matches(
                        "^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
        StagingClientes copia = scliente.copiar();
        if (nombreInvalido) {
            ResumenETL.RECHAZADOS.inc();
            copia.rechazar(Constantes.ESTADO_RECHAZADO, Constantes.MR_NOMBRE_INVALIDO);
            salida.output(copia);
        } else if (emailInvalido) {
            ResumenETL.RECHAZADOS.inc();
            copia.rechazar(Constantes.ESTADO_RECHAZADO, Constantes.MR_EMAIL_INVALIDO);
            salida.output(copia);
        } else {
            ResumenETL.VALIDOS.inc();
            salida.output(scliente);
        }
    }
}
