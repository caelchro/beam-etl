package cl.etl.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import cl.etl.Constantes;
import cl.etl.model.ResumenETL;
import cl.etl.model.StagingClientes;

public class DetectarDuplicadosStaging extends DoFn<KV<Long, Iterable<StagingClientes>>, StagingClientes> {

    @ProcessElement
    public void procesar(
            @Element KV<Long, Iterable<StagingClientes>> grupo,
            OutputReceiver<StagingClientes> salida) {

        List<StagingClientes> clientes = new ArrayList<>();
        grupo.getValue().forEach(clientes::add);
        if (clientes.isEmpty())
            return;
        if (clientes.size() > 1) {
            clientes.forEach(cliente -> {
                StagingClientes copia = cliente.copiar();
                ResumenETL.RECHAZADOS.inc();

                copia.rechazar(
                        Constantes.ESTADO_RECHAZADO,
                        Constantes.MR_DUPLICADO);

                salida.output(copia);
            });
        } else {
            salida.output(clientes.get(0));
        }
    }
}
