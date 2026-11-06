package cl.etl;

import org.apache.beam.sdk.transforms.DoFn;

import cl.etl.model.StagingClientes;

public class MostrarStaging extends DoFn<StagingClientes, StagingClientes> {

    @ProcessElement
    public void procesar(
            @Element StagingClientes scliente) {
      
                System.out.println(scliente);
    }
}
