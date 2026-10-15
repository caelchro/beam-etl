
package cl.etl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PipelineClientes1 {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        pipeline
            .apply("LeerClientes", Create.of(
                "ana",
                "pedro",
                "maria"
            ))
            .apply("NormalizarNombres",
                MapElements.into(TypeDescriptors.strings())
                    .via(nombre -> nombre.toUpperCase())
            )
            .apply("MostrarClientes",
                org.apache.beam.sdk.transforms.ParDo.of(
                    new org.apache.beam.sdk.transforms.DoFn<String, String>() {
                        @ProcessElement
                        public void procesar(
                                @Element String nombre,
                                OutputReceiver<String> salida) {
                            System.out.println("Cliente: " + nombre);
                            salida.output(nombre);
                        }
                    }
                )
            );

        pipeline.run().waitUntilFinish();
    }
}
