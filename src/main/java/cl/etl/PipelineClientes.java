
package cl.etl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.etl.model.ResumenETL;
import cl.etl.model.StagingClientes;
import cl.etl.persistencia.PersisteStaging;
import cl.etl.transform.DetectarDuplicadosStaging;
import cl.etl.transform.ParsearClienteStaging;
import cl.etl.transform.ValidarClienteStaging;

/************************************
 * persistencia staging, upsert en DB
 ************************************/
public class PipelineClientes {
        private static final Logger log = LoggerFactory.getLogger(PipelineClientes.class);

        // TODO cambiar a properties
        public static String input = "/datos/clientes.csv";

        public ResumenETL procesar(long procesoId) {
                try {
                        Pipeline pipeline = Pipeline.create();
                        ParsearClienteStaging parsearCliente = new ParsearClienteStaging();

                        PCollection<StagingClientes> clientesParseados = pipeline
                                        .apply("ReadFromCSV", TextIO.read().from(input))
                                        .apply("ExcluirEncabezado",
                                                        Filter.by(linea -> !linea.equals("id,nombre,email")))
                                        .apply("parsearClientes", ParDo.of(parsearCliente))
                                        .setCoder(new StagingCoder());

                        PCollection<KV<Long, StagingClientes>> clientesPorId = clientesParseados
                                        .apply("AsignarIdComoClave",
                                                        WithKeys.of(StagingClientes::getIdCliente))
                                        .setCoder(KvCoder.of(
                                                        VarLongCoder.of(),
                                                        new StagingCoder()));
                        PCollection<KV<Long, Iterable<StagingClientes>>> gruposPorId = clientesPorId.apply(
                                        "AgruparPorId",
                                        GroupByKey.create());
                        PCollection<StagingClientes> revisionDuplicados = gruposPorId.apply(
                                        "DetectarDuplicados",
                                        ParDo.of(new DetectarDuplicadosStaging()));

                        PCollection<StagingClientes> revisionValidaciones = revisionDuplicados.apply(
                                        "ValidarClientes",
                                        ParDo.of(new ValidarClienteStaging()));

                        revisionValidaciones.apply(
                                        "MostrarStaging",
                                        ParDo.of(new MostrarStaging()));
                        revisionValidaciones.apply("PersistenciaValidos",

                                        ParDo.of(new PersisteStaging(procesoId)));

                        // Count.globally()
                        PipelineResult resultado = pipeline.run();
                        resultado.waitUntilFinish();

                        MetricQueryResults metricas = resultado.metrics().queryMetrics(
                                        MetricsFilter.builder().build());

                        // Recuperar los valores de los contadores
                        int leidos = 0;
                        int validos = 0;
                        int rechazados = 0;

                        for (MetricResult<Long> m : metricas.getCounters()) {
                                log.info(
                                                "Métrica: {} | committed: {} | attempted: {}",
                                                m.getName().getName(),
                                                m.getCommitted(),
                                                m.getAttempted());
                                switch (m.getName().getName()) {
                                        case "leidos":
                                                leidos = m.getCommitted().intValue();
                                                break;
                                        case "validos":
                                                validos = m.getCommitted().intValue();
                                                break;
                                        case "rechazados":
                                                rechazados += m.getCommitted().intValue();
                                                break;
                                }
                        }

                        ResumenETL resumen = new ResumenETL(leidos, validos, rechazados);

                        log.info("Resumen ETL: {}", resumen);
                        return resumen;
                } catch (Exception e) {
                        log.error("Error ejecutando pipeline ETL {}", procesoId, e);
                        throw new RuntimeException("Falló la ejecución ETL", e);
                }
        }
}
