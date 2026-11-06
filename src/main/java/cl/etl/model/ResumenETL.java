package cl.etl.model;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

public class ResumenETL {

    public static final Counter LEIDOS = Metrics.counter(ResumenETL.class, "leidos");

    public static final Counter VALIDOS = Metrics.counter(ResumenETL.class, "validos");

    public static final Counter RECHAZADOS = Metrics.counter(ResumenETL.class, "rechazados");

    private final int totalLeidos;
    private final int totalValidos;
    private final int totalRechazados;

    public ResumenETL(
            int totalLeidos,
            int totalValidos,
            int totalRechazados) {
        this.totalLeidos = totalLeidos;
        this.totalValidos = totalValidos;
        this.totalRechazados = totalRechazados;
    }

    public int getTotalLeidos() {
        return totalLeidos;
    }

    public int getTotalValidos() {
        return totalValidos;
    }

    public int getTotalRechazados() {
        return totalRechazados;
    }

    @Override
    public String toString() {
        return "ResumenETL [totalLeidos=" + totalLeidos + ", totalValidos=" + totalValidos + ", totalRechazados="
                + totalRechazados + "]";
    }
}
