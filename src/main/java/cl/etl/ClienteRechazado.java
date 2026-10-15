
package cl.etl;

import java.util.Objects;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class ClienteRechazado {

    public String linea;
    public String motivo;

    public ClienteRechazado() {
    }

    public ClienteRechazado(String linea, String motivo) {
        this.linea = linea;
        this.motivo = motivo;
    }

    @Override
    public String toString() {
        return "ClienteRechazado{linea='" + linea
                + "', motivo='" + motivo + "'}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof ClienteRechazado otro))
            return false;

        return Objects.equals(linea, otro.linea)
                && Objects.equals(motivo, otro.motivo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linea, motivo);
    }
}
