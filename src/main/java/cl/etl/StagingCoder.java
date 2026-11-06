package cl.etl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

import cl.etl.model.StagingClientes;

public class StagingCoder extends AtomicCoder<StagingClientes> {

    private static final VarLongCoder ID_CODER = VarLongCoder.of();
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

    private void encodeString(String valor, OutputStream out) throws IOException {
        STRING_CODER.encode(valor != null ? valor : "", out);
    }
    private void encodeLong(Long valor, OutputStream out) throws IOException {
        ID_CODER.encode(valor != null ? valor : 0L, out);
    }

    @Override
    public void encode(StagingClientes c, OutputStream out) throws IOException {
        // ID_CODER.encode(c.getIdCliente(), out);
        encodeLong(c.getIdCliente(), out);
        encodeString(c.getNombre(), out);
        encodeString(c.getEmail(), out);
        encodeString(c.getEstado(), out);
        encodeString(c.getMotivoRechazo(), out);
        encodeString(c.getLinea(), out);
    }

    @Override
    public StagingClientes decode(InputStream in) throws IOException {
        Long idCliente = ID_CODER.decode(in);
        String nombre = STRING_CODER.decode(in);
        String email = STRING_CODER.decode(in);
        String estado = STRING_CODER.decode(in);
        String motivo = STRING_CODER.decode(in);
        String linea = STRING_CODER.decode(in);

        return new StagingClientes(idCliente, nombre, email, estado, motivo, linea);
    }
}
