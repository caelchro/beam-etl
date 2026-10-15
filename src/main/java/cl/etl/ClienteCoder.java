package cl.etl;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class ClienteCoder extends AtomicCoder<Cliente> {

    private static final VarLongCoder ID_CODER = VarLongCoder.of();
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

    @Override
    public void encode(Cliente cliente, OutputStream out) throws IOException {
        ID_CODER.encode(cliente.getId(), out);
        STRING_CODER.encode(cliente.getNombre(), out);
        STRING_CODER.encode(cliente.getEmail(), out);
    }

    @Override
    public Cliente decode(InputStream in) throws IOException {
        long id = ID_CODER.decode(in);
        String nombre = STRING_CODER.decode(in);
        String email = STRING_CODER.decode(in);

        return new Cliente(id, nombre, email);
    }
}
