package cl.etl.model;

import java.io.Serializable;
import java.util.Date;

public class StagingClientes implements Serializable {

    private long id;
    private long idProceso;
    private Long idCliente;
    private String linea;
    private String nombre;
    private String email;
    private String estado;
    private String motivoRechazo;

    private Date fechaCarga;

    public StagingClientes() {
    }

    public StagingClientes(String estado, String linea) {
        this.linea = linea;
        this.estado = estado;
    }

    public StagingClientes(String estado, String motivoRechazo, String linea) {
        this.linea = linea;
        this.estado = estado;
        this.motivoRechazo = motivoRechazo;
    }

    public StagingClientes(long idCliente, String nombre, String email) {
        this.idCliente = idCliente;
        this.nombre = nombre;
        this.email = email;
    }

    public StagingClientes(String estado, long idCliente, String nombre, String email, String linea) {
        this.linea = linea;
        this.estado = estado;
        this.idCliente = idCliente;
        this.nombre = nombre;
        this.email = email;
    }

    public StagingClientes(long id, long idProceso, long idCliente, String linea, String nombre, String email,
            String estado, String motivoRechazo, Date fechaCarga) {
        this.id = id;
        this.idProceso = idProceso;
        this.idCliente = idCliente;
        this.linea = linea;
        this.nombre = nombre;
        this.email = email;
        this.estado = estado;
        this.motivoRechazo = motivoRechazo;
        this.fechaCarga = fechaCarga;
    }

    public StagingClientes(Long idCliente, String nombre, String email, String estado, String motivoRechazo, String linea) {
        this.motivoRechazo = motivoRechazo;
        this.estado = estado;
        this.idCliente = idCliente;
        this.nombre = nombre;
        this.email = email;
        this.linea = linea;
    }

    public void rechazar(String estadoRechazado, String motivoRechazo) {
        this.estado = estadoRechazado;
        this.motivoRechazo = motivoRechazo;
    }

    public StagingClientes copiar() {
        StagingClientes copia = new StagingClientes();

        copia.setId(this.id);
        copia.setIdProceso(this.idProceso);
        copia.setIdCliente(this.idCliente);
        copia.setLinea(this.linea);
        copia.setNombre(this.nombre);
        copia.setEmail(this.email);
        copia.setEstado(this.estado);
        copia.setMotivoRechazo(this.motivoRechazo);
        copia.setFechaCarga(this.fechaCarga);

        return copia;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getIdProceso() {
        return idProceso;
    }

    public void setIdProceso(long idProceso) {
        this.idProceso = idProceso;
    }

    public Long getIdCliente() {
        return idCliente;
    }

    public void setIdCliente(Long idCliente) {
        this.idCliente = idCliente;
    }

    public String getLinea() {
        return linea;
    }

    public void setLinea(String linea) {
        this.linea = linea;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getMotivoRechazo() {
        return motivoRechazo;
    }

    public void setMotivoRechazo(String motivoRechazo) {
        this.motivoRechazo = motivoRechazo;
    }

    public Date getFechaCarga() {
        return fechaCarga;
    }

    public void setFechaCarga(Date fechaCarga) {
        this.fechaCarga = fechaCarga;
    }

    @Override
    public String toString() {
        return "StagingClientes{idCliente=" + idCliente
                + ", nombre='" + nombre + '\''
                + ", email='" + email + '\''
                + ", estado='" + estado + '\''
                + ", motivoRechazo='" + motivoRechazo + '\''
                + ", linea='" + linea + '\''
                + '}';
    }
}
