package cl.etl.model;

import java.util.Date;

public class ETLProceso {
    private long id;
    private Date fechaInicio;
    private Date fechaTermino;
    private int totalLeidos;
    private int totalValidos;
    private int totalRechazados;
    private String estado;
    private String mensajeError;

    public ETLProceso() {
    }

    public ETLProceso(long id, Date fechaInicio, Date fechaTermino,
            int totalLeidos, int totalValidos, int totalRechazados,
            String estado, String mensajeError) {
        this.id = id;
        this.fechaInicio = fechaInicio;
        this.fechaTermino = fechaTermino;
        this.totalLeidos = totalLeidos;
        this.totalValidos = totalValidos;
        this.totalRechazados = totalRechazados;
        this.estado = estado;
        this.mensajeError = mensajeError;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getFechaInicio() {
        return fechaInicio;
    }

    public void setFechaInicio(Date fechaInicio) {
        this.fechaInicio = fechaInicio;
    }

    public Date getFechaTermino() {
        return fechaTermino;
    }

    public void setFechaTermino(Date fechaTermino) {
        this.fechaTermino = fechaTermino;
    }

    public int getTotalLeidos() {
        return totalLeidos;
    }

    public void setTotalLeidos(int totalLeidos) {
        this.totalLeidos = totalLeidos;
    }

    public int getTotalValidos() {
        return totalValidos;
    }

    public void setTotalValidos(int totalValidos) {
        this.totalValidos = totalValidos;
    }

    public int getTotalRechazados() {
        return totalRechazados;
    }

    public void setTotalRechazados(int totalRechazados) {
        this.totalRechazados = totalRechazados;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getMensajeError() {
        return mensajeError;
    }

    public void setMensajeError(String mensajeError) {
        this.mensajeError = mensajeError;
    }
}
