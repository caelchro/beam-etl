beam-etl
Java · Apache Beam · Maven
Pipeline ETL local para procesar datos de clientes desde un archivo CSV.

¿Qué implementa?
- Lectura y parseo de registros CSV.
- Validación de datos y correo electrónico.
- Detección de identificadores duplicados.
- Separación de registros válidos y rechazados, con sus motivos.
Estructura del proyecto
src/main/java/cl/etl/
├── Cliente.java
├── ClienteCoder.java
├── ClienteRechazado.java
├── PipelineClientes.java
└── transform/
    ├── ParsearCliente.java
    ├── ValidarCliente.java
    └── DetectarDuplicados.java


Evolución del proyecto
- PipelineClientes1: carga de datos on the fly.
- PipelineClientes2: validaciones en una versión anterior.
- PipelineClientes: pipeline actual con responsabilidades separadas.


mvn compile exec:java "-Dexec.mainClass=cl.etl.PipelineClientes"  