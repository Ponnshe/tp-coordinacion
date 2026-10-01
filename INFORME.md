# Informe del Trabajo Práctico: Coordinación

## Coordinación entre Instancias de Sum y Aggregation

El sistema utiliza RabbitMQ como middleware para garantizar la comunicación asincrónica y la escalabilidad. La coordinación principal entre los nodos `Sum` y `Aggregation` se basa en dos mecanismos arquitectónicos:

1. **Protocolo "Inner" y Topología Dinámica:**
   Para evitar suposiciones rígidas sobre la red (tales como nombres de host estáticos o cantidades fijas de nodos), el sistema emplea una fase inicial de reconocimiento (`SYN`). Al iniciarse, cada nodo procesador emite un mensaje que permite a los nodos posteriores (`Aggregators` y `Joiner`) descubrir dinámicamente la topología y cantidad de instancias operativas.
   
2. **In-Band y Out-of-Band Signaling (Barrera de Sincronización de EOFs):**
   La detección del fin de ingesta es un desafío en entornos distribuidos. El sistema lo aborda mediante una combinación de señalización:
   - **Broadcast entre hermanos:** Cuando un nodo `Sum` detecta un mensaje `EOF` en la cola in-band compartida, consolida sus resultados locales y emite una notificación de control (`out-of-band`) hacia sus hermanos mediante un Exchange dedicado (`controlExchange`). Esto asegura que todas las instancias de `Sum` tomen conocimiento de la finalización de la carga de datos sin que un nodo acapare y elimine el único `EOF` disponible en la cola.
   - **Barrera Estricta de Sincronización:** Los `Aggregators` no envían resultados parciales ante la recepción de un único `EOF`. En su lugar, utilizan un modelo de barrera (*Synchronization Barrier*): interceptan los `EOFs` individuales emitidos por cada `Sum` y únicamente cuando el conjunto de identificadores recibidos coincide de manera estricta con los nodos registrados durante la fase `SYN`, consolidan y emiten su Top Parcial hacia el `Joiner`.

## Escalabilidad del Sistema

El diseño del sistema distribuye la carga horizontalmente y elimina los cuellos de botella por contención.

### 1. Escalabilidad respecto a Múltiples Clientes (Concurrencia)
El sistema mantiene un estado aislado por sesión (`SessionState`). Al ingresar un lote de datos a través del `Gateway`, se le asigna a cada cliente un `sessionID` único. Todos los nodos de la topología separan rigurosamente el procesamiento, la acumulación matemática y las barreras de sincronización apoyándose en este identificador. Esto permite la resolución de consultas de múltiples clientes de forma enteramente concurrente y paralela sobre la misma red de contenedores, previniendo la contaminación de datos entre clientes.

### 2. Escalabilidad respecto a Grandes Volúmenes de Datos (Big Data)
La carga masiva se procesa de forma distribuida bajo el paradigma *MapReduce*:
- **Round-Robin en la Ingesta:** La cola principal del sistema distribuye uniformemente la carga de trabajo entre todas las réplicas de `Sum` disponibles, asegurando un uso equitativo de los recursos de CPU del clúster.
- **Deterministic Hashing:** Para evitar un tráfico de red redundante (O(N) *broadcasts*), las instancias de `Sum` operan como *Mappers* locales. Acumulan las cantidades en memoria y posteriormente aplican una función de Hash Determinístico (FNV) sobre el identificador de la fruta. Esto asegura que todos los datos pertenecientes a una misma especie frutal sean dirigidos exclusivamente hacia una única y misma instancia de `Aggregator`, evitando sobrecarga de enrutamiento y procesamiento duplicado.

### 3. Tolerancia a la Cantidad de Controles y Contenedores (Graceful Shutdown)
Para garantizar la integridad del sistema ante la creación o destrucción dinámica de clústeres masivos, la arquitectura prescinde del uso de bloqueos tradicionales (`sync.Mutex`). En su lugar, se implementa el **Patrón Actor**: los mensajes provenientes de RabbitMQ se reinyectan hacia un canal nativo de Go (*Channel*), centralizando la mutación del estado en un único hilo de ejecución síncrono.
Esta decisión de diseño otorga soporte natural para apagados controlados (*Graceful Shutdown*). Al recibir la señal `SIGTERM` por parte del orquestador, los contenedores detienen su consumo de RabbitMQ pero mantienen activa la red hasta vaciar la totalidad de los mensajes en tránsito y enviar sus notificaciones finales, garantizando la liberación limpia de memoria y *sockets* sin pérdida de datos.
