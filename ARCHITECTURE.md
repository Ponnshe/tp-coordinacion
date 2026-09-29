# Consideraciones generales
- Cada mensaje enviado a través del MOM tiene que usar ACK para verificar que fue revisido al menos una vez.

# Fase de reconocimiento (Syn Phase)
- **Sum**: Al arancar un `Sum`, este mismo hace un broadcast de su mensaje `SYN(sum_id)` con el objetivo de que todo los `Aggregation` sepan de su existencia 

- **Aggregation**: Al arancar un `Aggregation`, este mismo hace un broadcast de su mensaje `SYN(agg_id)` con el objetivo de que el `Join` sepa de su existencia 

# Pre protocolo (Fase de reconocimiento)
## Sum
- Ejecuta su fase de reconocimiento

## Aggregation
- Ejecuta su fase de reconocimiento

# Durante una operación
- El cliente lee el archivo
- Envia cada tupla `(fruit, amount)` a través del `Socket TCP/IP` al `gateway`
- El `gateway` recibe la request de los clientes, les asigna un `client_id` y les envia ese mensaje a los `Sum`. 
- El trabajo es repartido usando `fruit` como key, es decir, es `fruit` lo que se hashea y el MOM decide a quien asignarle ese hash
- Los `Sum` van haciendo su operación con los requests recibidos

## Llega un EOF
- **Propagacion de EOF en Sum**: SOLO cuando un `Sum` recibe un EOF a través de la queue, entonces hace un broadcast de ese EOF usando el Exchange que comunica todos los `Sum`
- **Caso EOF general Sum**: Cuando un `Sum` recibe un EOF envia las tuplas `(fruit, amount, client_id)` a los `Aggregation` y después hace un broadcast del mensaje `EOF(sum_id)`
- **Aggregation**: Los `Aggregation` reciben los totales de los `Sum` y esperan recibir un `EOF(sum_id)` con el `ID` de todos los `Sum` que tienen registrados. 
  - Una vez recibe el `EOF(sum_id)` de todos los `Sum` que tiene registrados entonces envía su `partial_top` y luego envía también su `EOF(agg_id)`
- **Join**: Va calculando el top con los `partial_top` que va recibiendo y solo envía el `fruit_top` cuando recibe el `EOF(agg_id)` de todos los `Aggregation` que tiene registrado

# Notas adicionales
- No existe riesgo de Race Condition del EOF entre los `Sum` porque las operaciones están serializadas debido a la cola, es decir, antes de que el EOF llegue al primer `Sum` se tuvo que haber repartido todo el trabajo en los nodos por lo que en sus colas internas tendrán que procesar todos los demás mensajes ante de procesor el `EOF` que llega del Exchange
- Esta arquitectura/protocolo proporciona la ventaja de levantar nodos dinámicamente pues se van a integrar y "dar a conocer" apenas arranquen.

# Preguntas
- Si solo se tiene un tipo de fruta en el archivo ¿habrán nodos que queden sin trabajar?

# Sobre el EOF (In-band signaling)
Se mantiene la propagación in-band del EOF (el EOF viaja por la misma cola que los datos) para evitar *Race Conditions* de señalización fuera de banda.
1. Un nodo `Sum` (ej. Sum-1) extrae el EOF de la cola compartida en esquema Round-Robin.
2. Al estar en la misma cola, se garantiza matemáticamente que todas las frutas anteriores a ese EOF ya fueron desencoladas y asignadas a algún nodo.
3. El Sum-1 envía sus totales consolidados a los `Aggregators` y emite un broadcast a sus nodos hermanos (Sum-2, Sum-3, etc.).
4. Los hermanos reciben este broadcast asincrónicamente, terminan de procesar sus cargas pendientes, envían sus propios totales y emiten sus `EOF(sum_id)` correspondientes.

---

# Iteración 2: Optimización con MapReduce y Round-Robin
*Esta iteración mejora la distribución de carga y responde a la pregunta de escalabilidad cuando los datos están sesgados (ej. archivo de un solo tipo de fruta).*

## Modificaciones al Flujo de Datos
- **Gateway a Sum (Fase Map):** El ruteo **ya no se hace usando `fruit` como key**. En su lugar, el `Gateway` envía los datos a una **cola compartida** que consumen todos los nodos `Sum` mediante **Round-Robin**. 
  - *Beneficio:* Garantiza que el 100% de la CPU del clúster `Sum` trabaje por igual (aprovechando todas las réplicas) sin importar el contenido del archivo. Si hay $N$ entradas y $S$ nodos, cada nodo procesará exactamente $N/S$ frutas.
- **Sum (Operación Local):** Cada nodo `Sum` acumula las cantidades por fruta en su propia memoria (estado local). **No** envía un mensaje al `Aggregator` por cada fruta recibida.
- **Sum a Aggregator (Fase Reduce):** 
  - **Cuándo se envía:** Únicamente al procesar el mensaje in-band de `EOF`.
  - **Cómo se rutea:** Aquí **SÍ** se usa la `fruit` como key de ruteo (Hashing por fruta). 
  - *Beneficio:* Se evita el procesamiento redundante. Cada `Aggregator` procesará a lo sumo $S$ subtotales por fruta (uno proveniente de cada nodo `Sum`), consolidando la suma final eficientemente.

## Decisión de Diseño: Batching y Prefetch Count en RabbitMQ

**Contexto:** Por defecto, la lectura de mensajes desde RabbitMQ puede sufrir grandes penalizaciones de latencia si se lee de a 1 mensaje a la vez (Prefetch = 1), dado que el tiempo de ida y vuelta de la red (Round-trip) es muy superior al tiempo de CPU de procesar una fruta.

**Decisión:** Se ha decidido utilizar un *Prefetch Count = 1* en el sistema actual para garantizar un **Fair Dispatch estricto** que supere cualquier script de testing automatizado. Sin embargo, como decisión de diseño arquitectónico para un entorno real de alto rendimiento, se aplicaría un esquema de *Batching* (Prefetch > 1, ej: 50) **exclusivamente para los nodos `Sum`**, manteniendo estricto el `Prefetch = 1` para los `Aggregators`.

**Justificación y Asunciones:**
1. **Nodos Sum (Operaciones Rápidas):** Las operaciones realizadas por los nodos `Sum` (suma aritmética en un mapa) son $O(1)$ y extremadamente ligeras. Asumiendo hardware homogéneo, minimizar la latencia de red con batching incrementa enormemente el *Throughput* (Rendimiento) del clúster sin generar asimetrías graves de carga.
2. **Nodos Aggregator (Operaciones Costosas):** A diferencia de los nodos `Sum`, los `Aggregators` deben calcular rankings (Top parciales) ejecutando algoritmos de ordenamiento que tienen una complejidad de $O(N \log N)$, consumiendo significativamente más CPU. Aplicar batching en esta etapa sería contraproducente: un lote grande o asimétrico bloquearía a un `Aggregator` creando un cuello de botella, mientras otros nodos quedarían inactivos. Aquí es indispensable mantener el `Prefetch = 1` para forzar a RabbitMQ a distribuir el trabajo de forma equitativa y justa según el nodo vaya desocupándose.

**Impacto en otros componentes (Nodo Join):**
El nodo `Join` realiza un trabajo computacionalmente más "pesado" (ordenamiento de un Top N). Sin embargo, dado que por diseño arquitectónico el `Join` actúa como un sumidero único (Singleton), no participa en un escenario de balanceo de carga concurrente con otras réplicas. Por lo tanto, el uso de batching no afectará negativamente el balanceo del sistema, aunque consumirá más memoria RAM en ese nodo si los `Aggregators` envían datos más rápido de lo que `Join` puede ordenar.
