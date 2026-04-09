# Implementación del Middleware de RabbitMQ

## Conexión y ciclo de vida

Cada instancia del middleware establece su propia `BlockingConnection` a RabbitMQ en el momento de construcción (`__init__`). Esto es una decisión deliberada: `pika.BlockingConnection` no es thread-safe, por lo que compartir una conexión entre múltiples threads o procesos generaría condiciones de carrera. Como los tests lanzan consumidores como procesos separados con `multiprocessing.Process`, cada proceso tiene su propia instancia y conexión independiente, evitando cualquier problema de concurrencia sin necesidad de locks.

Se inicializan `_channel`, `_connection` y `_consumer_tag` en `None` antes de intentar conectar. Esto permite que `close()` y `stop_consuming()` funcionen correctamente incluso si la conexión falla a mitad del `__init__`, ya que los guards sobre estos atributos no rompen si nunca llegaron a asignarse.

## Manejo de SIGTERM

Ambas clases registran un handler para `SIGTERM` en el `__init__`. Al recibir la señal, se invoca `close()`, cerrando el canal y la conexión a RabbitMQ antes de que el proceso muera. Esto aplica directamente el feedback del TP0, donde se observó que al recibir SIGTERM no se forzaba el cierre de conexiones activas, dejándolas abiertas hasta que el cliente se desconectara solo.

## Manejo de excepciones

Se diferencia entre tres tipos de error siguiendo el contrato de la interfaz abstracta:
- `MessageMiddlewareDisconnectedError`: cuando se pierde la conexión con RabbitMQ (`AMQPConnectionError`)
- `MessageMiddlewareMessageError`: cuando ocurre un error interno durante el envío o consumo (`AMQPError`)
- `MessageMiddlewareCloseError`: cuando ocurre un error durante el cierre de recursos

Se captura `pika.exceptions.AMQPError` en lugar de `Exception` para no silenciar errores de programación como `AttributeError` o `TypeError`, que deben propagarse normalmente.

## Callback wrapper

Se extrae `_build_pika_callback()` como función auxiliar a nivel de módulo para evitar duplicación entre las dos clases. Esta función wrappea el callback interno de pika — que recibe `(ch, method, properties, body)` — y expone la interfaz `(message, ack, nack)` definida por la clase abstracta, ocultando los detalles de pika al consumidor.

El `ack` y `nack` son funciones que el consumidor invoca manualmente. Esto implementa at-least-once delivery: un mensaje solo se confirma después de ser procesado exitosamente. Si el proceso muere antes de hacer `ack`, RabbitMQ reencola el mensaje. El `nack` se invoca con `requeue=True` para devolver el mensaje a la cola en caso de fallo transitorio.

## Work Queue (`MessageMiddlewareQueueRabbitMQ`)

### Declaración de la cola

La cola se declara con `durable=True`, lo que significa que sobrevive a reinicios del broker. Múltiples instancias pueden declarar la misma cola — RabbitMQ simplemente verifica que los parámetros coincidan y no la recrea.

### Envío de mensajes

`send()` publica directamente en la cola usando `exchange=''` (el exchange default de RabbitMQ) con `routing_key` igual al nombre de la cola. Este es el mecanismo estándar de work queue en RabbitMQ.

### Consumo de mensajes

`start_consuming()` es bloqueante — el proceso se queda en el loop de eventos de pika hasta que se llama a `stop_consuming()`. El `MessageConsumerTester` llama a `stop_consuming()` desde adentro del callback una vez que recibió todos los mensajes esperados. Pika está diseñado para soportar esto: `channel.stop_consuming()` es seguro de llamar desde dentro de un callback.

Se usa `basic_qos(prefetch_count=1)` antes de consumir para garantizar un reparto equitativo entre consumers concurrentes — evita que un consumer acumule mensajes mientras otros están ociosos.

El `_consumer_tag` se guarda al registrar el consumer y se limpia en el bloque `finally` de `start_consuming()`, independientemente de cómo termine el loop.

### stop_consuming sin efecto si no se estaba consumiendo

Siguiendo el contrato de la interfaz abstracta, `stop_consuming()` no tiene efecto ni lanza excepciones si no se estaba consumiendo. Esto se implementa verificando tanto `channel.is_open` como `_consumer_tag` antes de llamar a `channel.stop_consuming()`.

### Distribución entre consumers

RabbitMQ distribuye los mensajes de una cola entre todos los consumers activos en round-robin. Con múltiples consumers sobre la misma cola, cada mensaje es procesado por exactamente uno — útil para distribuir carga de trabajo horizontalmente.

## Exchange (`MessageMiddlewareExchangeRabbitMQ`)

### Tipo de exchange

Se usa un exchange de tipo `direct`, declarado como `durable=True`. El exchange rutea cada mensaje a las colas que tienen un binding con la routing key exacta del mensaje publicado. Se elige `direct` sobre `fanout` porque permite routing selectivo por routing key, cubriendo tanto direct messaging como broadcast según cómo se configuren los bindings.

### Cola exclusiva y anónima por instancia

Cada instancia consumidora crea su propia cola exclusiva y anónima con `queue_declare(queue='', exclusive=True)`. RabbitMQ genera un nombre único para cada una automáticamente. Esta cola se elimina automáticamente cuando la conexión se cierra, sin necesidad de cleanup manual.

Esta decisión es fundamental para el broadcast: si múltiples consumers se subscriben a la misma routing key, cada uno tiene su propia cola bindeada a esa key. RabbitMQ entrega una copia del mensaje a cada cola, garantizando que todos los consumers la reciben. Si se usara una cola compartida con nombre fijo, el comportamiento sería round-robin y solo uno de los consumers recibiría cada mensaje.

### Routing keys y bindings

En el `__init__`, la instancia bindea su cola anónima a cada routing key de la lista. Esto permite que un consumer escuche mensajes de múltiples routing keys simultáneamente.

### Envío de mensajes

`send()` publica el mensaje en el exchange por cada routing key configurada en la instancia. Esto sigue el criterio de la cátedra: una instancia declarada con un conjunto de routing keys envía a todas ellas al invocar `send()`.

## Cómo correr

```bash
make up    # levanta RabbitMQ y corre los tests
make down  # detiene los contenedores
```