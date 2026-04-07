# Implementación del Middleware de RabbitMQ

## Conexión y ciclo de vida

Cada instancia del middleware establece su propia `BlockingConnection` a RabbitMQ en el momento de construcción (`__init__`). Esto significa que cada proceso tiene su propia conexión independiente, lo cual es necesario porque `pika.BlockingConnection` no es thread-safe. Como los tests lanzan consumidores como procesos separados (`multiprocessing.Process`), cada uno tiene su propia instancia y conexión, evitando condiciones de carrera.

El cierre de la conexión se hace de forma explícita en `close()`, verificando que el canal y la conexión estén abiertos antes de cerrarlos. Esto evita errores si se llama a `close()` más de una vez o luego de una desconexión.

## Manejo de SIGTERM

Ambas clases registran un handler para la señal `SIGTERM` en el `__init__`. Al recibir la señal, se invoca `close()`, que cierra el canal y la conexión a RabbitMQ antes de que el proceso muera. Esto evita dejar conexiones colgadas en el broker y mensajes sin acknowledgear.

## Work Queue (`MessageMiddlewareQueueRabbitMQ`)

### Declaración de la cola

La cola se declara con `durable=True`, lo que significa que sobrevive a reinicios del broker RabbitMQ. Múltiples instancias pueden declarar la misma cola — RabbitMQ simplemente verifica que los parámetros coincidan.

### Envío de mensajes

`send()` publica directamente en la cola usando `exchange=''` (el exchange default de RabbitMQ) con la `routing_key` igual al nombre de la cola. Los mensajes se publican con `delivery_mode=2` (persistentes), lo que significa que sobreviven a reinicios del broker.

### Consumo de mensajes

`start_consuming()` es un método bloqueante — el proceso se queda en el loop de eventos de pika hasta que se llama a `stop_consuming()`. Internamente wrappea el callback de pika para exponer la interfaz `(message, ack, nack)` definida por la clase abstracta, ocultando los detalles de pika al consumidor.

El `ack` y `nack` son funciones que el consumidor invoca manualmente, lo que permite confirmar un mensaje solo después de procesarlo exitosamente (at-least-once delivery).

### Distribución de mensajes entre consumers

RabbitMQ distribuye los mensajes de una cola entre todos los consumidores activos en round-robin. Esto significa que con múltiples consumers sobre la misma cola, cada mensaje es procesado por exactamente uno de ellos — útil para distribuir carga de trabajo.

## Exchange (`MessageMiddlewareExchangeRabbitMQ`)

### Tipo de exchange

Se usa un exchange de tipo `direct`, declarado como `durable=True`. El exchange rutea cada mensaje a las colas que tienen un binding con la routing key exacta del mensaje.

### Cola exclusiva y anónima por instancia

Cada instancia consumidora crea su propia cola exclusiva y anónima con `queue_declare(queue='', exclusive=True)`. RabbitMQ genera un nombre único para cada una. Esta cola se elimina automáticamente cuando la conexión se cierra.

Esto es fundamental para el broadcast: si múltiples consumers se subscriben a la misma routing key, cada uno tiene su propia cola bindeada a esa key, y RabbitMQ entrega una copia del mensaje a cada cola. Así todos los consumers reciben todos los mensajes que les corresponden.

### Routing keys y bindings

En el `__init__`, la instancia bindea su cola anónima a cada routing key de la lista. Esto permite que un consumer escuche mensajes de múltiples routing keys simultáneamente.

### Envío de mensajes

`send()` publica en el exchange usando la primera routing key de la lista. El exchange se encarga de rutear el mensaje a todas las colas que tienen un binding con esa key.

### Consumo de mensajes

Idéntico al de la Work Queue — `start_consuming()` es bloqueante, wrappea el callback para exponer `(message, ack, nack)`, y `stop_consuming()` detiene el loop desde adentro del callback.