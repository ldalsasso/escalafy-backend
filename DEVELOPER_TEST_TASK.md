# Prueba Técnica — Senior Backend Engineer

**Tiempo estimado:** 4–6 horas
**Lenguaje:** Python (obligatorio). Podés elegir tu framework HTTP, sistema de colas, y cualquier librería de soporte — pero tenés que justificar cada elección no trivial en tu README.
**Entregable:** Un repositorio de GitHub con el código funcionando y un README.

---

## El problema

Construimos una plataforma de analytics para tiendas de e-commerce. Somos la capa de datos detrás del negocio online de nuestros clientes: trackeamos lo que hacen los compradores, agregamos ese comportamiento y ayudamos a los dueños de las tiendas a entender qué está impulsando sus ventas.

Hoy, los dueños de tiendas usan nuestra plataforma para responder preguntas como: "¿Cuántas personas visitaron mi tienda ayer?", "¿Cuántos realmente compraron algo?", "¿Qué campaña de marketing trajo más ventas la semana pasada?" Para responder estas preguntas necesitamos datos de comportamiento en bruto — y muchos, colectados de manera confiable y correctamente atribuidos a los usuarios correctos.

Una de las piezas centrales de ese sistema es un **pipeline de eventos**. Un snippet de JavaScript liviano vive en el sitio web de cada tienda cliente. Cada vez que un comprador hace algo — ve una página de producto, agrega algo al carrito, empieza un checkout, completa una compra — ese snippet dispara un evento hacia nuestro backend.

Actualmente tenemos **más de 3.000 tiendas clientes activas**. A nuestro volumen actual, esto representa aproximadamente **2 millones de eventos por día**. Ese número crece a medida que sumamos clientes, y durante picos (Black Friday, flash sales) una sola tienda puede llegar a decenas de miles de eventos por hora.

La infraestructura que vas a construir tiene tres responsabilidades:

1. **Recibir eventos de forma confiable** — con alto throughput, sin perder eventos, incluso durante picos o caídas downstream.
2. **Entender a los usuarios** — unir sesiones anónimas en journeys coherentes, para poder decir "esta persona visitó 3 veces antes de comprar."
3. **Potenciar el reporting** — darle a los dueños de tiendas una API rápida que responda preguntas de negocio sobre sus datos sin consultar millones de filas en bruto por cada request.

Tu tarea es diseñar y construir este pipeline. No te vamos a dar el esquema de la base de datos — diseñar el modelo de datos correcto a partir de los requisitos de negocio de arriba es parte de la evaluación.

---

## Qué vas a construir

Son cuatro piezas. Se construyen una sobre otra.

### 1. API de Ingesta de Eventos

Construí una API HTTP que reciba eventos desde los sitios web de los clientes.

Cada evento tiene exactamente estos campos:

| Campo | Tipo | Requerido | Descripción |
|-------|------|-----------|-------------|
| `store_id` | string | sí | Identifica la tienda cliente |
| `event_type` | string | sí | Uno de: `page_view`, `add_to_cart`, `checkout_start`, `checkout_success` |
| `session_id` | string | sí | Identificador de sesión anónima, scoped al browser |
| `timestamp` | string | sí | ISO 8601 |
| `user_ip` | string | sí | IP del cliente |
| `event_object_id` | string | sí | Depende del contexto: el path de página para `page_view`, el ID de producto para `add_to_cart`, el ID de checkout para `checkout_start` y `checkout_success` |

La API debe:
- Aceptar un evento individual via `POST /events`
- Aceptar un batch de hasta 100 eventos via `POST /events/batch`
- Retornar `200 Accepted` inmediatamente — nunca bloquear en el procesamiento
- Validar los campos requeridos y retornar `400` con un error estructurado si el input es inválido
- Exponer `GET /health` retornando el estado básico del servicio

La API no debe procesar los eventos en sí misma. Recibe, valida y entrega.

#### Ejemplo de payload

Un evento POST a `/events`:

```json
{
  "store_id": "store_123",
  "event_type": "page_view",
  "session_id": "sess_456789",
  "timestamp": "2024-01-15T10:23:45.123Z",
  "user_ip": "192.168.1.100",
  "event_object_id": "/products/electronics"
}
```

Un batch POST a `/events/batch`:

```json
[
  {
    "store_id": "store_123",
    "event_type": "page_view",
    "session_id": "sess_456789",
    "timestamp": "2024-01-15T10:23:45.123Z",
    "user_ip": "192.168.1.100",
    "event_object_id": "/products"
  },
  {
    "store_id": "store_123",
    "event_type": "add_to_cart",
    "session_id": "sess_456789",
    "timestamp": "2024-01-15T10:23:50.456Z",
    "user_ip": "192.168.1.100",
    "event_object_id": "prod_789"
  },
  {
    "store_id": "store_123",
    "event_type": "checkout_start",
    "session_id": "sess_456789",
    "timestamp": "2024-01-15T10:24:00.789Z",
    "user_ip": "192.168.1.100",
    "event_object_id": "checkout_123"
  }
]
```

---

### 2. Consumer Worker

Construí un consumer que drene la queue continuamente y escriba los eventos en tu base de datos.

El consumer debe:
- Procesar eventos en batches, no de a uno
- Garantizar **entrega at-least-once**: si el consumer cae a mitad de un batch, ningún evento debe perderse
- Manejar **poison pills**: un evento malformado no debe bloquear el resto del batch
- Manejar **duplicados**: el mismo evento puede llegar más de una vez (retry del cliente, duplicado de red). Tu esquema debe contemplar esto.
- En fallo parcial de batch: commitear los eventos exitosos, trackear los fallos

Para la queue, hay libertad absoluta, y no es necesario montar un sistema complejo para facilitar la ejecución del ejercicio. Si preferís armar algo simple y especificar que tecnología usarias en prod (Redis Streams, RabbitMQ, Kafka u otro sistema), está bien — pero explicá el trade-off en tu README. 

---

### 3. Reconocimiento de Usuarios

Esta es la parte donde modelás datos a partir de un problema de negocio, no de una especificación.

El problema: **nuestros eventos son anónimos**. No hay cuentas de usuario ni login tokens. Todo lo que tenemos es un session ID (scoped al browser, se resetea entre sesiones) y una dirección IP.

Pero igualmente necesitamos saber quién es un comprador a lo largo de su journey completo. Para nuestros propósitos:

- **Dos sesiones pertenecen al mismo usuario si comparten el mismo `user_ip`**
- **Dos sesiones también pertenecen al mismo usuario si comparten el mismo checkout ID** (el `event_object_id` de un evento `checkout_start` o `checkout_success`)

Lo que necesitamos de los datos:
- Para cualquier tienda, poder responder: ¿cuántos usuarios únicos vimos este mes?
- Para cualquier usuario, poder trazar su historial de sesiones: ¿qué páginas visitó, qué agregó al carrito, terminó comprando?
- Para una conversión (un evento `checkout_success`), poder decir: ¿qué sesiones tuvo este usuario antes de comprar?

Vos decidís cómo modelar esto. Vos decidís qué tablas crear. El esquema es parte de lo que evaluamos.

Construí un worker que corra periódicamente, procese eventos recientes y mantenga las estructuras de datos que diseñes. Debe ser seguro de correr concurrentemente con el consumer.

---

### 4. Reporting API

Construí una API de lectura que los dueños de tiendas usan para consultar sus datos.

El endpoint central es un **reporte de funnel diario** para una tienda dada:

```
GET /stores/{store_id}/report?from=2024-01-01&to=2024-01-31
```

La respuesta debe incluir, para cada día en el rango:
- `date`
- `unique_users` — usuarios distintos identificados ese día
- `sessions` — session IDs distintos vistos ese día
- `page_views` — conteo de eventos `page_view`
- `add_to_carts` — conteo de eventos `add_to_cart`
- `checkouts_started` — conteo de eventos `checkout_start`
- `checkouts_completed` — conteo de eventos `checkout_success`

Este endpoint representa el funnel de conversión completo a lo largo del tiempo.

#### Customer journey por conversión

Un segundo endpoint te permite explorar el viaje completo de un cliente que convirtió:

```
GET /conversions/{checkout_id}/journey
```

Dado un `checkout_id`, retorna todas las sesiones previas de ese usuario (en orden cronológico) que llevaron a esa compra:

```json
{
  "checkout_id": "checkout_123",
  "store_id": "store_123",
  "conversion_date": "2024-01-15T10:30:00Z",
  "sessions": [
    {
      "session_id": "sess_111",
      "first_seen": "2024-01-15T08:00:00Z",
      "last_seen": "2024-01-15T08:15:00Z",
      "events": [
        {"type": "page_view", "object_id": "/products"},
        {"type": "page_view", "object_id": "/products/electronics"}
      ]
    },
    {
      "session_id": "sess_222",
      "first_seen": "2024-01-15T09:00:00Z",
      "last_seen": "2024-01-15T09:45:00Z",
      "events": [
        {"type": "page_view", "object_id": "/products/electronics"},
        {"type": "add_to_cart", "object_id": "prod_456"}
      ]
    },
    {
      "session_id": "sess_333",
      "first_seen": "2024-01-15T10:00:00Z",
      "last_seen": "2024-01-15T10:30:00Z",
      "events": [
        {"type": "page_view", "object_id": "/cart"},
        {"type": "checkout_start", "object_id": "checkout_123"},
        {"type": "checkout_success", "object_id": "checkout_123"}
      ]
    }
  ]
}
```

Este endpoint es fundamental para la atribución: permite ver exactamente qué touchpoints llevaron a una venta. Podés agregar otros endpoints si los considerás útiles — pero estos dos son los obligatorios.

Tené en cuenta: esta API corre contra **2 millones de eventos por día** en 3.000+ tiendas. Una tienda haciendo una promoción grande puede tener 50.000 eventos en un solo día. Consultar filas de eventos en bruto en cada API request no es un diseño viable a esta escala. El query performance para ambos endpoints es crítico.

Cómo resolvés el problema de performance de lectura es parte de la evaluación.

---

## Constraints

### Hard (no negociables)

- Python en todos lados. Type hints en todas las firmas de funciones y variables no triviales. Sin `Any` salvo donde sea necesario, con un comentario explicando por qué.
- `dataclass` para todos los modelos de datos. Sin `dict` crudo para representar entidades de dominio.
- Logging estructurado en JSON hacia stdout. Un objeto JSON por línea. Cada línea de log debe tener como mínimo: `timestamp`, `level`, `message`, y el nombre del componente que la emite.
- Nunca `except:` desnudo. Siempre capturar un tipo de excepción específico.
- SQL siempre con placeholders de parámetros. Nunca interpolación de strings en queries.
- Un `docker-compose.yml` que levante toda la infraestructura necesaria. Vamos a correr `docker compose up` y esperamos que funcione.
- Un `README.md`: overview de arquitectura, cómo correr localmente paso a paso, cómo interactúa cada componente con los demás, elección de queue y por qué, cómo garantizás entrega at-least-once, una cosa que cambiarías dado más tiempo.
- El repositorio debe estar listo para correr. `docker compose up` seguido de tu comando de inicio documentado debe levantar el sistema completo sin pasos manuales.

### Soft (señales de calidad)

- Funciones de menos de 40 líneas.
- Sin código muerto. Sin TODOs sin explicar.
- Conexiones a base de datos via pool, no abiertas por operación.
- Timeouts en todos los requests HTTP salientes.

---

## Testing bajo carga

Incluimos un script de prueba de carga (`load_test_example.py`) que simula tráfico realista de producción. Podés usarlo para testear tu API:

```bash
python load_test_example.py http://localhost:5000 --duration 300 --target-rps 20
```

El script:
- Genera eventos aleatorios válidos de todos los tipos
- Simula picos realistas: algunos segundos envía 50 eventos/seg, otros 5, con un promedio de ~20 eventos/seg
- Corre durante ~5 minutos
- Reporta estadísticas: total de requests, success/error rate, latencias (min/max/p50/p95/p99)

Esta es una herramienta de debugging — usala para encontrar cuellos de botella en tu API y verificar que manejás la carga esperada sin timeouts o errores.

---

## Ir más allá

Los requisitos de arriba son la línea de base. Si te sobra tiempo o querés mostrar más de lo que sabés, todo lo que agregues — implementado o descrito por escrito — va a ser evaluado positivamente.

Pensá en esto como tu oportunidad para demostrar cómo construirías esto en producción. ¿Qué agregarías? ¿Qué harías más robusto? ¿Qué documentarías? No hay respuestas incorrectas — queremos ver cómo pensás los sistemas de nivel productivo.

Todo suma.

---

## Entrega

1. Repositorio público en GitHub
2. `docker-compose.yml` que levante toda la infraestructura
3. Archivos SQL de migración para todas las tablas que crees
4. `README.md` como se describió arriba
5. Sin secrets ni credenciales commiteados

**Las partes 3 y 4 son intencionalmente abiertas.** Una excelente Parte 1 + Parte 2 con una Parte 3 bien razonada pero incompleta es mejor señal que cuatro partes superficiales. Valoramos la profundidad por sobre la amplitud.
