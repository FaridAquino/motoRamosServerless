# MotoRamos — Manual de Integración Backend

## Plataforma de Transporte en Mototaxi para Tarma, Junín
### Proyecto de Ingeniería — UTEC 2026

---

## Índice

1. [Arquitectura General](#1-arquitectura-general)
2. [Despliegue](#2-despliegue)
3. [API REST — Usuarios (Pasajeros)](#3-api-rest--usuarios-pasajeros)
4. [API REST — Conductores](#4-api-rest--conductores)
5. [WebSocket — Tiempo Real](#5-websocket--tiempo-real)
6. [Tablas DynamoDB](#6-tablas-dynamodb)
7. [Flujo Completo de un Viaje](#7-flujo-completo-de-un-viaje)
8. [Reconexión WebSocket](#8-reconexión-websocket)
9. [Códigos de Error](#9-códigos-de-error)

---

## 1. Arquitectura General

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  App Pasajero    │────▶│ API Gateway REST  │────▶│ Lambda: usuarios    │
│  (Flutter)       │     │ /usuarios/*       │     │  handler.py         │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
         │                                                │
         │              ┌──────────────────┐              ▼
         │              │ API Gateway REST  │────▶┌─────────────────────┐
         │              │ /conductores/*    │     │ Lambda: conductores │
         │              └──────────────────┘     │  handler.py         │
         │                       ▲                └─────────────────────┘
         │                       │                        │
┌─────────────────┐              │                        ▼
│  App Conductor   │─────────────┘                ┌─────────────────────┐
│  (Flutter)       │                              │     DynamoDB        │
└─────────────────┘                               │  ┌───────────────┐  │
         │                                        │  │  usuarios     │  │
         │              ┌──────────────────┐      │  │  conductores  │  │
         └─────────────▶│ API Gateway WS   │─────▶│  │  servicios    │  │
         │              │ wss://...        │      │  │  conexiones   │  │
         │              └──────────────────┘      │  └───────────────┘  │
┌─────────────────┐              │                └─────────────────────┘
│  App Pasajero    │─────────────┘                        │
│  (Flutter)       │                              ┌───────┴───────┐
└─────────────────┘                               │      S3       │
                                                  │  (fotos)      │
                                                  └───────────────┘
```

### Microservicios

| Servicio              | Tipo      | Ruta Base                 | Descripción                              |
|----------------------|-----------|---------------------------|------------------------------------------|
| `api-usuarios`       | REST      | `https://{api-id}.../dev` | Auth, perfil, historial, calificaciones  |
| `api-conductores`    | REST      | `https://{api-id}.../dev` | Auth, perfil, toggle activo, ganancias   |
| `ws-motoRamos`       | WebSocket | `wss://{ws-id}.../dev`    | Viajes en tiempo real, ubicación, chat   |
| `infra-motoRamos`    | Infra     | —                         | Tablas DynamoDB, bucket S3               |

---

## 2. Despliegue

### Orden de despliegue (importante)

```bash
# 1. Primero la infraestructura (crea las tablas y el bucket)
cd serviciosNecesarios
serverless deploy --stage dev

# 2. Luego los microservicios REST
cd ../usuarios
serverless deploy --stage dev

cd ../conductores
serverless deploy --stage dev

# 3. Finalmente el WebSocket
cd ../webSocket
serverless deploy --stage dev
```

### Variables de Entorno Requeridas

| Variable     | Descripción                          | Default                                    |
|-------------|--------------------------------------|--------------------------------------------|
| `JWT_SECRET` | Clave secreta para firmar JWT tokens | `motoRamos-tarma-jwt-secret-utec-2026`     |

> **IMPORTANTE**: En producción, configura `JWT_SECRET` como variable de entorno real.

---

## 3. API REST — Usuarios (Pasajeros)

**Base URL**: `https://{api-id}.execute-api.us-east-1.amazonaws.com/dev`

### 3.1 Registro de Usuario

```
POST /registerUsuario
```

**Headers**: Ninguno (público)

**Request Body**:
```json
{
  "nombre": "Carlos",
  "apellido": "Ramos",
  "correo": "carlos@email.com",
  "contrasena": "MiContraseña123",
  "telefono": "987654321",
  "edad": 25
}
```

**Response 201**:
```json
{
  "message": "Usuario registrado exitosamente",
  "userId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Errores**:
| Código | Respuesta |
|--------|-----------|
| 400    | `{"error": "Campos requeridos faltantes: nombre, correo"}` |
| 409    | `{"error": "Ya existe un usuario registrado con ese correo"}` |

---

### 3.2 Login de Usuario

```
POST /loginUsuario
```

**Request Body**:
```json
{
  "correo": "carlos@email.com",
  "contrasena": "MiContraseña123"
}
```

**Response 200**:
```json
{
  "message": "Login exitoso",
  "userId": "a1b2c3d4-...",
  "nombre": "Carlos",
  "apellido": "Ramos",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Errores**:
| Código | Respuesta |
|--------|-----------|
| 400    | `{"error": "Correo y contraseña son requeridos"}` |
| 401    | `{"error": "Contraseña incorrecta"}` |
| 403    | `{"error": "Cuenta desactivada"}` |
| 404    | `{"error": "Usuario no encontrado"}` |

---

### 3.3 Obtener Perfil

```
GET /perfil
Authorization: Bearer <token>
```

**Response 200**:
```json
{
  "usuario": {
    "userId": "a1b2c3d4-...",
    "nombre": "Carlos",
    "apellido": "Ramos",
    "correo": "carlos@email.com",
    "telefono": "987654321",
    "fotoUrl": "https://motoramos-fotos-dev.s3.amazonaws.com/usuarios/.../foto.jpg",
    "calificacionPromedio": 4.75,
    "activo": true,
    "creadoEn": "2026-03-04T10:30:00-05:00"
  }
}
```

---

### 3.4 Actualizar Perfil

```
PUT /perfil
Authorization: Bearer <token>
```

**Request Body** (solo campos a actualizar):
```json
{
  "nombre": "Carlos Alberto",
  "telefono": "912345678",
  "edad": 26
}
```

**Campos permitidos**: `nombre`, `apellido`, `telefono`, `edad`

**Response 200**:
```json
{
  "message": "Perfil actualizado exitosamente"
}
```

---

### 3.5 Obtener URL para Subir Foto

```
GET /perfil/foto-url
Authorization: Bearer <token>
```

**Response 200**:
```json
{
  "uploadUrl": "https://motoramos-fotos-dev.s3.amazonaws.com/usuarios/.../foto.jpg?X-Amz-...",
  "fotoUrl": "https://motoramos-fotos-dev.s3.amazonaws.com/usuarios/.../foto.jpg"
}
```

**Uso en Flutter**:
```dart
// 1. Obtener la URL pre-firmada
final response = await http.get('/perfil/foto-url', headers: authHeaders);
final uploadUrl = response['uploadUrl'];

// 2. Subir la imagen directamente a S3 con PUT
await http.put(
  Uri.parse(uploadUrl),
  headers: {'Content-Type': 'image/jpeg'},
  body: imageBytes,
);
```

---

### 3.6 Historial de Viajes

```
GET /historial
GET /historial?desde=2026-03-01T00:00:00&hasta=2026-03-04T23:59:59&limit=10
Authorization: Bearer <token>
```

**Response 200**:
```json
{
  "servicios": [
    {
      "serviceId": "uuid-...",
      "usuarioId": "a1b2c3d4-...",
      "driverId": "x1y2z3-...",
      "estado": "COMPLETADO",
      "origen": {
        "lat": -11.4198,
        "lng": -75.6896,
        "direccion": "Plaza de Armas"
      },
      "destino": {
        "lat": -11.4150,
        "lng": -75.6820,
        "direccion": "Terminal Terrestre"
      },
      "precioFinal": 5.0,
      "creadoEn": "2026-03-04T08:30:00-05:00",
      "completadoEn": "2026-03-04T08:45:00-05:00"
    }
  ],
  "count": 1
}
```

---

### 3.7 Calificar Conductor

```
POST /calificar
Authorization: Bearer <token>
```

**Request Body**:
```json
{
  "serviceId": "uuid-del-servicio",
  "puntuacion": 5,
  "comentario": "Excelente servicio, muy amable"
}
```

**Response 200**:
```json
{
  "message": "Calificación registrada exitosamente"
}
```

**Errores**:
| Código | Respuesta |
|--------|-----------|
| 400    | `{"error": "La puntuación debe ser un número entre 1 y 5"}` |
| 403    | `{"error": "No autorizado para calificar este servicio"}` |
| 404    | `{"error": "Servicio no encontrado"}` |

---

## 4. API REST — Conductores

**Base URL**: `https://{api-id}.execute-api.us-east-1.amazonaws.com/dev`

### 4.1 Registro de Conductor

```
POST /registerConductor
```

**Request Body**:
```json
{
  "nombre": "Juan",
  "apellido": "Pérez",
  "correo": "juan@email.com",
  "contrasena": "MiContraseña123",
  "telefono": "976543210",
  "placa": "ABC-123",
  "marca": "Bajaj",
  "color": "Rojo"
}
```

**Response 201**:
```json
{
  "message": "Conductor registrado exitosamente. Esperando autorización del admin.",
  "driverId": "x1y2z3w4-...",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Errores**:
| Código | Respuesta |
|--------|-----------|
| 409    | `{"error": "Ya existe un conductor registrado con ese correo"}` |
| 409    | `{"error": "Ya existe un conductor registrado con esa placa"}` |

---

### 4.2 Login de Conductor

```
POST /loginConductor
```

**Request Body**:
```json
{
  "correo": "juan@email.com",
  "contrasena": "MiContraseña123"
}
```

**Response 200**:
```json
{
  "message": "Login exitoso",
  "driverId": "x1y2z3w4-...",
  "nombre": "Juan",
  "apellido": "Pérez",
  "activo": false,
  "autorizadoPorAdmin": true,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

---

### 4.3 Obtener Perfil

```
GET /perfil
Authorization: Bearer <token>
```

**Response 200**:
```json
{
  "conductor": {
    "driverId": "x1y2z3w4-...",
    "nombre": "Juan",
    "apellido": "Pérez",
    "correo": "juan@email.com",
    "telefono": "976543210",
    "placa": "ABC-123",
    "marca": "Bajaj",
    "color": "Rojo",
    "fotoUrl": "",
    "calificacionPromedio": 4.8,
    "activo": false,
    "autorizadoPorAdmin": true,
    "creadoEn": "2026-03-01T09:00:00-05:00"
  }
}
```

---

### 4.4 Actualizar Perfil

```
PUT /perfil
Authorization: Bearer <token>
```

**Request Body**:
```json
{
  "telefono": "999888777",
  "marca": "Honda",
  "color": "Azul"
}
```

**Campos permitidos**: `nombre`, `apellido`, `telefono`, `placa`, `marca`, `color`

---

### 4.5 Toggle Activo (En Línea / Fuera de Línea)

```
PUT /toggle-activo
Authorization: Bearer <token>
```

**Request Body**:
```json
{
  "activo": true
}
```

**Response 200**:
```json
{
  "message": "Estado actualizado: EN LÍNEA",
  "activo": true
}
```

**Errores**:
| Código | Respuesta |
|--------|-----------|
| 403    | `{"error": "Tu cuenta aún no ha sido autorizada por un administrador"}` |

---

### 4.6 Historial de Viajes del Conductor

```
GET /historial
GET /historial?desde=2026-03-01T00:00:00&hasta=2026-03-04T23:59:59&limit=10
Authorization: Bearer <token>
```

**Response 200**: (mismo formato que historial de usuarios)

---

### 4.7 Calificar Usuario

```
POST /calificar
Authorization: Bearer <token>
```

**Request Body**:
```json
{
  "serviceId": "uuid-del-servicio",
  "puntuacion": 4,
  "comentario": "Buen pasajero"
}
```

---

### 4.8 Resumen de Ganancias

```
GET /ganancias
GET /ganancias?desde=2026-03-04T00:00:00&hasta=2026-03-04T23:59:59
Authorization: Bearer <token>
```

**Response 200**:
```json
{
  "desde": "2026-03-04T00:00:00",
  "hasta": "2026-03-04T15:30:00-05:00",
  "totalServicios": 8,
  "totalGanancia": 38.50,
  "servicios": [...]
}
```

---

## 5. WebSocket — Tiempo Real

### 5.1 Conexión

```
wss://{ws-id}.execute-api.us-east-1.amazonaws.com/dev?token=<JWT>
```

El token JWT se pasa como query parameter. Se valida al momento de conectar.

### 5.2 Formato de Mensajes

Todos los mensajes WebSocket son JSON con un campo `action`:

```json
{"action": "nombreDeAccion", ...datos}
```

---

### 5.3 Acciones Disponibles

#### `servicioRequerido` (Pasajero → Servidor)

Solicita un mototaxi. Se envía a todos los conductores activos.

**Enviar**:
```json
{
  "action": "servicioRequerido",
  "origen": {
    "lat": -11.4198,
    "lng": -75.6896,
    "direccion": "Plaza de Armas de Tarma"
  },
  "destino": {
    "lat": -11.4150,
    "lng": -75.6820,
    "direccion": "Terminal Terrestre"
  },
  "precioSugerido": 4.00,
  "comentario": "Tengo una maleta"
}
```

**Respuesta al pasajero**:
```json
{
  "action": "servicioCreado",
  "serviceId": "uuid-...",
  "estado": "PENDIENTE",
  "message": "Buscando conductor disponible en Tarma..."
}
```

**Broadcast a conductores activos**:
```json
{
  "action": "nuevoServicio",
  "serviceId": "uuid-...",
  "origen": {"lat": -11.4198, "lng": -75.6896, "direccion": "Plaza de Armas"},
  "destino": {"lat": -11.4150, "lng": -75.6820, "direccion": "Terminal Terrestre"},
  "precioSugerido": 4.0,
  "nombreUsuario": "Carlos",
  "comentario": "Tengo una maleta",
  "creadoEn": "2026-03-04T10:30:00-05:00"
}
```

---

#### `aceptarServicio` (Conductor → Servidor)

El primer conductor en aceptar gana (asignación atómica).

**Enviar**:
```json
{
  "action": "aceptarServicio",
  "serviceId": "uuid-del-servicio",
  "precioOfrecido": 5.00
}
```

**Respuesta al pasajero**:
```json
{
  "action": "servicioAceptado",
  "serviceId": "uuid-...",
  "estado": "EN_CAMINO",
  "conductor": {
    "driverId": "x1y2z3-...",
    "nombre": "Juan",
    "apellido": "Pérez",
    "telefono": "976543210",
    "placa": "ABC-123",
    "marca": "Bajaj",
    "color": "Rojo",
    "fotoUrl": "https://...",
    "calificacion": 4.8
  },
  "precioFinal": 5.0,
  "message": "¡Tu conductor está en camino!"
}
```

**Confirmación al conductor**:
```json
{
  "action": "servicioAceptadoConfirmacion",
  "serviceId": "uuid-...",
  "estado": "EN_CAMINO",
  "origen": {"lat": -11.4198, "lng": -75.6896, "direccion": "Plaza de Armas"},
  "destino": {"lat": -11.4150, "lng": -75.6820, "direccion": "Terminal Terrestre"},
  "nombreUsuario": "Carlos",
  "precioFinal": 5.0,
  "message": "Servicio aceptado. Ve al punto de recojo."
}
```

**Error si ya fue tomado**:
```json
{
  "action": "servicioNoDisponible",
  "serviceId": "uuid-...",
  "message": "Este servicio ya fue tomado por otro conductor."
}
```

**Broadcast a otros conductores**:
```json
{
  "action": "servicioTomado",
  "serviceId": "uuid-..."
}
```

---

#### `iniciarViaje` (Conductor → Servidor)

El conductor confirma que recogió al pasajero.

**Enviar**:
```json
{
  "action": "iniciarViaje",
  "serviceId": "uuid-del-servicio"
}
```

**Respuesta al pasajero**:
```json
{
  "action": "viajeIniciado",
  "serviceId": "uuid-...",
  "estado": "EN_CURSO",
  "message": "¡Viaje iniciado! Ya estás en camino a tu destino."
}
```

---

#### `completarViaje` (Conductor → Servidor)

El conductor marca el viaje como finalizado.

**Enviar**:
```json
{
  "action": "completarViaje",
  "serviceId": "uuid-del-servicio"
}
```

**Respuesta al pasajero**:
```json
{
  "action": "viajeCompletado",
  "serviceId": "uuid-...",
  "estado": "COMPLETADO",
  "precioFinal": 5.0,
  "message": "¡Viaje completado! Gracias por usar MotoRamos. Por favor califica al conductor."
}
```

---

#### `cancelarServicio` (Pasajero o Conductor → Servidor)

Cancela un servicio PENDIENTE o EN_CAMINO. No se puede cancelar EN_CURSO.

**Enviar**:
```json
{
  "action": "cancelarServicio",
  "serviceId": "uuid-del-servicio",
  "motivo": "El conductor tardó mucho"
}
```

**Respuesta a ambos participantes**:
```json
{
  "action": "servicioCancelado",
  "serviceId": "uuid-...",
  "estado": "CANCELADO",
  "canceladoPor": "USUARIO",
  "motivo": "El conductor tardó mucho",
  "message": "Servicio cancelado por el pasajero."
}
```

---

#### `registrarUbicacionMoto` (Conductor → Servidor)

Envía la ubicación GPS actual del conductor al pasajero en tiempo real.

**Enviar** (cada 3-5 segundos):
```json
{
  "action": "registrarUbicacionMoto",
  "serviceId": "uuid-del-servicio",
  "lat": -11.4190,
  "lng": -75.6885
}
```

**Respuesta al pasajero**:
```json
{
  "action": "ubicacionConductor",
  "serviceId": "uuid-...",
  "lat": -11.4190,
  "lng": -75.6885,
  "timestamp": "2026-03-04T10:32:15-05:00"
}
```

---

#### `informar` (Pasajero o Conductor ↔ Servidor)

Mensajes de texto entre pasajero y conductor.

**Enviar**:
```json
{
  "action": "informar",
  "serviceId": "uuid-del-servicio",
  "mensaje": "Estoy en la puerta azul, frente a la iglesia"
}
```

**Al destinatario**:
```json
{
  "action": "mensajeRecibido",
  "serviceId": "uuid-...",
  "de": "Carlos",
  "rol": "USUARIO",
  "mensaje": "Estoy en la puerta azul, frente a la iglesia",
  "timestamp": "2026-03-04T10:31:00-05:00"
}
```

**Confirmación al remitente**:
```json
{
  "action": "mensajeEnviado",
  "serviceId": "uuid-...",
  "mensaje": "Estoy en la puerta azul, frente a la iglesia",
  "timestamp": "2026-03-04T10:31:00-05:00"
}
```

---

#### `ping` (Cualquiera → Servidor)

Heartbeat para mantener la conexión activa.

**Enviar**:
```json
{"action": "ping"}
```

**Respuesta**:
```json
{
  "action": "pong",
  "timestamp": "2026-03-04T10:30:00-05:00",
  "message": "Conexión activa"
}
```

---

## 6. Tablas DynamoDB

### 6.1 `usuarios`

| Atributo             | Tipo    | Descripción                    |
|---------------------|---------|--------------------------------|
| `userId` (PK)       | String  | UUID del usuario               |
| `nombre`            | String  | Nombre del pasajero            |
| `apellido`          | String  | Apellido                       |
| `correo`            | String  | Email (único vía GSI)          |
| `telefono`          | String  | Número de celular              |
| `contrasenaHasheada`| String  | PBKDF2 hash                    |
| `fotoUrl`           | String  | URL de foto en S3              |
| `edad`              | Number  | Edad (opcional)                |
| `sumaCalificaciones`| Number  | Suma total de calificaciones   |
| `totalCalificaciones`| Number | Conteo de calificaciones       |
| `activo`            | Boolean | Cuenta activa/desactivada      |
| `creadoEn`          | String  | ISO timestamp                  |

**GSI**: `CorreoIndex` (PK: correo)

### 6.2 `conductores`

| Atributo              | Tipo    | Descripción                      |
|----------------------|---------|----------------------------------|
| `driverId` (PK)      | String  | UUID del conductor               |
| `nombre`             | String  | Nombre                           |
| `apellido`           | String  | Apellido                         |
| `correo`             | String  | Email (único vía GSI)            |
| `telefono`           | String  | Número de celular                |
| `placa`              | String  | Placa del vehículo (único)       |
| `contrasenaHasheada` | String  | PBKDF2 hash                      |
| `fotoUrl`            | String  | URL de foto en S3                |
| `marca`              | String  | Marca del vehículo               |
| `color`              | String  | Color del vehículo               |
| `sumaCalificaciones` | Number  | Suma total de calificaciones     |
| `totalCalificaciones`| Number  | Conteo de calificaciones         |
| `activo`             | Boolean | Disponible para recibir viajes   |
| `autorizadoPorAdmin` | Boolean | Aprobado por administrador       |
| `creadoEn`           | String  | ISO timestamp                    |

**GSI**: `CorreoIndex` (PK: correo), `PlacaIndex` (PK: placa)

### 6.3 `servicios`

| Atributo               | Tipo    | Descripción                        |
|-----------------------|---------|-------------------------------------|
| `serviceId` (PK)      | String  | UUID del servicio                   |
| `usuarioId`           | String  | ID del pasajero                     |
| `driverId`            | String  | ID del conductor ("NONE" si no asignado) |
| `estado`              | String  | PENDIENTE/EN_CAMINO/EN_CURSO/COMPLETADO/CANCELADO |
| `origen`              | Map     | {lat, lng, direccion}               |
| `destino`             | Map     | {lat, lng, direccion}               |
| `precioSugerido`      | Number  | Precio propuesto por el pasajero    |
| `precioFinal`         | Number  | Precio acordado                     |
| `comentario`          | String  | Nota del pasajero                   |
| `nombreUsuario`       | String  | Nombre del pasajero                 |
| `nombreConductor`     | String  | Nombre del conductor                |
| `telefonoConductor`   | String  | Teléfono del conductor              |
| `placaConductor`      | String  | Placa del conductor                 |
| `ubicacionConductor`  | Map     | {lat, lng} — última ubicación GPS   |
| `calificacionUsuario` | Number  | Estrellas dadas al conductor (1-5)  |
| `calificacionConductor` | Number | Estrellas dadas al usuario (1-5)  |
| `comentarioUsuario`   | String  | Comentario del usuario              |
| `comentarioConductor` | String  | Comentario del conductor            |
| `canceladoPor`        | String  | USUARIO o CONDUCTOR                 |
| `motivoCancelacion`   | String  | Motivo de la cancelación            |
| `creadoEn`            | String  | ISO — momento de creación           |
| `aceptadoEn`          | String  | ISO — momento de aceptación         |
| `iniciadoEn`          | String  | ISO — momento de inicio del viaje   |
| `completadoEn`        | String  | ISO — momento de finalización       |
| `canceladoEn`         | String  | ISO — momento de cancelación        |
| `actualizadoEn`       | String  | ISO — última actualización          |

**GSI**:
- `UsuarioFechaIndex` (PK: usuarioId, SK: creadoEn) — Historial del pasajero
- `ConductorFechaIndex` (PK: driverId, SK: creadoEn) — Historial del conductor
- `UsuarioEstadoIndex` (PK: usuarioId, SK: estado) — Anti-duplicidad de pedidos

### 6.4 `conexiones`

| Atributo          | Tipo    | Descripción                     |
|------------------|---------|----------------------------------|
| `connectionId` (PK) | String | ID de la conexión WebSocket    |
| `userId`         | String  | ID del usuario o conductor       |
| `rol`            | String  | USUARIO o CONDUCTOR              |
| `nombre`         | String  | Nombre para mostrar              |
| `correo`         | String  | Email del conectado              |
| `conectadoEn`    | String  | ISO timestamp                    |
| `ttl`            | Number  | Unix timestamp — auto-expiración |

**GSI**: `UserIdIndex` (PK: userId) — Buscar conexiones por usuario
**TTL**: Habilitado en campo `ttl` — limpieza automática de conexiones muertas

---

## 7. Flujo Completo de un Viaje

```
PASAJERO                          SERVIDOR                          CONDUCTOR
   │                                 │                                 │
   │──servicioRequerido─────────────▶│                                 │
   │                                 │──nuevoServicio────────────────▶│ (broadcast)
   │◀──servicioCreado────────────────│                                 │
   │                                 │                                 │
   │                                 │◀──aceptarServicio──────────────│
   │◀──servicioAceptado──────────────│                                 │
   │                                 │──servicioAceptadoConfirmacion─▶│
   │                                 │──servicioTomado───────────────▶│ (otros)
   │                                 │                                 │
   │                                 │◀──registrarUbicacionMoto───────│ (cada 3-5s)
   │◀──ubicacionConductor────────────│                                 │
   │                                 │                                 │
   │                                 │◀──iniciarViaje─────────────────│
   │◀──viajeIniciado────────────────│                                 │
   │                                 │──viajeIniciadoConfirmacion────▶│
   │                                 │                                 │
   │◀─────informar──────────────────▶│◀──────────informar────────────▶│
   │                                 │                                 │
   │                                 │◀──completarViaje───────────────│
   │◀──viajeCompletado──────────────│                                 │
   │                                 │──viajeCompletadoConfirmacion──▶│
   │                                 │                                 │
   │──POST /calificar───────────────▶│                                 │
   │                                 │◀──POST /calificar──────────────│
```

### Estados del Servicio

```
PENDIENTE ──▶ EN_CAMINO ──▶ EN_CURSO ──▶ COMPLETADO
    │              │
    └──────────────┴──────────▶ CANCELADO
```

| Estado       | Descripción                                        |
|-------------|-----------------------------------------------------|
| `PENDIENTE`  | Pasajero solicitó, esperando conductor              |
| `EN_CAMINO`  | Conductor aceptó, va al punto de recojo             |
| `EN_CURSO`   | Conductor recogió al pasajero, viaje en progreso    |
| `COMPLETADO` | Viaje finalizado exitosamente                       |
| `CANCELADO`  | Cancelado por pasajero o conductor (antes de EN_CURSO) |

---

## 8. Reconexión WebSocket

Dado que Tarma está a 3,048 m s.n.m. con posibles micro-cortes de señal, el frontend debe implementar reconexión automática:

### Estrategia Recomendada (Flutter)

```dart
class WebSocketService {
  WebSocket? _ws;
  String? _token;
  Timer? _pingTimer;
  int _reconnectAttempts = 0;
  static const int _maxReconnectDelay = 30; // segundos

  Future<void> connect(String token) async {
    _token = token;
    final url = 'wss://YOUR_WS_ID.execute-api.us-east-1.amazonaws.com/dev?token=$token';

    try {
      _ws = await WebSocket.connect(url);
      _reconnectAttempts = 0;

      // Heartbeat cada 5 minutos
      _pingTimer = Timer.periodic(Duration(minutes: 5), (_) {
        _ws?.add(json.encode({'action': 'ping'}));
      });

      _ws!.listen(
        (data) => _handleMessage(json.decode(data)),
        onDone: () => _reconnect(),
        onError: (_) => _reconnect(),
      );
    } catch (e) {
      _reconnect();
    }
  }

  void _reconnect() {
    _pingTimer?.cancel();
    if (_token == null) return;

    // Backoff exponencial con jitter (máx 30s)
    final delay = min(
      pow(2, _reconnectAttempts).toInt(),
      _maxReconnectDelay,
    ) + Random().nextInt(3);
    _reconnectAttempts++;

    Future.delayed(Duration(seconds: delay), () => connect(_token!));
  }
}
```

### Consideraciones Clave

1. **Ping cada 5 minutos**: Mantiene la conexión viva y renueva el TTL.
2. **Backoff exponencial**: 1s, 2s, 4s, 8s, 16s, 30s (máximo).
3. **Jitter aleatorio**: Evita que todos los clientes reconecten al mismo tiempo.
4. **Token reusable**: El JWT dura 72 horas, ideal para conectividad intermitente.
5. **Estado local**: El frontend debe cachear el estado del viaje activo localmente.

---

## 9. Códigos de Error

### REST

| Código | Tipo                    | Descripción                          |
|--------|------------------------|--------------------------------------|
| 200    | OK                     | Operación exitosa                    |
| 201    | Created                | Recurso creado (registro)            |
| 400    | Bad Request            | Datos faltantes o inválidos          |
| 401    | Unauthorized           | Token ausente o inválido             |
| 403    | Forbidden              | Sin permisos para esta acción        |
| 404    | Not Found              | Recurso no encontrado                |
| 409    | Conflict               | Duplicado (correo, placa, etc.)      |
| 503    | Service Unavailable    | Servicio no configurado (S3)         |

### WebSocket

Los errores se devuelven como mensajes JSON:

```json
{
  "error": "Descripción del error"
}
```

### Headers en Todas las Respuestas REST

```json
{
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS"
}
```

---

## JWT Token

### Payload del Token

```json
{
  "sub": "user-or-driver-uuid",
  "correo": "correo@email.com",
  "rol": "USUARIO",
  "nombre": "Carlos",
  "iat": 1709550000,
  "exp": 1709809200
}
```

- **Duración**: 72 horas (3 días) — ventana amplia para la sierra.
- **Algoritmo**: HS256 (HMAC-SHA256).
- **Roles**: `USUARIO` o `CONDUCTOR`.

### Uso en Headers HTTP

```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Uso en WebSocket

```
wss://...?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```
