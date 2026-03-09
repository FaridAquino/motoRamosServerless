# MotoRamos Backend — Serverless

Plataforma de transporte en mototaxi para Tarma, Junín (3,048 m s.n.m.).  
Proyecto de Ingeniería — UTEC 2026.

## Arquitectura

| Microservicio | Tipo | Descripción |
|---|---|---|
| `serviciosNecesarios/` | Infra (CloudFormation) | Tablas DynamoDB + Bucket S3 |
| `usuarios/` | REST API (Lambda + API Gateway) | Auth, perfil, historial, calificaciones de pasajeros |
| `conductores/` | REST API (Lambda + API Gateway) | Auth, perfil, toggle activo, ganancias de conductores |
| `webSocket/` | WebSocket API (Lambda + API Gateway v2) | Viajes en tiempo real, ubicación GPS, chat |

## Stack Tecnológico

- **Runtime**: Python 3.12
- **Framework**: Serverless Framework
- **Base de Datos**: DynamoDB (PAY_PER_REQUEST)
- **Almacenamiento**: S3 (fotos de perfil)
- **Auth**: JWT HS256 (sin dependencias externas)
- **Passwords**: PBKDF2-SHA256 (600,000 iteraciones)
- **Región**: us-east-1

## Despliegue

```bash
# 1. Infraestructura primero
cd serviciosNecesarios && serverless deploy --stage dev

# 2. APIs REST
cd ../usuarios && serverless deploy --stage dev
cd ../conductores && serverless deploy --stage dev

# 3. WebSocket
cd ../webSocket && serverless deploy --stage dev
```

## Documentación

Ver [API_DOCUMENTATION.md](API_DOCUMENTATION.md) para el manual completo de integración frontend.

## Tablas DynamoDB

| Tabla | PK | GSIs |
|---|---|---|
| `usuarios` | `userId` | CorreoIndex |
| `conductores` | `driverId` | CorreoIndex, PlacaIndex |
| `servicios` | `serviceId` | UsuarioFechaIndex, ConductorFechaIndex, UsuarioEstadoIndex |
| `conexiones` | `connectionId` | UserIdIndex (+ TTL auto-cleanup) |

## Estados de un Viaje

```
PENDIENTE → EN_CAMINO → EN_CURSO → COMPLETADO
    ↓           ↓
    └───────────┴──→ CANCELADO
```