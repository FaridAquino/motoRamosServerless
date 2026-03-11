"""
MotoRamos — Microservicio de Conductores
REST API: Registro, Login, Perfil, Historial, Toggle-activo, Foto S3, Calificar usuario.

Diseñado para los conductores de mototaxi de Tarma, Junín.
Todas las contraseñas se almacenan con PBKDF2-SHA256 (600 000 iteraciones).
Cada respuesta incluye headers CORS para integración con Flutter.
"""

import json
import os
import uuid
import boto3
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo
from boto3.dynamodb.conditions import Key

from auth_utils import (
    generate_jwt, hash_password, verify_password,
    success, error, require_auth, extract_body,
)

# ─── Tablas y recursos ──────────────────────────────────────────────────────
CONDUCTORES_TABLE = os.environ['conductoresTable']
SERVICIOS_TABLE = os.environ['serviciosTable']
USUARIOS_TABLE = os.environ['usuariosTable']
FOTOS_BUCKET = os.environ.get('fotosBucket', '')

dynamodb = boto3.resource('dynamodb')
ZONA_PERU = ZoneInfo('America/Lima')


# ═══════════════════════════════════════════════════════════════════════════════
# POST /registerConductor  (público)
# ═══════════════════════════════════════════════════════════════════════════════
def registerConductor(event, context):
    """Registra un nuevo conductor de mototaxi. Devuelve token JWT."""
    body = extract_body(event)
    if not body:
        return error('Falta el body de la solicitud')

    required = ['nombre', 'apellido', 'telefono', 'contrasena', 'placa']
    missing = [f for f in required if f not in body or not body[f]]
    if missing:
        return error(f'Campos requeridos faltantes: {", ".join(missing)}')

    tabla = dynamodb.Table(CONDUCTORES_TABLE)

    # Verificar teléfono duplicado
    dup = tabla.query(
        IndexName='TelefonoIndex',
        KeyConditionExpression=Key('telefono').eq(body['telefono']),
        Select='COUNT',
    )
    if dup['Count'] > 0:
        return error('Ya existe un conductor registrado con ese teléfono', 409)

    # Verificar placa duplicada
    dup_placa = tabla.query(
        IndexName='PlacaIndex',
        KeyConditionExpression=Key('placa').eq(body['placa'].upper()),
        Select='COUNT',
    )
    if dup_placa['Count'] > 0:
        return error('Ya existe un conductor registrado con esa placa', 409)

    ahora = datetime.now(ZONA_PERU).isoformat()
    driver_id = str(uuid.uuid4())

    item = {
        'driverId': driver_id,
        'nombre': body['nombre'],
        'apellido': body['apellido'],
        'telefono': body['telefono'],
        'placa': body['placa'].upper(),
        'contrasenaHasheada': hash_password(body['contrasena']),
        'fotoUrl': '',
        'marca': body.get('marca', ''),
        'color': body.get('color', ''),
        'sumaCalificaciones': Decimal('0'),
        'totalCalificaciones': Decimal('0'),
        'activo': False,         # Inicia como NO disponible
        'autorizadoPorAdmin': False,
        'creadoEn': ahora,
    }

    tabla.put_item(Item=item)

    token = generate_jwt({
        'sub': driver_id,
        'telefono': body['telefono'],
        'rol': 'CONDUCTOR',
        'nombre': body['nombre'],
    })

    return success({
        'message': 'Conductor registrado exitosamente. Esperando autorización del admin.',
        'driverId': driver_id,
        'token': token,
    }, 201)


# ═══════════════════════════════════════════════════════════════════════════════
# POST /loginConductor  (público)
# ═══════════════════════════════════════════════════════════════════════════════
def loginConductor(event, context):
    """Login de conductor. Devuelve token JWT firmado."""
    body = extract_body(event)
    if not body:
        return error('Falta el body de la solicitud')

    telefono = body.get('telefono')
    contrasena = body.get('contrasena')
    if not telefono or not contrasena:
        return error('Teléfono y contraseña son requeridos')

    tabla = dynamodb.Table(CONDUCTORES_TABLE)
    result = tabla.query(
        IndexName='TelefonoIndex',
        KeyConditionExpression=Key('telefono').eq(telefono),
    )
    items = result.get('Items', [])
    if not items:
        return error('Conductor no encontrado', 404)

    conductor = items[0]

    if not verify_password(contrasena, conductor.get('contrasenaHasheada', '')):
        return error('Contraseña incorrecta', 401)

    token = generate_jwt({
        'sub': conductor['driverId'],
        'telefono': conductor.get('telefono'),
        'rol': 'CONDUCTOR',
        'nombre': conductor.get('nombre', ''),
    })

    return success({
        'message': 'Login exitoso',
        'driverId': conductor['driverId'],
        'nombre': conductor.get('nombre'),
        'apellido': conductor.get('apellido'),
        'activo': conductor.get('activo', False),
        'autorizadoPorAdmin': conductor.get('autorizadoPorAdmin', False),
        'token': token,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# GET /perfil  (auth — CONDUCTOR)
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getPerfilConductor(event, context):
    """Devuelve el perfil del conductor autenticado (sin contraseña)."""
    claims = event['authClaims']
    tabla = dynamodb.Table(CONDUCTORES_TABLE)
    result = tabla.get_item(Key={'driverId': claims['sub']})
    if 'Item' not in result:
        return error('Conductor no encontrado', 404)

    c = result['Item']
    c.pop('contrasenaHasheada', None)

    # Calcular promedio de calificación
    total = int(c.get('totalCalificaciones', 0))
    if total > 0:
        c['calificacionPromedio'] = round(
            float(c.get('sumaCalificaciones', 0)) / total, 2
        )
    else:
        c['calificacionPromedio'] = 5.0

    return success({'conductor': c})


# ═══════════════════════════════════════════════════════════════════════════════
# PUT /perfil  (auth — CONDUCTOR)
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def updatePerfilConductor(event, context):
    """Actualiza nombre, apellido, teléfono, placa, marca o color del conductor."""
    claims = event['authClaims']
    body = extract_body(event)
    if not body:
        return error('Falta el body')

    allowed = ['nombre', 'apellido', 'telefono', 'placa', 'marca', 'color']
    expr_parts, values, names = [], {}, {}

    for campo in allowed:
        if campo in body:
            safe = f'#a_{campo}'
            placeholder = f':v_{campo}'
            expr_parts.append(f'{safe} = {placeholder}')
            val = body[campo]
            if campo == 'placa':
                val = val.upper()
            values[placeholder] = val
            names[safe] = campo

    if not expr_parts:
        return error('No se proporcionaron campos para actualizar')

    tabla = dynamodb.Table(CONDUCTORES_TABLE)
    tabla.update_item(
        Key={'driverId': claims['sub']},
        UpdateExpression='SET ' + ', '.join(expr_parts),
        ExpressionAttributeValues=values,
        ExpressionAttributeNames=names,
    )

    return success({'message': 'Perfil actualizado exitosamente'})


# ═══════════════════════════════════════════════════════════════════════════════
# GET /perfil/foto-url  (auth) — URL pre-firmada para subir foto a S3
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getPresignedUploadUrl(event, context):
    """Genera una URL pre-firmada PUT para que el conductor suba su foto a S3."""
    if not FOTOS_BUCKET:
        return error('Servicio de fotos no configurado', 503)

    claims = event['authClaims']
    s3 = boto3.client('s3')
    key = f"conductores/{claims['sub']}/foto.jpg"

    upload_url = s3.generate_presigned_url(
        'put_object',
        Params={'Bucket': FOTOS_BUCKET, 'Key': key, 'ContentType': 'image/jpeg'},
        ExpiresIn=300,
    )
    foto_url = f"https://{FOTOS_BUCKET}.s3.amazonaws.com/{key}"

    # Guardar la URL de la foto en el perfil
    tabla = dynamodb.Table(CONDUCTORES_TABLE)
    tabla.update_item(
        Key={'driverId': claims['sub']},
        UpdateExpression='SET fotoUrl = :u',
        ExpressionAttributeValues={':u': foto_url},
    )

    return success({'uploadUrl': upload_url, 'fotoUrl': foto_url})


# ═══════════════════════════════════════════════════════════════════════════════
# PUT /toggle-activo  (auth) — Conductor se pone en línea / fuera de línea
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def toggleActivo(event, context):
    """Activa o desactiva la disponibilidad del conductor.
    En Tarma es clave: un conductor que baja a descansar a Acobamba
    marca su estado como inactivo para no recibir pedidos."""
    claims = event['authClaims']
    body = extract_body(event)
    if not body or 'activo' not in body:
        return error('Campo "activo" (true/false) es requerido')

    nuevo_estado = bool(body['activo'])
    tabla = dynamodb.Table(CONDUCTORES_TABLE)

    # Verificar autorización del admin
    res = tabla.get_item(Key={'driverId': claims['sub']}, ProjectionExpression='autorizadoPorAdmin')
    if 'Item' not in res:
        return error('Conductor no encontrado', 404)
    if not res['Item'].get('autorizadoPorAdmin', False):
        return error('Tu cuenta aún no ha sido autorizada por un administrador', 403)

    tabla.update_item(
        Key={'driverId': claims['sub']},
        UpdateExpression='SET activo = :a',
        ExpressionAttributeValues={':a': nuevo_estado},
    )

    return success({
        'message': f'Estado actualizado: {"EN LÍNEA" if nuevo_estado else "FUERA DE LÍNEA"}',
        'activo': nuevo_estado,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# GET /historial  (auth) — Viajes pasados del conductor
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getHistorialConductor(event, context):
    """Consulta servicios pasados del conductor. Filtros opcionales: ?desde=&hasta=&limit=."""
    claims = event['authClaims']
    params = event.get('queryStringParameters') or {}

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    key_cond = Key('driverId').eq(claims['sub'])

    if 'desde' in params and 'hasta' in params:
        key_cond = key_cond & Key('creadoEn').between(params['desde'], params['hasta'])
    elif 'desde' in params:
        key_cond = key_cond & Key('creadoEn').gte(params['desde'])

    result = tabla.query(
        IndexName='ConductorFechaIndex',
        KeyConditionExpression=key_cond,
        ScanIndexForward=False,  # Más recientes primero
        Limit=int(params.get('limit', '20')),
    )

    return success({
        'servicios': result.get('Items', []),
        'count': result.get('Count', 0),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# POST /calificar  (auth — CONDUCTOR califica al USUARIO)
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def calificarUsuario(event, context):
    """Registra una calificación (1-5 estrellas) del conductor al usuario."""
    claims = event['authClaims']
    body = extract_body(event)
    if not body:
        return error('Falta el body')

    service_id = body.get('serviceId')
    puntuacion = body.get('puntuacion')
    comentario = body.get('comentario', '')

    if not service_id:
        return error('serviceId es requerido')
    if not isinstance(puntuacion, (int, float)) or not (1 <= puntuacion <= 5):
        return error('La puntuación debe ser un número entre 1 y 5')

    tabla_serv = dynamodb.Table(SERVICIOS_TABLE)
    result = tabla_serv.get_item(Key={'serviceId': service_id})
    if 'Item' not in result:
        return error('Servicio no encontrado', 404)

    serv = result['Item']
    if serv.get('driverId') != claims['sub']:
        return error('No autorizado para calificar este servicio', 403)
    if serv.get('estado') != 'COMPLETADO':
        return error('Solo se pueden calificar servicios completados')
    if serv.get('calificacionConductor') is not None:
        return error('Ya calificaste este servicio')

    # Guardar calificación en el servicio
    tabla_serv.update_item(
        Key={'serviceId': service_id},
        UpdateExpression='SET calificacionConductor = :c, comentarioConductor = :m',
        ExpressionAttributeValues={
            ':c': Decimal(str(puntuacion)),
            ':m': comentario,
        },
    )

    # Actualizar suma y conteo del usuario (operación atómica con ADD)
    user_id = serv.get('usuarioId')
    if user_id:
        tabla_usr = dynamodb.Table(USUARIOS_TABLE)
        tabla_usr.update_item(
            Key={'userId': user_id},
            UpdateExpression='ADD sumaCalificaciones :c, totalCalificaciones :uno',
            ExpressionAttributeValues={
                ':c': Decimal(str(puntuacion)),
                ':uno': Decimal('1'),
            },
        )

    return success({'message': 'Calificación registrada exitosamente'})


# ═══════════════════════════════════════════════════════════════════════════════
# GET /ganancias  (auth) — Resumen de ganancias del conductor
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getGananciasConductor(event, context):
    """Devuelve resumen de ganancias: hoy, esta semana, este mes.
    Útil para conductores tarmeños que necesitan calcular sus ingresos diarios."""
    claims = event['authClaims']
    params = event.get('queryStringParameters') or {}

    ahora = datetime.now(ZONA_PERU)
    desde = params.get('desde', ahora.strftime('%Y-%m-%d') + 'T00:00:00')
    hasta = params.get('hasta', ahora.isoformat())

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    result = tabla.query(
        IndexName='ConductorFechaIndex',
        KeyConditionExpression=(
            Key('driverId').eq(claims['sub']) &
            Key('creadoEn').between(desde, hasta)
        ),
        FilterExpression='estado = :e',
        ExpressionAttributeValues={':e': 'COMPLETADO'},
    )

    servicios = result.get('Items', [])
    total = sum(float(s.get('precio', 0)) for s in servicios)

    return success({
        'desde': desde,
        'hasta': hasta,
        'totalServicios': len(servicios),
        'totalGanancia': round(total, 2),
        'servicios': servicios,
    })
