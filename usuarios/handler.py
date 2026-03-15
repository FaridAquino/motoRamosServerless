"""
MotoRamos — Microservicio de Usuarios (Pasajeros)
REST API: Registro, Login, Perfil, Historial, Calificaciones, Foto S3.

Diseñado para los ciudadanos y turistas de Tarma, Junín.
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
USUARIOS_TABLE = os.environ['usuariosTable']
SERVICIOS_TABLE = os.environ['serviciosTable']
CONDUCTORES_TABLE = os.environ['conductoresTable']
FOTOS_BUCKET = os.environ.get('fotosBucket', '')

dynamodb = boto3.resource('dynamodb')
ZONA_PERU = ZoneInfo('America/Lima')


# ═══════════════════════════════════════════════════════════════════════════════
# POST /registerUsuario  (público)
# ═══════════════════════════════════════════════════════════════════════════════
def registerUsuario(event, context):
    """Registra un nuevo pasajero. Devuelve token JWT al registrarse."""
    body = extract_body(event)
    if not body:
        return error('Falta el body de la solicitud')

    required = ['nombre', 'apellido','telefono', 'contrasena']
    missing = [f for f in required if f not in body or not body[f]]
    if missing:
        return error(f'Campos requeridos faltantes: {", ".join(missing)}')

    tabla = dynamodb.Table(USUARIOS_TABLE)

    # Verificar teléfono duplicado
    dup = tabla.query(
        IndexName='TelefonoIndex',
        KeyConditionExpression=Key('telefono').eq(body['telefono']),
        Select='COUNT',
    )
    if dup['Count'] > 0:
        return error('Ya existe un usuario registrado con ese teléfono', 409)

    ahora = datetime.now(ZONA_PERU).isoformat()
    user_id = str(uuid.uuid4())

    item = {
        'userId': user_id,
        'telefono': body['telefono'],
        'nombre': body['nombre'],
        'apellido': body['apellido'],
        'contrasenaHasheada': hash_password(body['contrasena']),
        'fotoUrl': '',
        'sumaCalificaciones': Decimal('0'),
        'totalCalificaciones': Decimal('0'),
        'activo': True,
        'creadoEn': ahora,
    }

    tabla.put_item(Item=item)

    token = generate_jwt({
        'sub': user_id,
        'telefono': body['telefono'],
        'rol': 'USUARIO',
        'nombre': body['nombre'],
    })

    return success({
        'message': 'Usuario registrado exitosamente',
        'userId': user_id,
        'token': token,
    }, 201)


# ═══════════════════════════════════════════════════════════════════════════════
# POST /loginUsuario  (público)
# ═══════════════════════════════════════════════════════════════════════════════
def loginUsuario(event, context):
    """Login de pasajero. Devuelve token JWT firmado."""
    body = extract_body(event)
    if not body:
        return error('Falta el body de la solicitud')

    telefono = body.get('telefono')
    contrasena = body.get('contrasena')
    if not telefono or not contrasena:
        return error('Teléfono y contraseña son requeridos')

    tabla = dynamodb.Table(USUARIOS_TABLE)
    result = tabla.query(
        IndexName='TelefonoIndex',
        KeyConditionExpression=Key('telefono').eq(telefono),
    )
    items = result.get('Items', [])
    if not items:
        return error('Usuario no encontrado', 404)

    usuario = items[0]
    if not usuario.get('activo', True):
        return error('Cuenta desactivada', 403)

    if not verify_password(contrasena, usuario.get('contrasenaHasheada', '')):
        return error('Contraseña incorrecta', 401)

    token = generate_jwt({
        'sub': usuario['userId'],
        'telefono': usuario.get('telefono'),
        'rol': 'USUARIO',
        'nombre': usuario.get('nombre', ''),
    })

    return success({
        'message': 'Login exitoso',
        'userId': usuario['userId'],
        'nombre': usuario.get('nombre'),
        'apellido': usuario.get('apellido'),
        'token': token,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# GET /perfil  (auth)
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getPerfilUsuario(event, context):
    """Devuelve el perfil del usuario autenticado (sin contraseña)."""
    claims = event['authClaims']
    tabla = dynamodb.Table(USUARIOS_TABLE)
    result = tabla.get_item(Key={'userId': claims['sub']})
    if 'Item' not in result:
        return error('Usuario no encontrado', 404)

    u = result['Item']
    u.pop('contrasenaHasheada', None)

    # Calcular promedio de calificación
    total = int(u.get('totalCalificaciones', 0))
    if total > 0:
        u['calificacionPromedio'] = round(
            float(u.get('sumaCalificaciones', 0)) / total, 2
        )
    else:
        u['calificacionPromedio'] = 5.0

    return success({'usuario': u})


# ═══════════════════════════════════════════════════════════════════════════════
# PUT /perfil  (auth)
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def updatePerfilUsuario(event, context):
    """Actualiza nombre, apellido, teléfono o edad del usuario."""
    claims = event['authClaims']
    body = extract_body(event)
    if not body:
        return error('Falta el body')

    allowed = ['nombre', 'apellido', 'telefono']
    expr_parts, values, names = [], {}, {}

    for campo in allowed:
        if campo in body:
            safe = f'#a_{campo}'
            placeholder = f':v_{campo}'
            expr_parts.append(f'{safe} = {placeholder}')
            val = body[campo]
            values[placeholder] = val
            names[safe] = campo

    if not expr_parts:
        return error('No se proporcionaron campos para actualizar')

    tabla = dynamodb.Table(USUARIOS_TABLE)
    tabla.update_item(
        Key={'userId': claims['sub']},
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
    """Genera una URL pre-firmada PUT para que el cliente suba su foto directamente a S3."""
    if not FOTOS_BUCKET:
        return error('Servicio de fotos no configurado', 503)

    claims = event['authClaims']
    s3 = boto3.client('s3')
    key = f"usuarios/{claims['sub']}/foto.jpg"

    upload_url = s3.generate_presigned_url(
        'put_object',
        Params={'Bucket': FOTOS_BUCKET, 'Key': key, 'ContentType': 'image/jpeg'},
        ExpiresIn=300,
    )
    foto_url = f"https://{FOTOS_BUCKET}.s3.amazonaws.com/{key}"

    # Guardar la URL de la foto en el perfil
    tabla = dynamodb.Table(USUARIOS_TABLE)
    tabla.update_item(
        Key={'userId': claims['sub']},
        UpdateExpression='SET fotoUrl = :u',
        ExpressionAttributeValues={':u': foto_url},
    )

    return success({'uploadUrl': upload_url, 'fotoUrl': foto_url})


# ═══════════════════════════════════════════════════════════════════════════════
# GET /historial  (auth) — Viajes pasados del usuario
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getHistorialUsuario(event, context):
    """Consulta servicios pasados del usuario. Soporta filtros ?desde=&hasta=&limit=."""
    claims = event['authClaims']
    params = event.get('queryStringParameters') or {}

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    key_cond = Key('usuarioId').eq(claims['sub'])

    if 'desde' in params and 'hasta' in params:
        key_cond = key_cond & Key('creadoEn').between(params['desde'], params['hasta'])
    elif 'desde' in params:
        key_cond = key_cond & Key('creadoEn').gte(params['desde'])

    result = tabla.query(
        IndexName='UsuarioFechaIndex',
        KeyConditionExpression=key_cond,
        ScanIndexForward=False,  # Más recientes primero
        Limit=int(params.get('limit', '20')),
    )

    return success({
        'servicios': result.get('Items', []),
        'count': result.get('Count', 0),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# POST /calificar  (auth) — Usuario califica al conductor después de un viaje
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def calificarConductor(event, context):
    """Registra una calificación (1-5 estrellas) del usuario al conductor."""
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
    if serv.get('usuarioId') != claims['sub']:
        return error('No autorizado para calificar este servicio', 403)
    if serv.get('estado') != 'COMPLETADO':
        return error('Solo se pueden calificar servicios completados')
    if serv.get('calificacionUsuario') is not None:
        return error('Ya calificaste este servicio')

    # Guardar calificación en el servicio
    tabla_serv.update_item(
        Key={'serviceId': service_id},
        UpdateExpression='SET calificacionUsuario = :c, comentarioUsuario = :m',
        ExpressionAttributeValues={
            ':c': Decimal(str(puntuacion)),
            ':m': comentario,
        },
    )

    # Actualizar suma y conteo del conductor (operación atómica con ADD)
    driver_id = serv.get('driverId')
    if driver_id:
        tabla_cond = dynamodb.Table(CONDUCTORES_TABLE)
        tabla_cond.update_item(
            Key={'driverId': driver_id},
            UpdateExpression='ADD sumaCalificaciones :c, totalCalificaciones :uno',
            ExpressionAttributeValues={
                ':c': Decimal(str(puntuacion)),
                ':uno': Decimal('1'),
            },
        )

    return success({'message': 'Calificación registrada exitosamente'})

# ═══════════════════════════════════════════════════════════════════════════════
# GET /informacion_servicio (auth) — Obtener información de un servicio específico
# ═══════════════════════════════════════════════════════════════════════════════
@require_auth
def getInformacionServicio(event, context):
    """Devuelve detalles de un servicio específico, incluyendo estado y ubicación del conductor.
        https://moto-ramos.com/usuarios/servicio?serviceId=12345
    """
    claims = event['authClaims']
    service_id = event.get('queryStringParameters', {}).get('serviceId')

    if not service_id:
        return error('serviceId es requerido')

    tabla_serv = dynamodb.Table(SERVICIOS_TABLE)
    result = tabla_serv.get_item(Key={'serviceId': service_id})
    if 'Item' not in result:
        return error('Servicio no encontrado', 404)

    serv = result['Item']
    if serv.get('usuarioId') != claims['sub']:
        return error('No autorizado para ver este servicio', 403)

    return success({
        'serviceId': serv.get('serviceId'),
        'driverId': serv.get('driverId'),
        'nombreConductor': serv.get('nombreConductor'),
        'placaConductor': serv.get('placaConductor'),
        'colorVehiculo': serv.get('colorVehiculo', ''),
        'numeroVehiculo': serv.get('numeroVehiculo', ''),
        'ubicacionConductor': serv.get('ubicacionConductor', {}),
        'origen': serv.get('origen', {}),
        'destino': serv.get('destino', {}),
        'precioFinal': serv.get('precioFinal', 0),
    })
