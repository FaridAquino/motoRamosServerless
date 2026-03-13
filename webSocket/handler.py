"""
MotoRamos — Microservicio WebSocket
Gestión en tiempo real: solicitud de viaje, asignación, ubicación, cancelación,
completar viaje e informar estado.

Diseñado para operar con conectividad intermitente en Tarma (3 048 m s.n.m.).
Incluye reconexión robusta y operaciones DynamoDB atómicas para evitar duplicidad.

Flujo de un servicio:
  1. Pasajero envía 'servicioRequerido' → se crea servicio PENDIENTE
  2. Conductores activos reciben la notificación → mandan 'aceptarServicio'
  3. Se transmite el servicio a todos los conductores en estado "TRABAJANDO" → estado EN_CAMINO
  4. Conductor actualiza ubicación con 'registrarUbicacionMoto'
  5. Conductor envía 'iniciarViaje' cuando recoge al pasajero → EN_CURSO
  6. Conductor envía 'completarViaje' al llegar a destino → COMPLETADO
  7. Cualquiera puede enviar 'cancelarServicio' antes de EN_CURSO → CANCELADO
  8. 'informar' → envío genérico de mensajes entre pasajero y conductor
"""

import json
import os
import uuid
import boto3
import time
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo
from boto3.dynamodb.conditions import Key, Attr
import urllib.request
import urllib.parse

from auth_utils import verify_jwt

# ─── Tablas y recursos ──────────────────────────────────────────────────────
CONEXIONES_TABLE = os.environ['conexionesTable']
SERVICIOS_TABLE = os.environ['serviciosTable']
CONDUCTORES_TABLE = os.environ['conductoresTable']
USUARIOS_TABLE = os.environ['usuariosTable']

dynamodb = boto3.resource('dynamodb')
ZONA_PERU = ZoneInfo('America/Lima')


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers internos
# ═══════════════════════════════════════════════════════════════════════════════

def _json_serial(obj):
    """Serializer para Decimal y tipos no estándar de DynamoDB."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f'Tipo no serializable: {type(obj)}')


def _get_apigw_client(event):
    """Construye el cliente de API Gateway Management para enviar mensajes WebSocket."""
    domain = event['requestContext']['domainName']
    stage = event['requestContext']['stage']
    return boto3.client(
        'apigatewaymanagementapi',
        endpoint_url=f'https://{domain}/{stage}',
    )


def _send_to_connection(apigw, connection_id, payload: dict):
    """Envía un mensaje JSON a una conexión WebSocket específica.
    Si la conexión ya no existe, la elimina de DynamoDB (limpieza por desconexión)."""
    try:
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload, default=_json_serial, ensure_ascii=False).encode(),
        )
    except apigw.exceptions.GoneException:
        # Conexión muerta — limpiar registro
        dynamodb.Table(CONEXIONES_TABLE).delete_item(
            Key={'connectionId': connection_id}
        )
    except Exception:
        pass  # No fallar por una conexión problemática


def _notify_user(apigw, user_id: str, payload: dict):
    """Busca todas las conexiones activas del usuario y les envía el mensaje.
    Un usuario puede tener múltiples conexiones (app + web)."""
    tabla = dynamodb.Table(CONEXIONES_TABLE)
    result = tabla.query(
        IndexName='UserIdIndex',
        KeyConditionExpression=Key('userId').eq(user_id),
    )
    for conn in result.get('Items', []):
        _send_to_connection(apigw, conn['connectionId'], payload)


def _notify_driver(apigw, driver_id: str, payload: dict):
    """Busca todas las conexiones activas del conductor y les envía el mensaje."""
    tabla = dynamodb.Table(CONEXIONES_TABLE)
    result = tabla.query(
        IndexName='UserIdIndex',
        KeyConditionExpression=Key('userId').eq(driver_id),
    )
    for conn in result.get('Items', []):
        _send_to_connection(apigw, conn['connectionId'], payload)


def _broadcast_to_active_drivers(apigw, payload: dict, exclude_id: str = ''):
    """Envía mensaje a TODOS los conductores activos —
    esencial para difundir nuevas solicitudes de viaje cerca de la Plaza de Armas."""
    tabla_cond = dynamodb.Table(CONDUCTORES_TABLE)
    tabla_conn = dynamodb.Table(CONEXIONES_TABLE)

    # Scan de conductores activos (para Tarma la cantidad es manejable)
    result = tabla_cond.scan( #poner activo como un GSI con tipo string.
        FilterExpression=Attr('activo').eq(True),
        ProjectionExpression='driverId',
    )
    for driver in result.get('Items', []):
        did = driver['driverId']
        if did == exclude_id:
            continue
        conns = tabla_conn.query(
            IndexName='UserIdIndex',
            KeyConditionExpression=Key('userId').eq(did),
        )
        for conn in conns.get('Items', []):
            _send_to_connection(apigw, conn['connectionId'], payload)


def _ws_ok(body=None):
    return {'statusCode': 200, 'body': json.dumps(body or {'ok': True})}


def _ws_error(msg, code=400):
    return {'statusCode': code, 'body': json.dumps({'error': msg})}


def _parse_body(event) -> dict:
    """Parsea el body del mensaje WebSocket."""
    raw = event.get('body', '{}')
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return {}
    return raw if isinstance(raw, dict) else {}


def _get_claims(event) -> dict | None:
    """Extrae las claims del JWT almacenadas en la conexión."""
    conn_id = event['requestContext']['connectionId']
    tabla = dynamodb.Table(CONEXIONES_TABLE)
    result = tabla.get_item(Key={'connectionId': conn_id})
    if 'Item' not in result:
        return None
    item = result['Item']
    return {
        'sub': item.get('userId', ''),
        'rol': item.get('rol', ''),
        'nombre': item.get('nombre', ''),
        'telefono': item.get('telefono', ''),
    }


def _coordenadas_a_decimal(ubicacion) -> dict:
    """
    Convierte lat y lng de un diccionario a Decimal para DynamoDB.
    Usa str() intermedio para evitar errores de precisión de punto flotante.
    """
    if not ubicacion:
        return None
        
    # Hacemos una copia para no modificar el diccionario original por error
    ubi_segura = ubicacion.copy()
    
    if 'lat' in ubi_segura:
        ubi_segura['lat'] = Decimal(str(ubi_segura['lat']))
    if 'lng' in ubi_segura:
        ubi_segura['lng'] = Decimal(str(ubi_segura['lng']))
        
    return ubi_segura


def _obtener_distancia_tiempo(lat_origen, lon_origen, lat_destino, lon_destino) -> dict:
    # 1. Leer la variable que Serverless inyectó desde tu .env
    api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        print("Error: GOOGLE_MAPS_API_KEY no está configurada")
        return None

    # 2. Construir los parámetros
    params = {
        "origin": f"{lat_origen},{lon_origen}",
        "destination": f"{lat_destino},{lon_destino}",
        "mode": "driving",
        "key": api_key
    }
    
    # Codificar los parámetros en la URL
    query_string = urllib.parse.urlencode(params)
    url = f"https://maps.googleapis.com/maps/api/directions/json?{query_string}"

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as response:
            respuesta_json = json.loads(response.read().decode('utf-8'))

            if respuesta_json.get("status") == "OK":
                leg = respuesta_json["routes"][0]["legs"][0]
                return {
                    "distancia_texto": leg["distance"]["text"],
                    "distancia_metros": leg["distance"]["value"],
                    "tiempo_texto": leg["duration"]["text"],
                    "tiempo_segundos": leg["duration"]["value"]
                }
            else:
                print(f"Error en Google Maps API: {respuesta_json.get('status')}")
                return None
                
    except Exception as e:
        print(f"Error de conexión con Google Maps: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# $connect — Autenticación JWT al conectar al WebSocket
# ═══════════════════════════════════════════════════════════════════════════════
def connect(event, context):
    """Valida JWT en query string: wss://...?token=<jwt>
    En Tarma, la reconexión automática del frontend debe reenviar el token."""
    conn_id = event['requestContext']['connectionId']
    params = event.get('queryStringParameters') or {}
    token = params.get('token', '')

    if not token:
        return {'statusCode': 401, 'body': 'Token requerido'}

    claims = verify_jwt(token)
    if claims is None:
        return {'statusCode': 401, 'body': 'Token inválido o expirado'}

    # Registrar conexión
    tabla = dynamodb.Table(CONEXIONES_TABLE)
    tabla.put_item(Item={
        'connectionId': conn_id,
        'userId': claims['sub'],
        'rol': claims.get('rol', ''),
        'nombre': claims.get('nombre', ''),
        'telefono': claims.get('telefono', ''),
        'conectadoEn': datetime.now(ZONA_PERU).isoformat(),
        'ttl': int(time.time()) + 86400,  # Auto-expirar en 24h
    })

    return {'statusCode': 200, 'body': 'Conectado'}


# ═══════════════════════════════════════════════════════════════════════════════
# $disconnect — Limpieza al desconectar
# ═══════════════════════════════════════════════════════════════════════════════
def disconnect(event, context):
    """Limpieza al desconectar.
    Si el conductor pierde su última conexión activa, se marca activo=false
    en DynamoDB para que no reciba solicitudes de viaje.
    Esto cubre el caso donde el usuario elimina la app desde recientes."""
    conn_id = event['requestContext']['connectionId']
    tabla = dynamodb.Table(CONEXIONES_TABLE)

    # Obtener info de la conexión antes de eliminarla
    response = tabla.get_item(Key={'connectionId': conn_id})
    item = response.get('Item')

    # Eliminar la conexión
    tabla.delete_item(Key={'connectionId': conn_id})

    # Si era un conductor, verificar si quedaron otras conexiones activas
    if item and item.get('rol') == 'CONDUCTOR':
        driver_id = item['userId']
        remaining = tabla.query(
            IndexName='UserIdIndex',
            KeyConditionExpression=Key('userId').eq(driver_id),
        )
        if not remaining.get('Items'):
            # Última conexión del conductor → desactivar
            dynamodb.Table(CONDUCTORES_TABLE).update_item(
                Key={'driverId': driver_id},
                UpdateExpression='SET activo = :val',
                ExpressionAttributeValues={':val': False},
            )

    return {'statusCode': 200, 'body': 'Desconectado'}


# ═══════════════════════════════════════════════════════════════════════════════
# $default — Ruta por defecto para mensajes no reconocidos
# ═══════════════════════════════════════════════════════════════════════════════
def default_handler(event, context):
    """Maneja mensajes WebSocket con action desconocida."""
    conn_id = event['requestContext']['connectionId']
    apigw = _get_apigw_client(event)
    _send_to_connection(apigw, conn_id, {
        'action': 'error',
        'message': 'Acción no reconocida. Acciones válidas: servicioRequerido, '
                   'aceptarServicio, cancelarServicio, iniciarViaje, completarViaje, '
                   'registrarUbicacionMoto, informar, ping',
    })
    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# servicioRequerido — Pasajero solicita un viaje
# ═══════════════════════════════════════════════════════════════════════════════
def servicio_requerido(event, context):
    """El pasajero solicita un mototaxi.
    Crea el servicio en DynamoDB y notifica a todos los conductores activos.

    Body esperado:
    {
      "action": "servicioRequerido",
      "origen": {"lat": -11.4198, "lng": -75.6896, "direccion": "Plaza de Armas"},
      "destino": {"lat": -11.4150, "lng": -75.6820, "direccion": "Terminal Terrestre"},
      "precioSugerido": 1.00,
      "comentario": "Tengo una maleta",
      "cantidad": 1
    }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'USUARIO':
        return _ws_error('Solo usuarios pueden solicitar servicios', 403)

    body = _parse_body(event)

    origen_crudo = body.get('origen')
    destino_crudo = body.get('destino')


    if not origen_crudo or not destino_crudo:
        return _ws_error('Se requiere origen y destino')

    origen = _coordenadas_a_decimal(origen_crudo)
    destino = _coordenadas_a_decimal(destino_crudo)

    ahora = datetime.now(ZONA_PERU).isoformat()
    service_id = str(uuid.uuid4())

    tabla = dynamodb.Table(SERVICIOS_TABLE)

    # Verificar que el usuario no tenga un servicio activo (anti-duplicidad)
    active = tabla.query(
        IndexName='UsuarioEstadoIndex',
        KeyConditionExpression=(
            Key('usuarioId').eq(claims['sub']) &
            Key('estado').eq('PENDIENTE')
        ),
        Select='COUNT',
    )
    if active['Count'] > 0:
        return _ws_error('Ya tienes un servicio pendiente. Cancela o espera a que sea aceptado.')

    active_curso = tabla.query(
        IndexName='UsuarioEstadoIndex',
        KeyConditionExpression=(
            Key('usuarioId').eq(claims['sub']) &
            Key('estado').eq('EN_CURSO')
        ),
        Select='COUNT',
    )
    if active_curso['Count'] > 0:
        return _ws_error('Ya tienes un viaje en curso.')

    item = {
        'serviceId': service_id,
        'usuarioId': claims['sub'],
        'nombreUsuario': claims.get('nombre', ''),
        'driverId': 'NONE',  # Aún no asignado
        'estado': 'PENDIENTE',
        'origen': origen,
        'destino': destino,
        'precioSugerido': Decimal(str(body.get('precioSugerido', 0))),
        'precioFinal': Decimal('0'),
        'comentario': body.get('comentario', ''),
        'cantidad': body.get('cantidad', 1),
        'creadoEn': ahora,
        'actualizadoEn': ahora,
    }

    # Escritura condicional: no duplicar si ya existe (idempotencia)
    try:
        tabla.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(serviceId)',
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('Error de duplicidad. Intente nuevamente.')

    # Notificar a todos los conductores activos
    apigw = _get_apigw_client(event)
    _broadcast_to_active_drivers(apigw, {
        'action': 'nuevoServicio',
        'serviceId': service_id,
        'origen': origen,
        'destino': destino,
        'precioSugerido': float(item['precioSugerido']),
        'nombreUsuario': claims.get('nombre', ''),
        'usuarioId': claims.get('sub', ''),
        'comentario': body.get('comentario', ''),
        'cantidad': body.get('cantidad', 1),
        'creadoEn': ahora,
    })

    # Confirmar al pasajero
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'servicioCreado',
        'serviceId': service_id,
        'estado': 'PENDIENTE',
        'message': 'Buscando conductor disponible en Tarma...',
    })

    return _ws_ok()

# ═══════════════════════════════════════════════════════════════════════════════
# enviarOfertaConductor — Conductor envía una oferta para aceptar el servicio
# ═══════════════════════════════════════════════════════════════════════════════
def enviar_oferta_conductor(event, context):
    """El conductor hce una solicutd de aceptar el servicio del pasajero.

    Body esperado:
    {
      "action": "enviarOfertaConductor",
      "conductorId": "uuid-del-conductor",
      "usuarioId": "uuid-del-usuario",
      "serviceId": "uuid-del-servicio",
      "ubicaciónConductor": {"lat": -11.4198, "lng": -75.6896},
      "ubicaciónPasajero": {"lat": -11.4198, "lng": -75.6896},
      "precioOfrecido": 2.00,
      "nombreConductor": "Juan Pérez"
    }
    """
    claims = _get_claims(event)
    if claims['rol'] != 'CONDUCTOR':
        print(f"Intento de solicitudServicioRequerido por rol {claims['rol']}")
        return _ws_error('Solo conductores pueden solicitar aceptar servicios', 403)
    body = _parse_body(event)

    tablaServicios= dynamodb.Table(SERVICIOS_TABLE)
    
    # Verificar que el servicio exista y esté PENDIENTE
    result = tablaServicios.get_item(Key={'serviceId': body.get('serviceId', '')})

    if not result.get('Item'):
        return _ws_error('Servicio no encontrado o ya no está disponible', 404)
    if result['Item']['estado'] != 'PENDIENTE':
        return _ws_error('Servicio no está disponible para aceptar', 400)

    informacionDistancia = _obtener_distancia_tiempo(
        body.get('ubicaciónConductor', {}).get('lat'),
        body.get('ubicaciónConductor', {}).get('lng'),
        body.get('ubicaciónPasajero', {}).get('lat'),
        body.get('ubicaciónPasajero', {}).get('lng'),
    )

    if informacionDistancia is None:
        print("No se pudo obtener distancia y tiempo desde Google Maps. Usando valores por defecto.")
        informacionDistancia = {
            'distancia_texto': 'Desconocida',
            'distancia_metros': 0,
            'tiempo_texto': 'Desconocido',
            'tiempo_segundos': 0,
        }
    
    #Notificar al usuario
    apigw = _get_apigw_client(event)
    _notify_user(apigw, body.get('usuarioId', ''), {
        'action': 'nuevaOfertaConductor',
        'serviceId': body.get('serviceId', ''),
        'conductorId': body.get('conductorId', ''),
        'ubicaciónConductor': body.get('ubicaciónConductor', {}),
        'distancia': informacionDistancia.get('distancia_texto', "0"),
        'tiempoLlegada': informacionDistancia.get('tiempo_texto', "0"),
        'precioOfrecido': body.get('precioOfrecido', 0),
        'nombreConductor': body.get('nombreConductor', ''),
    })

    #Notificamos al conductor que su solicitud fue enviada
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'ofertaEnviadaConfirmacion',
        'serviceId': body.get('serviceId', ''),
        'estado': 'PENDIENTE',
        'message': 'Esperando la respuesta del pasajero...',
    })
    
    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# aceptarOferta — Pasajero acepta la oferta de un conductor
# ═══════════════════════════════════════════════════════════════════════════════
def aceptar_oferta(event, context):
    """El pasajero acepta el servicio de un conductor específico. Se asigna el conductor al servicio y se notifica a ambas partes.

    Body esperado:
    {
      "action": "aceptarOferta",
      "usuariosId": "uuid-del-usuario",
      "conductorId": "uuid-del-conductor",
      "serviceId": "uuid-del-servicio",
      "precioOfrecido": 5.00
    }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'USUARIO':
        return _ws_error('Solo usuarios pueden aceptar servicios', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    if not service_id:
        return _ws_error('serviceId es requerido')

    ahora = datetime.now(ZONA_PERU).isoformat()
    tabla = dynamodb.Table(SERVICIOS_TABLE)
    tabla_cond = dynamodb.Table(CONDUCTORES_TABLE)

    # Obtener datos del conductor
    driver_res = tabla_cond.get_item(
        Key={'driverId': body.get('conductorId', '')},
        ProjectionExpression='driverId, nombre, apellido, telefono, placa, marca, color, fotoUrl, '
                             'sumaCalificaciones, totalCalificaciones',
    )
    driver_data = driver_res.get('Item', {})

    # Asignación atómica: solo funciona si estado=PENDIENTE y driverId=NONE
    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression=(
                'SET driverId = :d, estado = :e, nombreConductor = :nc, '
                'telefonoConductor = :tc, placaConductor = :pl, '
                'precioFinal = :pf, aceptadoEn = :t, actualizadoEn = :t'
            ),
            ConditionExpression='estado = :pendiente AND driverId = :none',
            ExpressionAttributeValues={
                ':d': body.get('conductorId', ''),
                ':e': 'EN_CAMINO',
                ':pendiente': 'PENDIENTE',
                ':none': 'NONE',
                ':nc': driver_data.get('nombre', '') + ' ' + driver_data.get('apellido', ''),
                ':tc': driver_data.get('telefono', ''),
                ':pl': driver_data.get('placa', ''),
                ':pf': Decimal(str(body.get('precioOfrecido', 0))),
                ':t': ahora,
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        conn_id = event['requestContext']['connectionId']
        apigw = _get_apigw_client(event)
        _send_to_connection(apigw, conn_id, {
            'action': 'servicioNoDisponible',
            'serviceId': service_id,
            'message': 'Este servicio ya fue tomado por otro conductor.',
        })
        return _ws_ok()

    # Obtener servicio actualizado para notificar
    serv_result = tabla.get_item(Key={'serviceId': service_id})
    serv = serv_result.get('Item', {})

    apigw = _get_apigw_client(event)

    # Calcular calificación promedio del conductor
    total_cal = int(driver_data.get('totalCalificaciones', 0))
    prom = round(float(driver_data.get('sumaCalificaciones', 0)) / total_cal, 2) if total_cal > 0 else 5.0

    # Notificar al conductor
    print("hasta aqui todo bien: "+ str(serv.get('driverId', '')))
    _notify_driver(apigw, serv.get('driverId', ''), {
        'action': 'ofertaAceptadaPasajero',
        'serviceId': service_id,
        'estado': 'EN_CAMINO',
        'origen': serv.get('origen', {}),
        'destino': serv.get('destino', {}),
        'nombreUsuario': serv.get('nombreUsuario', ''),
        'precioFinal': float(serv.get('precioFinal', 0)),
        'message': 'Servicio aceptado. Ve al punto de recojo.'
    })
    
    # Confirmar al usuario
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'ofertaAceptadaConfirmacion',
        'serviceId': service_id,
        'estado': 'EN_CAMINO',
        'conductor': {
            'driverId': body.get('conductorId', ''),
            'nombre': driver_data.get('nombre', ''),
            'apellido': driver_data.get('apellido', ''),
            'telefono': driver_data.get('telefono', ''),
            'placa': driver_data.get('placa', ''),
            'marca': driver_data.get('marca', ''),
            'color': driver_data.get('color', ''),
            'fotoUrl': driver_data.get('fotoUrl', ''),
            'calificacion': prom,
        },
        'precioFinal': float(serv.get('precioFinal', 0)),
        'message': '¡Tu conductor está en camino!'
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# conductorEsperando — Conductor llegó al punto de recojo, esperando al pasajero
# ═══════════════════════════════════════════════════════════════════════════════
def conductor_esperando(event, context):
    """El pasajero acepta el servicio de un conductor específico. Se asigna el conductor al servicio y se notifica a ambas partes.

    Body esperado:
    {
      "action": "conductorEsperando",
      "usuariosId": "uuid-del-usuario",
      "conductorId": "uuid-del-conductor",
      "ubicacionConductor": {"lat": -11.4198, "lng": -75.6896},
      "serviceId": "uuid-del-servicio",
    }
    """
    print("Conductor esperando - evento recibido: ", event)
    claims = _get_claims(event)
    print("Claims obtenidas: ", claims)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'CONDUCTOR':
        return _ws_error('Solo conductores pueden aceptar servicios', 403)
    
    print("Saliendo de validacion")

    body = _parse_body(event)
    ubicacionConductorCruda= body.get('ubicacionConductor')
    print(ubicacionConductorCruda)
    
    if not ubicacionConductorCruda:
        return _ws_error('ubicacionConductor es requerida')
    
    ubicacionConductor=_coordenadas_a_decimal(ubicacionConductorCruda)

    try:
        print("Intentado actualizar servicio")
        tabla = dynamodb.Table(SERVICIOS_TABLE)
        tabla.update_item(
            Key={'serviceId': body.get('serviceId', '')},
            UpdateExpression='SET estado = :e, actualizadoEn = :t, ubicacionConductor = :ub',
            ConditionExpression='estado = :enc AND driverId = :d',
            ExpressionAttributeValues={
                ':e': 'ESPERANDO',
                ':enc': 'EN_CAMINO',
                ':d': claims['sub'],
                ':t': datetime.now(ZONA_PERU).isoformat(),
                ':ub': ubicacionConductor,
            }
        )
    except Exception as e:
        print("Error al actualizar el servicio a ESPERANDO: ", str(e))
        return _ws_error('Error al actualizar el servicio', 500)

    print("Servicio actualizado a ESPERANDO, notificando al usuario...")
    #notificamos al usuario que el conductor ya está esperando en el punto de recojo
    apigw = _get_apigw_client(event)
    _notify_user(apigw, body.get('usuarioId', ''), {
        'action': 'conductorEsperandoConfirmacion',
        'serviceId': body.get('serviceId', ''),
        'estado': 'ESPERANDO',
        'ubicacionConductor': body.get('ubicacionConductor', {}),
        'message': 'Tu conductor ya está esperando en el punto de recojo. Por favor, confirma que estás listo para iniciar el viaje.',
    })

    #confirmamos al conductor que le eviamos el websocket al pasajero
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'conductorEsperandoConfirmacion',
        'serviceId': body.get('serviceId', ''),
        'estado': 'ESPERANDO',
        'ubicacionConductor': body.get('ubicacionConductor', {}),
        'message': 'Tu pasajero ya fue informado. Por favor, espere a que llegue.',
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# cancelarViajeConductor — Conductor cancela el viaje antes de iniciar (pasajero no subió)
# ═══════════════════════════════════════════════════════════════════════════════
def cancelar_viaje_conductor(event, context):
    """El conductor cancela el viaje antes de iniciar (pasajero no subió). Estado → CANCELADO.

    Body esperado:
    {
      "action": "cancelarViajeConductor",
      "serviceId": "uuid-del-servicio",
      "motivo": "Pasajero no apareció"
    }
    """

    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'CONDUCTOR':
        return _ws_error('Solo conductores pueden cancelar viajes', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    motivo = body.get('motivo', 'Sin motivo especificado')
    if not service_id:
        return _ws_error('serviceId es requerido')

    ahora = datetime.now(ZONA_PERU).isoformat()
    tabla = dynamodb.Table(SERVICIOS_TABLE)

    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression='SET estado = :e, canceladoEn = :t, motivoCancelacion = :m, actualizadoEn = :t, canceladoPor = :c',
            ConditionExpression='estado IN (:encamino) AND driverId = :d',
            ExpressionAttributeValues={
                ':e': 'CANCELADO',
                ':encamino': 'EN_CAMINO',
                ':d': claims['sub'],
                ':m': motivo,
                ':t': ahora,
                ':c': 'CONDUCTOR',
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('No se puede cancelar el viaje. Verifica el estado del servicio.')

    # Obtener servicio para notificar al pasajero
    serv = tabla.get_item(Key={'serviceId': service_id}).get('Item', {})
    apigw = _get_apigw_client(event)

    _notify_user(apigw, serv.get('usuarioId', ''), {
        'action': 'viajeCanceladoConductor',
        'serviceId': service_id,
        'estado': 'CANCELADO',
        'motivo': motivo,
        'message': f'El conductor ha cancelado el viaje. Motivo: {motivo}',
    })

    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'viajeCanceladoConductorConfirmacion',
        'serviceId': service_id,
        'estado': 'CANCELADO',
        'motivo': motivo,
        'message': f'Viaje cancelado. Motivo: {motivo}',
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# obtenerUbicacionRecojo - Usuario obtiene la ubicación del conductor para el recojo
# ═══════════════════════════════════════════════════════════════════════════════
def obtener_ubicacion_recojo(event, context):
    """El usuario obtiene la ubicación del conductor para el recojo.

    Body esperado:
    { "action": "obtenerUbicacionRecojo", "serviceId": "uuid" }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'USUARIO':
        return _ws_error('Solo usuarios pueden obtener la ubicación del conductor', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    if not service_id:
        return _ws_error('serviceId es requerido')

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    serv = tabla.get_item(Key={'serviceId': service_id}).get('Item', {})

    if not serv:
        return _ws_error('Servicio no encontrado')

    conn_id = event['requestContext']['connectionId']
    apigw = _get_apigw_client(event)

    _send_to_connection(apigw, conn_id, {
        'action': 'ubicacionRecojo',
        'serviceId': service_id,
        'ubicacionConductor': serv.get('ubicacionConductor', {}),
        'message': 'Ubicación del conductor obtenida.',
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# iniciarViaje — Conductor recogió al pasajero, inicia el viaje
# ═══════════════════════════════════════════════════════════════════════════════
def iniciar_viaje(event, context):
    """El conductor confirma que recogió al pasajero. Estado → EN_CURSO.

    Body esperado:
    { "action": "iniciarViaje", "serviceId": "uuid" }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'CONDUCTOR':
        return _ws_error('Solo conductores pueden iniciar viajes', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    if not service_id:
        return _ws_error('serviceId es requerido')

    ahora = datetime.now(ZONA_PERU).isoformat()
    tabla = dynamodb.Table(SERVICIOS_TABLE)

    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression='SET estado = :e, iniciadoEn = :t, actualizadoEn = :t',
            ConditionExpression='estado = :enc AND driverId = :d',
            ExpressionAttributeValues={
                ':e': 'EN_CURSO',
                ':enc': 'EN_CAMINO',
                ':d': claims['sub'],
                ':t': ahora,
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('No se puede iniciar el viaje. Verifica el estado del servicio.')

    # Obtener servicio para notificar al pasajero
    serv = tabla.get_item(Key={'serviceId': service_id}).get('Item', {})
    apigw = _get_apigw_client(event)

    _notify_user(apigw, serv.get('usuarioId', ''), {
        'action': 'viajeIniciado',
        'serviceId': service_id,
        'estado': 'EN_CURSO',
        'message': '¡Viaje iniciado! Ya estás en camino a tu destino.',
    })

    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'viajeIniciadoConfirmacion',
        'serviceId': service_id,
        'estado': 'EN_CURSO',
        'message': 'Viaje iniciado. Lleva al pasajero a su destino.',
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# completarViaje — Conductor completó el viaje
# ═══════════════════════════════════════════════════════════════════════════════
def completar_viaje(event, context):
    """El conductor marca el viaje como completado. Estado → COMPLETADO.

    Body esperado:
    { "action": "completarViaje", "serviceId": "uuid" }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'CONDUCTOR':
        return _ws_error('Solo conductores pueden completar viajes', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    if not service_id:
        return _ws_error('serviceId es requerido')

    ahora = datetime.now(ZONA_PERU).isoformat()
    tabla = dynamodb.Table(SERVICIOS_TABLE)

    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression='SET estado = :e, completadoEn = :t, actualizadoEn = :t',
            ConditionExpression='estado = :ec AND driverId = :d',
            ExpressionAttributeValues={
                ':e': 'COMPLETADO',
                ':ec': 'EN_CURSO',
                ':d': claims['sub'],
                ':t': ahora,
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('No se puede completar el viaje. Verifica el estado del servicio.')

    serv = tabla.get_item(Key={'serviceId': service_id}).get('Item', {})
    apigw = _get_apigw_client(event)

    _notify_user(apigw, serv.get('usuarioId', ''), {
        'action': 'viajeCompletado',
        'serviceId': service_id,
        'estado': 'COMPLETADO',
        'precioFinal': float(serv.get('precioFinal', 0)),
        'message': '¡Viaje completado! Gracias por usar MotoRamos. '
                   'Por favor califica al conductor.',
    })

    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'viajeCompletadoConfirmacion',
        'serviceId': service_id,
        'estado': 'COMPLETADO',
        'precioFinal': float(serv.get('precioFinal', 0)),
        'message': 'Viaje completado exitosamente. Puedes calificar al pasajero.',
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# cancelarServicio — Cancelación por pasajero o conductor
# ═══════════════════════════════════════════════════════════════════════════════
def cancelar_servicio(event, context):
    """Permite cancelar un servicio PENDIENTE o EN_CAMINO.
    No se puede cancelar un viaje EN_CURSO (ya recogió al pasajero).

    Body esperado:
    {
      "action": "cancelarServicio",
      "serviceId": "uuid",
      "motivo": "Conductor tardó mucho"
    }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    if not service_id:
        return _ws_error('serviceId es requerido')

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    result = tabla.get_item(Key={'serviceId': service_id})
    if 'Item' not in result:
        return _ws_error('Servicio no encontrado', 404)

    serv = result['Item']
    estado = serv.get('estado')

    # Verificar que quien cancela sea el usuario o el conductor del servicio
    es_usuario = serv.get('usuarioId') == claims['sub']
    es_conductor = serv.get('driverId') == claims['sub']
    if not es_usuario and not es_conductor:
        return _ws_error('No autorizado para cancelar este servicio', 403)

    # Solo cancelar si PENDIENTE o EN_CAMINO
    if estado not in ('PENDIENTE', 'EN_CAMINO'):
        return _ws_error(f'No se puede cancelar un servicio en estado {estado}')

    ahora = datetime.now(ZONA_PERU).isoformat()
    cancelado_por = 'USUARIO' if es_usuario else 'CONDUCTOR'
    motivo = body.get('motivo', 'Sin motivo especificado')

    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression=(
                'SET estado = :e, canceladoEn = :t, canceladoPor = :cp, '
                'motivoCancelacion = :m, actualizadoEn = :t'
            ),
            ConditionExpression='estado IN (:p, :enc)',
            ExpressionAttributeValues={
                ':e': 'CANCELADO',
                ':t': ahora,
                ':cp': cancelado_por,
                ':m': motivo,
                ':p': 'PENDIENTE',
                ':enc': 'EN_CAMINO',
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('El servicio ya cambió de estado. No se pudo cancelar.')

    apigw = _get_apigw_client(event)

    # Notificar a ambas partes
    cancel_payload = {
        'action': 'servicioCancelado',
        'serviceId': service_id,
        'estado': 'CANCELADO',
        'canceladoPor': cancelado_por,
        'motivo': motivo,
        'message': f'Servicio cancelado por el {"pasajero" if es_usuario else "conductor"}.',
    }

    if es_usuario and serv.get('driverId', 'NONE') != 'NONE':
        _notify_driver(apigw, serv['driverId'], cancel_payload)
    elif es_conductor:
        _notify_user(apigw, serv.get('usuarioId', ''), cancel_payload)

    # Confirmar al que canceló
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, cancel_payload)

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# registrarUbicacionMoto — Conductor envía su GPS en tiempo real
# ═══════════════════════════════════════════════════════════════════════════════
def registrar_ubicacion_moto(event, context):
    """El conductor envía su ubicación GPS actual.
    Vital para seguimiento en las calles empinadas y zonas rurales de Tarma.

    Body esperado:
    {
      "action": "registrarUbicacionMoto",
      "serviceId": "uuid",
      "lat": -11.4198,
      "lng": -75.6896
    }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)
    if claims['rol'] != 'CONDUCTOR':
        return _ws_error('Solo conductores pueden enviar ubicación', 403)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    lat = body.get('lat')
    lng = body.get('lng')

    if service_id is None or lat is None or lng is None:
        return _ws_error('serviceId, lat y lng son requeridos')

    ahora = datetime.now(ZONA_PERU).isoformat()
    tabla = dynamodb.Table(SERVICIOS_TABLE)

    # Actualizar ubicación del conductor en el servicio activo
    try:
        tabla.update_item(
            Key={'serviceId': service_id},
            UpdateExpression=(
                'SET ubicacionConductor = :ub, actualizadoEn = :t'
            ),
            ConditionExpression='driverId = :d AND estado IN (:enc, :ec)',
            ExpressionAttributeValues={
                ':ub': {'lat': Decimal(str(lat)), 'lng': Decimal(str(lng))},
                ':d': claims['sub'],
                ':enc': 'EN_CAMINO',
                ':ec': 'EN_CURSO',
                ':t': ahora,
            },
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _ws_error('No se puede actualizar ubicación. Verifica el servicio.')

    # Obtener servicio para enviar ubicación al pasajero
    serv = tabla.get_item(Key={'serviceId': service_id}).get('Item', {})
    apigw = _get_apigw_client(event)

    _notify_user(apigw, serv.get('usuarioId', ''), {
        'action': 'ubicacionConductor',
        'serviceId': service_id,
        'lat': float(lat),
        'lng': float(lng),
        'timestamp': ahora,
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# informar — Mensajes genéricos entre pasajero y conductor
# ═══════════════════════════════════════════════════════════════════════════════
def informar(event, context):
    """Envía un mensaje de texto entre pasajero y conductor durante un servicio activo.
    Útil para indicaciones como "Estoy en la puerta azul" o "Espérame 1 minuto".

    Body esperado:
    {
      "action": "informar",
      "serviceId": "uuid",
      "mensaje": "Estoy en la esquina de la iglesia"
    }
    """
    claims = _get_claims(event)
    if not claims:
        return _ws_error('No autenticado', 401)

    body = _parse_body(event)
    service_id = body.get('serviceId')
    mensaje = body.get('mensaje', '')

    if not service_id or not mensaje:
        return _ws_error('serviceId y mensaje son requeridos')

    tabla = dynamodb.Table(SERVICIOS_TABLE)
    result = tabla.get_item(Key={'serviceId': service_id})
    if 'Item' not in result:
        return _ws_error('Servicio no encontrado', 404)

    serv = result['Item']
    estado = serv.get('estado')
    if estado not in ('PENDIENTE', 'EN_CAMINO', 'EN_CURSO'):
        return _ws_error('El servicio ya no está activo')

    es_usuario = serv.get('usuarioId') == claims['sub']
    es_conductor = serv.get('driverId') == claims['sub']
    if not es_usuario and not es_conductor:
        return _ws_error('No perteneces a este servicio', 403)

    apigw = _get_apigw_client(event)
    ahora = datetime.now(ZONA_PERU).isoformat()

    msg_payload = {
        'action': 'mensajeRecibido',
        'serviceId': service_id,
        'de': claims.get('nombre', 'Desconocido'),
        'rol': claims['rol'],
        'mensaje': mensaje,
        'timestamp': ahora,
    }

    # Enviar al otro participante
    if es_usuario and serv.get('driverId', 'NONE') != 'NONE':
        _notify_driver(apigw, serv['driverId'], msg_payload)
    elif es_conductor:
        _notify_user(apigw, serv.get('usuarioId', ''), msg_payload)

    # Confirmar al remitente
    conn_id = event['requestContext']['connectionId']
    _send_to_connection(apigw, conn_id, {
        'action': 'mensajeEnviado',
        'serviceId': service_id,
        'mensaje': mensaje,
        'timestamp': ahora,
    })

    return _ws_ok()


# ═══════════════════════════════════════════════════════════════════════════════
# ping — Heartbeat para mantener la conexión viva
# ═══════════════════════════════════════════════════════════════════════════════
def ping(event, context):
    """Responde con pong. El frontend debe enviar ping cada 5 minutos
    para mantener la conexión WebSocket activa (especialmente importante
    en zonas con conectividad intermitente como las alturas de Tarma)."""
    conn_id = event['requestContext']['connectionId']
    apigw = _get_apigw_client(event)

    # Renovar TTL de la conexión
    tabla = dynamodb.Table(CONEXIONES_TABLE)
    tabla.update_item(
        Key={'connectionId': conn_id},
        UpdateExpression='SET #ttl = :t',
        ExpressionAttributeNames={'#ttl': 'ttl'},
        ExpressionAttributeValues={':t': int(time.time()) + 86400},
    )

    _send_to_connection(apigw, conn_id, {
        'action': 'pong',
        'timestamp': datetime.now(ZONA_PERU).isoformat(),
        'message': 'Conexión activa',
    })

    return _ws_ok()
