import json
import boto3
import os
import firebase_admin
from firebase_admin import credentials, messaging
from boto3.dynamodb.conditions import Key

from botocore.exceptions import ClientError

if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json") # Asegúrate de subir este archivo con tu lambda
    firebase_admin.initialize_app(cred)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['USERS_DEVICES'])

USERS_DEVICES_TABLE = os.environ['USERS_DEVICES']

def _parse_sns_data(data: dict) -> dict:
    """
    Extrae y normaliza los datos del mensaje SNS.
    Retorna un diccionario limpio donde 'body' está garantizado de ser un dict.
    """
    # Si por alguna razón la data no es un diccionario, retornamos valores por defecto
    if not isinstance(data, dict):
        return {'telefonos': [], 'title': '', 'body': {}}

    telefonos = data.get('telefonos', [])
    titulo = data.get('title', 'Notificación')
    raw_body = data.get('body', {})

    # Normalizar el body para que SIEMPRE sea un diccionario
    if isinstance(raw_body, str):
        try:
            body_dict = json.loads(raw_body)
        except json.JSONDecodeError:
            print("⚠️ Error: El 'body' venía como String pero no es un JSON válido.")
            body_dict = {}
    elif isinstance(raw_body, dict):
        body_dict = raw_body
    else:
        body_dict = {}

    # Retornamos todo empacado y listo para usar
    return {
        'telefonos': telefonos,
        'title': titulo,
        'body': body_dict
    }


def registrarToken(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Falta el body'})
            }

        body = json.loads(event['body'])

        telefono = body.get('telefono')
        device_token = body.get('device_token')
        
        if not telefono or not device_token:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Faltan datos (telefono o device_token)'})
            }

        usersDevicesTable = boto3.resource('dynamodb').Table(USERS_DEVICES_TABLE)
        
        usersDevicesJson = {
            'telefono': telefono,
            'device_token': device_token
        }
        
        try:
            usersDevicesTable.put_item(
                Item=usersDevicesJson,
                ConditionExpression='attribute_not_exists(telefono)'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Token registrado exitosamente'})
            }

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"El token para {telefono} ya existía. No se hizo nada.")
                return {
                    'statusCode': 200, # Retornamos 200 OK porque la solicitud fue procesada correctamente
                    'body': json.dumps({
                        'message': 'El dispositivo ya estaba registrado. No se realizaron cambios.',
                        'status': 'skipped'
                    })
                }
            else:
                raise e

    except Exception as e:
        print(f"Error crítico: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def _enviar_notificaciones_fcm(tokens_to_send, titulo, body_data):
    """
    Envía notificaciones push a una lista de tokens.
    Convierte el body_data (dict) en un string para el payload oculto ('data'),
    y genera un texto visible para la notificación principal.
    """
    enviados = 0
    errores = 0

    precio = body_data.get('precioSugerido', '0.00')
    texto_visible = f"Nuevo viaje por S/{precio}"

    body_string = json.dumps(body_data)

    for token in tokens_to_send:
        try:
            msg = messaging.Message(
                notification=messaging.Notification(
                    title=titulo,
                    body=texto_visible,
                ),
                data={
                    'click_action': 'FLUTTER_NOTIFICATION_CLICK',
                    'payload_viaje': body_string,
                    'type': body_data.get('action', 'general x')
                },
                token=token
            )
            
            messaging.send(msg)
            enviados += 1
            
        except Exception as e:
            print(f"Error enviando a token {token[:10]}...: {e}")
            errores += 1

    print(f"--- Resultado Final FCM: {enviados} enviados, {errores} fallidos ---")
    return enviados, errores

def lambda_handler(event, context):
    print("Evento recibido de SNS:", json.dumps(event))
    print(f"--- [DEBUG] Versión Firebase en Lambda: {firebase_admin.__version__} ---")
    for record in event['Records']:
        try:
            sns_message = json.loads(record['Sns']['Message'])
            notificar(sns_message)
        except Exception as e:
            print(f"Error procesando registro SNS: {e}")
            
    return {'statusCode': 200, 'body': 'Procesado'}

# ═══════════════════════════════════════════════════════════════════════════════
# nuevoServicio — Notifica a conductores un nuevo servicio
# ═══════════════════════════════════════════════════════════════════════════════
def notificar(data):
    """Data esperada: 
    {
        'telefonos': ['123456789'],
        'title': '(viene de SNS)',
        'body': {
            "action": "XXX", # Puedes usar esto para que el cliente sepa qué hacer al recibir la notificación
            "origen": {"lat": -11.4198, "lng": -75.6896, "direccion": "Plaza de Armas"},
            "destino": {"lat": -11.4150, "lng": -75.6820, "direccion": "Terminal Terrestre"},
            "precioSugerido": 1.00,
            "comentario": "Tengo una maleta",
            "cantidad": 1
        }
    }
    """
    print("Notificando nuevo servicio con data:", json.dumps(data))

    parsed_data = _parse_sns_data(data)
    
    target_users = parsed_data['telefonos']
    titulo = parsed_data.get('title', 'Nuevo servicio x')
    body = parsed_data['body']
    
    if not target_users:
        print("No hay usuarios destino.")
        return

    tokens_to_send = []
    
    for telefono in target_users:
        try:
            response = table.query(
                KeyConditionExpression=Key('telefono').eq(telefono)
            )
            items = response.get('Items', [])
            for item in items:
                # Ojo: Asegúrate si en tu BD guardaste como 'uuid' o 'fcm_token'
                # Según tu código anterior era 'uuid' (que usabas para el token)
                if 'device_token' in item: 
                    tokens_to_send.append(item['device_token'])
        except Exception as e:
            print(f"Error consultando DynamoDB para {telefono}: {e}")
            
    if not tokens_to_send:
        print("No se encontraron tokens válidos en BD.")
        return
    
    tokens_to_send = list(set(tokens_to_send))

    _enviar_notificaciones_fcm(tokens_to_send, titulo, body)
