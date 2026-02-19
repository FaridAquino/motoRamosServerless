import json
import os
import uuid
import boto3
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

CONNECTIONS_TABLE = os.environ['connectionsTable']
MOTOS_TABLE= os.environ["motosTable"]
CONDUCTORES_TABLE= os.environ["conductoresTable"]
USUARIOS_TABLE= os.environ["usuariosTable"]
SERVICIOS_TABLE=os.environ["serviciosTable"]
UBICACIONES_MOTOS_TABLE= os.environ["ubicacionesMotosTable"]

def transmitir(event, message_payload_dict):
    if not CONNECTIONS_TABLE:
        print("[Error] Variables de entorno de tablas no definidas.")
        return
    connections_Table= boto3.resource('dynamodb').Table(CONNECTIONS_TABLE)

    try:
        domain = event["requestContext"]["domainName"]
        stage = event["requestContext"]["stage"]
        endpoint_url = f"https://{domain}/{stage}"
        apigateway_management = boto3.client(
            "apigatewaymanagementapi", endpoint_url=endpoint_url
        )
    except KeyError:
        print("Error: No se pudo obtener el dominio del evento WebSocket.")
        return

    action = message_payload_dict.get('action')
    destinatarios = []

    if action == 'ubicacionMoto':
        print("Acción detectada: Enviar ubicación a USUARIOS")
        
        try:
            response = connections_Table.query(
                IndexName='RolIndex',  # Usamos el índice que creaste en el YAML
                KeyConditionExpression=Key('rol').eq('USUARIO') # Solo traemos USUARIOS
            )
            destinatarios = response.get('Items', [])
            print(f"Se encontraron {len(destinatarios)} usuarios conectados.")
            
        except ClientError as e:
            print(f"Error consultando DynamoDB Index: {e}")
            return

    elif action == 'servicioRequerido':
        print("Acción detectada: Enviar solicitud de servicio a CONDUCTORES")
        
        try:
            response = connections_Table.query(
                IndexName='RolIndex',  # Usamos el índice que creaste en el YAML
                KeyConditionExpression=Key('rol').eq('CONDUCTOR') # Solo traemos CONDUCTORES
            )
            destinatarios = response.get('Items', [])
            print(f"Se encontraron {len(destinatarios)} conductores conectados.")
            
        except ClientError as e:
            print(f"Error consultando DynamoDB Index: {e}")
            return
    
    elif action == 'servicioAceptado':
        print("Acción detectada: Notificar a USUARIO sobre servicio aceptado")
        try:
            response = connections_Table.query(
                IndexName='RolIndex',  # Usamos el índice que creaste en el YAML
                KeyConditionExpression=Key('rol').eq('USUARIO') # Solo traemos USUARIOS
            )
            destinatarios = response.get('Items', [])
            print(f"Se encontraron {len(destinatarios)} usuarios conectados.")
            
        except ClientError as e:
            print(f"Error consultando DynamoDB Index: {e}")
            return

    elif action == 'aceptarServicio':
        target_user_id = message_payload_dict.get('usuarioId') 
        
        if target_user_id:
            print(f"Buscando conexión para el usuario: {target_user_id}")
            try:
                # Usamos el NUEVO ÍNDICE para buscar la conexión de ese usuario
                response = connections_Table.query(
                    IndexName='UserIdIndex',
                    KeyConditionExpression=Key('userId').eq(target_user_id)
                )
                destinatarios = response.get('Items', [])
                
                if not destinatarios:
                    print(f"El usuario {target_user_id} no está conectado actualmente.")
            except ClientError as e:
                print(f"Error query UserIdIndex: {e}")
        else:
            print("Error: aceptarServicio requiere 'usuarioIdDestino' en el payload")
    
    elif action == 'servicioCompletado':
        target_user_id = message_payload_dict.get('usuarioId') 
        
        if target_user_id:
            print(f"Buscando conexión para el usuario: {target_user_id}")
            try:
                # Usamos el NUEVO ÍNDICE para buscar la conexión de ese usuario
                response = connections_Table.query(
                    IndexName='UserIdIndex',
                    KeyConditionExpression=Key('userId').eq(target_user_id)
                )
                destinatarios = response.get('Items', [])
                
                if not destinatarios:
                    print(f"El usuario {target_user_id} no está conectado actualmente.")
            except ClientError as e:
                print(f"Error query UserIdIndex: {e}")
        else:
            print("Error: servicioCompletado requiere 'usuarioId' en el payload")

    else:
        print(f"Acción '{action}' no tiene lógica de difusión definida.")
        return

    # 3. Bucle de Envío (Broadcast)
    if not destinatarios:
        print("No hay destinatarios para enviar.")
        return

    message_json = json.dumps(message_payload_dict)

    for user in destinatarios:
        connection_id = user['connectionId']
        
        try:
            apigateway_management.post_to_connection(
                ConnectionId=connection_id,
                Data=message_json
            )
        except ClientError as e:
            # Manejo de usuarios desconectados (limpieza de BD)
            if e.response['Error']['Code'] == 'GoneException':
                print(f"Usuario {connection_id} ya no existe. Borrando...")
                # Para borrar necesitamos la Primary Key (connectionId), no la del Index
                connections_Table.delete_item(Key={'connectionId': connection_id})
            else:
                print(f"Error enviando a {connection_id}: {e}")

    print("Transmisión completada.")


    # ==============================================================================
    # 🔔 LÓGICA SNS (NOTIFICACIONES PUSH)
    # Enviamos a TODOS los usuarios de la tabla de dispositivos
    # ==============================================================================
    # if SNS_TOPIC_ARN and USERS_DEVICES_TABLE:
    #     try:
    #         print("--- [SNS] Iniciando proceso de notificación masiva ---")
    #         users_dev_table = boto3.resource('dynamodb').Table(USERS_DEVICES_TABLE)
            
    #         # A. Escaneamos la tabla para sacar todos los correos (tenant_id)
    #         # NOTA: .scan() es costoso. En producción usa Query o lógica específica.
    #         response_scan = users_dev_table.scan(
    #             ProjectionExpression='tenant_id' 
    #         )
    #         items = response_scan.get('Items', [])
            
    #         # B. Extraemos IDs únicos (usamos set para evitar duplicados si hay paginación sucia)
    #         # Tu Worker espera una lista de "correos" (que son tus tenant_id)
    #         destinatarios = list(set([item['tenant_id'] for item in items]))

    #         if destinatarios:
    #             # C. Construimos el Payload EXACTAMENTE como lo espera tu Lambda Worker
    #             sns_payload = {
    #                 "correos": destinatarios,
    #                 "title": "🚛 Camión en camino",
    #                 "body": f"El recolector está pasando"
    #             }

    #             # D. Publicamos al SNS
    #             sns_client.publish(
    #                 TopicArn=SNS_TOPIC_ARN,
    #                 Message=json.dumps(sns_payload)
    #             )
    #             print(f"[SNS] Mensaje enviado al Topic para {len(destinatarios)} usuarios.")
    #         else:
    #             print("[SNS] No se encontraron usuarios en la tabla para notificar.")

    #     except Exception as e:
    #         # Ponemos try/except para que si falla SNS, NO rompa el WebSocket
    #         print(f"[Error SNS] Fallo al enviar notificación: {e}")
    # else:
    #     print("[SNS] Omitido: Faltan variables SNS_TOPIC_ARN o USERS_DEVICES_TABLE")

    # ==============================================================================
    # 📡 LÓGICA WEBSOCKET (Tu código original sigue aquí)
    # ==============================================================================

def connection_manager(event, context):
    connection_id = event['requestContext']['connectionId']
    route_key = event['requestContext']['routeKey']
    
    query_params = event.get('queryStringParameters', {}) or {}

    if not CONNECTIONS_TABLE:
        print("Error: CONNECTIONS_TABLE no está definida en las variables de entorno.")
        return {'statusCode': 500, 'body': 'Error de configuración del servidor.'}
        
    table = boto3.resource("dynamodb").Table(CONNECTIONS_TABLE)

    if route_key == '$connect':
        try:
            
            item = {
                'connectionId': connection_id,
                'role': query_params.get('role', 'NULL'),
                'userId': query_params.get('userId', 'NULL')
            }

            table.put_item(Item=item)

            print(f"Conexión registrada: {connection_id} con rol {item['role']}")
            
            return {'statusCode': 200, 'body': 'Conectado.'}

        except Exception as e:
            print(f"Error en $connect: {e}")
            return {'statusCode': 500, 'body': 'Fallo en $connect.'}

    elif route_key == '$disconnect':
        try:
            table.delete_item(
                Key={'connectionId': connection_id}
            )
            print(f"Conexión eliminada: {connection_id}")
            
            return {'statusCode': 200, 'body': 'Desconectado.'}
            
        except Exception as e:
            print(f"Error en $disconnect (no crítico): {e}")
            return {'statusCode': 200, 'body': 'Desconectado con error de limpieza.'}

    return {'statusCode': 500, 'body': 'Error en connection_manager.'}

def default_handler(event, context):
    print(f"Ruta $default invocada. Evento: {event}")
    return {
        'statusCode': 404,
        'body': json.dumps("Acción no reconocida.")
    }

def registrarUbicacionMoto(event, context):
    print("Evento recibido en registrarUbicacionMoto")

    zona_peru = ZoneInfo("America/Lima")
    ahora = datetime.now(zona_peru)

    timestamp_str = ahora.isoformat()

    try:
        body = json.loads(event['body'])
    except (TypeError, json.JSONDecodeError):
        print("Cuerpo de la solicitud no es un JSON válido")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'El cuerpo de la solicitud no es un JSON válido'})
        }

    try:
        motoId= body.get("motoId")
    except KeyError:
        print("Falta motoId en el cuerpo de la solicitud")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Falta motoId en el cuerpo de la solicitud'})
        }
    
    motos_Table=boto3.resource('dynamodb').Table(MOTOS_TABLE)
    response= motos_Table.get_item(
        Key={
            'motoId': motoId,
        }
    )

    if 'Item' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Moto no encontrada'})
        }
    try:
        lat_float = float(body["latitud"])
        lon_float = float(body["longitud"])
    except (ValueError, TypeError):
        return {
            'statusCode': 400, 
            'body': json.dumps({'error': 'Latitud y Longitud deben ser números válidos'})
        }

    if not (-90 <= lat_float <= 90):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Latitud inválida (debe estar entre -90 y 90)'})}
        
    if not (-180 <= lon_float <= 180):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Longitud inválida (debe estar entre -180 y 180)'})}

    lat_decimal = Decimal(str(lat_float))
    lon_decimal = Decimal(str(lon_float))

    ubicacion={
        'latitud': float(lat_decimal),
        'longitud': float(lon_decimal)
    }

    transmission_payload = {
            'action': 'ubicacionMoto',
            'placa': body.get("placa"),
            'nombre': body.get("nombre"),
            'ubicacion': ubicacion
    }
    
    ubicacion_Moto = {
        'placa': body.get("placa"),
        'timestamp': timestamp_str,
        'latitud': lat_decimal,   
        'longitud': lon_decimal,
        'nombre': body.get("nombre"),
    }
    
    try:
        ubicaciones_Motos_Table=boto3.resource('dynamodb').Table(UBICACIONES_MOTOS_TABLE)
        ubicaciones_Motos_Table.put_item(Item=ubicacion_Moto)
    except Exception as e:
        print(f"Error al almacenar la ubicación en DynamoDB: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al almacenar la ubicación'})
        }
        
    transmitir(event, transmission_payload)

    print("Ubicación de la moto almacenada y transmitida exitosamente")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Ubicación de la moto registrada exitosamente'})
    }

def registrarServicio(event, context):
    print("Evento recibido en registrarServicioRequerido")
    zona_peru = ZoneInfo("America/Lima")
    ahora = datetime.now(zona_peru)
    
    fecha_actual_str = ahora.strftime("%Y-%m-%d")
    hora_actual_str = ahora.strftime("%H:%M:%S")

    body = json.loads(event['body'])
    
    try:
        usuario_Table = boto3.resource('dynamodb').Table(USUARIOS_TABLE)
        response = usuario_Table.get_item(
            Key={
                'userId': body.get("usuarioId"),
            }
        )
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Usuario no encontrado'})
            }
    except Exception as e:
        print(f"Error al acceder a la tabla de usuarios: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al acceder a la tabla de usuarios'})
        }
        
    #GSI    
    estado = "PENDIENTE"

    nombre_Usuario = response['Item'].get('nombre')
    correo_Usuario = response['Item'].get('correo')

    cantidad = body.get("cantidad")
    monto = body.get("monto")
    nombreDestino = body.get("nombreDestino")

    try:
        lat_float_origen = float(body["latitudOrigen"])
        lon_float_origen = float(body["longitudOrigen"])
        lat_float_destino= float(body["latitudDestino"])
        lon_float_destino= float(body["longitudDestino"])
    except (ValueError, TypeError):
        return {
            'statusCode': 400, 
            'body': json.dumps({'error': 'Latitud y Longitud deben ser números válidos'})
        }

    if not (-90 <= lat_float_origen <= 90 and -90 <= lat_float_destino <= 90):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Latitud inválida'})}
        
    if not (-180 <= lon_float_origen <= 180 and -180 <= lon_float_destino <= 180):
        return {'statusCode': 400, 'body': json.dumps({'error': 'Longitud inválida'})}

    lat_decimal_origen = Decimal(str(lat_float_origen))
    lon_decimal_origen = Decimal(str(lon_float_origen))
    lat_decimal_destino = Decimal(str(lat_float_destino))
    lon_decimal_destino = Decimal(str(lon_float_destino))

    servicio_requerido = {
        'serviceId': str(uuid.uuid4()),
        'estado': estado,

        'fechaPedida': fecha_actual_str,
        'horaPedida': hora_actual_str,
        
        'nombreUsuario': nombre_Usuario,
        'correoUsuario': correo_Usuario,
        'cantidad': cantidad,
        'monto': Decimal(str(float(monto))),
        'latitudOrigen': lat_decimal_origen,
        'longitudOrigen': lon_decimal_origen,
        'latitudDestino': lat_decimal_destino,
        'longitudDestino': lon_decimal_destino,
        'nombreDestino': nombreDestino
    }

    try:
        servicio_Table = boto3.resource('dynamodb').Table(SERVICIOS_TABLE)
        servicio_Table.put_item(Item=servicio_requerido)
        
    except Exception as e:
        print(f"Error al almacenar en DynamoDB: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al almacenar la solicitud'})
        }
    
    transmission_payload = {
        'action': 'servicioRequerido',
        'serviceId': servicio_requerido['serviceId'],
        'usuarioId': body.get("usuarioId"),
        'latitudOrigen': float(lat_decimal_origen),
        'longitudOrigen': float(lon_decimal_origen),
        'latitudDestino': float(lat_decimal_destino),
        'longitudDestino': float(lon_decimal_destino),
        'monto': float(monto),
        'cantidad': cantidad
    }
    
    transmitir(event, transmission_payload)

    print("Ubicación de usuario almacenada y transmitida exitosamente")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Servicio registrado exitosamente'
        })

    }

def aceptarServicio(event, context):
    print("Evento recibido en aceptarServicio")
    body= json.loads(event['body'])

    try:
        motos_Table=boto3.resource('dynamodb').Table(MOTOS_TABLE)
        moto_response= motos_Table.get_item(
            Key={
                'motoId': body.get("motoId"),
            }
        )

        if 'Item' not in moto_response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Moto no encontrada'})
            }
        
        if moto_response['Item'].get('estado') == 'NO_TRABAJANDO':
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'La moto no está trabajando'})
            }
        
    except Exception as e:
        print(f"Error al acceder a la tabla de motos: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al acceder a la tabla de motos'})
        }

    monto_Final= body.get("montoFinal")

    try:
        servicio_Table = boto3.resource('dynamodb').Table(SERVICIOS_TABLE)
        servicio_Table.update_item(
            Key={'serviceId': body.get("serviceId")},
            
            ConditionExpression="attribute_not_exists(placaMoto) AND estado = 'PENDIENTE'",

            UpdateExpression="SET estado = :e, placaMoto = :pM, nombreMoto = :nM, montoFinal = :mF",
            
            ExpressionAttributeValues={
                ':e': 'ATENDIDO',
                ':nM': moto_response['Item'].get('nombre'),
                ':pM': moto_response['Item'].get('placa'),
                ':mF': Decimal(str(float(monto_Final))),

            }
        )
        print("Servicio ATENDIDO actualizado en DynamoDB")

        motos_Table.update_item(
            Key={'motoId': body.get("motoId")},
            UpdateExpression="SET estado = :e",
            ExpressionAttributeValues={
                ':e': 'CONDUCIENDO'
            }
        )

        print("Estado de la moto actualizado a CONDUCIENDO en DynamoDB")

    except servicio_Table.meta.client.exceptions.ConditionalCheckFailedException:
        print("El servicio ya fue aceptado o no está en estado PENDIENTE")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'El servicio ya fue aceptado o no está en estado PENDIENTE'})
        }
    
    transmission_payload = {
        'action': 'servicioAceptado',
        'serviceId': body.get("serviceId"),
        'usuarioId': body.get("usuarioId"),
        'placaMoto': moto_response['Item'].get('placa'),
        'nombreMoto': moto_response['Item'].get('nombre'),
        'montoFinal': float(monto_Final)
    }
    
    transmitir(event, transmission_payload)
    print("Servicio aceptado almacenado y transmitido exitosamente")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Servicio aceptado registrado exitosamente'})
    }

def completarServicio(event, context):
    print("Evento recibido en completarServicio")
    body= json.loads(event['body'])
    zona_peru = ZoneInfo("America/Lima")
    ahora = datetime.now(zona_peru)
    
    fecha_actual_str = ahora.strftime("%Y-%m-%d")
    hora_actual_str = ahora.strftime("%H:%M:%S")

    try:
        servicio_Table = boto3.resource('dynamodb').Table(SERVICIOS_TABLE)
        servicio_Table.update_item(
            Key={'serviceId': body.get("serviceId")},
            UpdateExpression="SET estado = :e, fechaCompletado = :fC, horaCompletado = :hC",
            ExpressionAttributeValues={
                ':e': 'COMPLETADO',
                ':fC': fecha_actual_str,
                ':hC': hora_actual_str
            }
        )
        print("Servicio completado actualizado en DynamoDB")

        motos_Table=boto3.resource('dynamodb').Table(MOTOS_TABLE)

        motos_Table.update_item(
            Key={'motoId': body.get("motoId")},
            UpdateExpression="SET estado = :e",
            ExpressionAttributeValues={
                ':e': 'TRABAJANDO'
            }
        )
        print("Estado de la moto actualizado a TRABAJANDO en DynamoDB")
    
    except Exception as e:
        print(f"Error al actualizar el servicio en DynamoDB: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al actualizar el servicio'})
        }
    
    transmission_payload = {
        'action': 'servicioCompletado',
        'serviceId': body.get("serviceId"),
        'usuarioId': body.get("usuarioId"),
    }
    
    transmitir(event, transmission_payload)
    print("Servicio completado transmitido exitosamente")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Servicio completado registrado exitosamente'})
    }   

