import json
import hmac
import os
import boto3
import hashlib
import uuid
from decimal import Decimal
from boto3.dynamodb.conditions import Key

MOTOS_TABLE= os.environ["motosTable"]
CONDUCTORES_TABLE= os.environ["conductoresTable"]
USUARIOS_TABLE= os.environ["usuariosTable"]
SERVICIOS_TABLE= os.environ["serviciosTable"]
UBICACIONES_MOTOS_TABLE= os.environ["ubicacionesMotosTable"]

def registerConductor(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        nombre = body['nombre']
        edad = body['edad']
        correo = body['correo']
        contrasena = body["contrasena"]

        # Hashing de la contraseña
        salt = os.urandom(16)
        hash_bytes = hashlib.pbkdf2_hmac(
            'sha256', 
            contrasena.encode('utf-8'), 
            salt, 
            600000
        )

        contrasena_hash = salt.hex() + ":" + hash_bytes.hex()

        conductores_Table = boto3.resource('dynamodb').Table(CONDUCTORES_TABLE)
        
        conductor_Json = {
            'driverId': str(uuid.uuid4()),
            'correo': correo,
            'nombre': nombre,
            'edad': Decimal(str(edad)),
            'contrasenaHasheada': contrasena_hash
        }
        
        conductores_Table.put_item(Item=conductor_Json)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Conductor registrado exitosamente'})
        }

    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def loginConductor(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        correo = body['correo']
        contrasena = body["contrasena"]

        conductores_Table = boto3.resource('dynamodb').Table(CONDUCTORES_TABLE)
        
        response = conductores_Table.query(
            IndexName='CorreoIndex',
            KeyConditionExpression=Key('correo').eq(correo)
        )
        
        items = response.get('Items', [])
        
        if len(items) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Conductor no encontrado'})
            }

        conductor_encontrado = items[0]

        contrasena_guardada = conductor_encontrado['contrasenaHasheada']
        
        try:
            salt_hex, hash_real_hex = contrasena_guardada.split(":")
        except ValueError:
            return {'statusCode': 500, 'body': json.dumps({'error': 'Error en datos de usuario'})}

        salt_bytes = bytes.fromhex(salt_hex)

        hash_intento = hashlib.pbkdf2_hmac(
            'sha256',
            contrasena.encode('utf-8'),
            salt_bytes,
            600000
        )

        hash_real_bytes = bytes.fromhex(hash_real_hex)
        
        if hmac.compare_digest(hash_intento, hash_real_bytes):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Login exitoso'})
            }
        else:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Contraseña incorrecta'})
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def registrarMoto(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        driverId = body['driverId']
        placa = body['placa']
        color = body['color']
        empresa= body['empresa']
        correo_Conductor = body['correoConductor']

        motos_Table = boto3.resource('dynamodb').Table(MOTOS_TABLE)
        conductores_table=boto3.resource('dynamodb').Table(CONDUCTORES_TABLE)

        moto_Json = {
            'motoId': str(uuid.uuid4()),
            'estado': 'NO_TRABAJANDO',
            'correoConductor': correo_Conductor,
            'placa': placa,
            'empresa': empresa,
            'color': color
        }
        
        response_update = conductores_table.update_item(
            Key={
                'driverId': driverId
            },
            UpdateExpression="SET motos = list_append(if_not_exists(motos, :empty_list), :moto)",
            ExpressionAttributeValues={
                ':empty_list': [],
                ':moto': [placa]
            },
            ReturnValues="UPDATED_NEW"
        )
        print("Moto agregada a conductor: ", response_update.get('Attributes'))
        
        motos_Table.put_item(Item=moto_Json)

        print("Moto registraad: ", moto_Json)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Moto registrada exitosamente al conductor'})
        }

    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def recorridoIniciado(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        moto_Id= body['motoId']

        motos_Table=boto3.resource('dynamodb').Table(MOTOS_TABLE)
        response= motos_Table.get_item(Key={'motoId': moto_Id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Moto no encontrada'})
            }

        update_response = motos_Table.update_item(
            Key={
                'motoId': moto_Id
            },
            UpdateExpression="set estado = :e",
            ExpressionAttributeValues={
                ':e': 'TRABAJANDO'
            },
            ReturnValues="UPDATED_NEW"
        )

        print("UpdateItem succeeded: ", update_response.get('Attributes'))

        return {
            'statusCode': 200, 
            'body': json.dumps({
                'message': 'Estado actualizado a "TRABAJANDO" correctamente',
                'updatedAttributes': update_response.get('Attributes')
            })
        }
    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def recorridoTerminado(event,context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        moto_Id= body['motoId']

        motos_Table=boto3.resource('dynamodb').Table(MOTOS_TABLE)
        response= motos_Table.get_item(Key={'motoId': moto_Id})

        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Moto no encontrada'})
            }

        update_response = motos_Table.update_item(
            Key={
                'motoId': moto_Id
            },
            UpdateExpression="set estado = :e",
            ExpressionAttributeValues={
                ':e': 'NO_TRABAJANDO'
            },
            ReturnValues="UPDATED_NEW"
        )

        print("UpdateItem succeeded: ", update_response.get('Attributes'))

        return {
            'statusCode': 200, 
            'body': json.dumps({
                'message': 'Estado actualizado a "NO_TRABAJANDO" correctamente',
                'updatedAttributes': update_response.get('Attributes')
            })
        }
    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}