import json
import hmac
import os
import boto3
import hashlib
from decimal import Decimal

USUARIOS_TABLE=os.environ['usuariosTable']

def registerUsuario(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        nombre = body['nombre']
        apellido = body['apellido']
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

        usuarios_Table = boto3.resource('dynamodb').Table(USUARIOS_TABLE)
        
        usuario_Json = {
            'tenant_id': nombre,
            'apellido': apellido,
            'uuid': correo,
            'edad': Decimal(str(edad)),
            'contrasenaHasheada': contrasena_hash
        }
        
        usuarios_Table.put_item(Item=usuario_Json)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Usuario registrado exitosamente'})
        }

    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps({'error': f'Falta el campo: {str(e)}'})}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def loginUsuario(event, context):
    try:
        if 'body' not in event or event['body'] is None:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Falta el body'})}

        body = json.loads(event['body'])

        nombre = body['nombre']
        correo = body['correo']
        contrasena = body["contrasena"]


        usuarios_Table = boto3.resource('dynamodb').Table(USUARIOS_TABLE)
        
        response = usuarios_Table.get_item(Key={'tenant_id': nombre, 'uuid': correo})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Usuario no encontrado'})
            }

        contrasena_guardada = response['Item']['contrasenaHasheada']
        
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
