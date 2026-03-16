import boto3
import random
import string
import time
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
otp_table = dynamodb.Table(os.environ.get('otpTable', 'otp_verificacion'))

def generar_codigo():
    return ''.join(random.choices(string.digits, k=6))

def enviar_codigo_sms(telefono: str) -> dict:
    # Formato peruano: +51XXXXXXXXX
    telefono_formateado = f"+51{telefono}" if not telefono.startswith('+') else telefono
    
    codigo = generar_codigo()
    expira_en = int(time.time()) + 300  # 5 minutos

    # Guardar en DynamoDB
    otp_table.put_item(Item={
        'telefono': telefono,
        'codigo': codigo,
        'expira_en': expira_en,
        'verificado': False,
    })

    # Enviar SMS via SNS
    try:
        sns_client.publish(
            PhoneNumber=telefono_formateado,
            Message=f"Tu código de verificación MotoRamos es: {codigo}. Válido por 5 minutos.",
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'  # mayor prioridad de entrega
                },
                'AWS.SNS.SMS.SenderID': {
                    'DataType': 'String',
                    'StringValue': 'MotoRamos'
                }
            }
        )
        return {'success': True}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def verificar_codigo(telefono: str, codigo: str) -> dict:
    try:
        response = otp_table.get_item(Key={'telefono': telefono})
        item = response.get('Item')

        if not item:
            return {'success': False, 'message': 'Código no encontrado o expirado'}

        if int(time.time()) > item['expira_en']:
            return {'success': False, 'message': 'El código ha expirado'}

        if item['codigo'] != codigo:
            return {'success': False, 'message': 'Código incorrecto'}

        # Marcar como verificado y eliminar
        otp_table.delete_item(Key={'telefono': telefono})

        return {'success': True}
    except Exception as e:
        return {'success': False, 'message': str(e)}