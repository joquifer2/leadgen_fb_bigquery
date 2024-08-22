import json
from google.cloud import bigquery
from functions_framework import http
from datetime import datetime

@http
def receive_webhook(request):
    verify_token = "xxxxxxxxxxxxxxxx"  # Cambia esto por el token que configuraste en Facebook

    # Verificación del token cuando Facebook manda el challenge
    if request.method == 'GET':
        if request.args.get('hub.verify_token') == verify_token:
            return request.args.get('hub.challenge')
        else:
            return 'Token de verificación inválido', 403

    # Procesar los datos recibidos en una solicitud POST
    if request.method == 'POST':
        request_json = request.get_json()
        print(f"Datos recibidos: {json.dumps(request_json)}")

        # Extraer los datos relevantes
        value = request_json.get('value', {})
        leadgen_id = value.get('leadgen_id')
        created_time_unix = value.get('created_time')
        field_data = value.get('field_data', [])

        # Inicializar campos
        full_name = ""
        email = ""

        # Mapear los campos
        for field in field_data:
            if field['name'] == 'Full name':
                full_name = field['values'][0]
            elif field['name'] == 'Email':
                email = field['values'][0]

        # Convertir el timestamp UNIX a un formato de TIMESTAMP para BigQuery
        created_time = datetime.utcfromtimestamp(created_time_unix).strftime('%Y-%m-%d %H:%M:%S')

        # Preparar el registro para BigQuery
        row_to_insert = {
            "leadgen_id": leadgen_id,
            "created_time": created_time,
            "full_name": full_name,
            "email": email,
            "ad_id": value.get('ad_id'),
            "form_id": value.get('form_id'),
            "page_id": value.get('page_id')
        }

        # Guardar los datos en BigQuery
        client = bigquery.Client()
        table_id = 'xxxxxxxxxxxxxxxxxxxx'  # Cambia esto por tu ID de tabla en BigQuery

        # Inserta el JSON mapeado en BigQuery
        errors = client.insert_rows_json(table_id, [row_to_insert])  
        if not errors:
            print('Datos insertados en BigQuery')
            return 'Evento recibido y almacenado en BigQuery', 200
        else:
            print(f"Errores al insertar en BigQuery: {errors}")
            return 'Error al almacenar los datos', 500

    return 'Método no permitido', 405