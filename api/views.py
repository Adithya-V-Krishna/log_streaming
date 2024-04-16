from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import boto3
import json

@csrf_exempt  # This decorator is used to exempt CSRF protection for this view
def send_data_to_firehose(request):
    if request.method == 'POST':
        try:
            # Extract JSON data from the request body
            json_data = json.loads(request.body)
            
            # Initialize the AWS Firehose client
            client = boto3.client('firehose', region_name='ap-south-1')
            
            # Send data to Firehose
            response = client.put_record(
                DeliveryStreamName='Social_media_log',
                Record={
                    'Data': json.dumps(json_data)
                }
            )
            
            return JsonResponse({'success': True, 'response': response})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    else:
        return JsonResponse({'error': 'This endpoint only accepts POST requests.'}, status=405)
