from django.http import JsonResponse
import json


def get_blogs(request):
    res = {
        'success': True,
        'message': "function based view : api to get blogs" 
    }
    return JsonResponse(res)

def create_blogs(request):
    body = json.loads(request.body)
    print(body)
    if request.method == 'POST' :

        res = {
            'success': True,
            'message': "function based view : api to create blogs" 
        }
    else :
         res = {
            'success': False,
            'message': "function based view : api to create blogs" 
        }   
    return JsonResponse(res)
    
