import requests
import json
import base64
import hashlib
import hmac
import time
from datetime import datetime
from os import getenv

class ProductMapper:
    def __init__(self) -> None:
        self.scale = 1/12 # inch to feet ratio
        
    def get_class_info(self, shelf_info:dict, timestamp:str, use_realogram:bool=False, use_timestamp:bool=True) -> list:
        '''
        Begin process of aggregating and sorting class info according to planogram/realogram at specified time
        '''
        unique_system_id = shelf_info.keys()
        raw_product_data = dict()
        for system_id in unique_system_id:
            raw_product_data[system_id] = self.getPlanogram(system_id, timestamp, use_realogram, use_timestamp) # This looks good to go
            if raw_product_data[system_id] is None:
                print(f"Failed to get product mapper data for {system_id}")
                return
        dataMapped = self.dataMapping(shelf_info, raw_product_data)
        class_info = self.sort_class_info(dataMapped)
        return class_info

    def sort_class_info(self, class_info:list) -> list:
        '''
        Re-organize class info once it has been mapped by dataMapping()
        '''
        for (gondola_id, shelf_id) in class_info:
            products = class_info[(gondola_id, shelf_id)]
            x_hash = dict()
            for i,product in enumerate(products):
                x_hash[product['X']] = i
            new_products = []
            sorted_x = sorted(x_hash.keys())
            for x in sorted_x:
                new_products.append(products[x_hash[x]])
            class_info[(gondola_id, shelf_id)] = new_products
        return class_info

    def dataMapping(self, shelf_info:dict, raw_product_data:list) -> dict:
        '''
        Combine shelf info with raw product data into parseable format
        '''
        class_info = dict()
        for system_id in raw_product_data:
            system_data = raw_product_data[system_id]
            for product_data in system_data:
                #print(product_data)
                frictionless_gondola_id = str(product_data['frictionlessGondolaId'])
                shelf = str(product_data['shelf'])
                #print(shelf_info[system_id].keys())
                #print(system_id, frictionless_gondola_id)
                shelf_data_section = shelf_info[system_id][frictionless_gondola_id]
                shelf_data = shelf_data_section[shelf]
                #print(shelf_data.keys())

                # getting frictionless index(s)
                shelf_id = int(shelf_data['shelf'])
                gondola_id = int(shelf_data['gondola_id'])
                if (gondola_id, shelf_id) not in class_info:
                    class_info[(gondola_id, shelf_id)] = []

                # product data object
                product = dict()
             
                product['gondola_id'] = gondola_id
                product['shelf_id'] = shelf_id
                product['price'] = product_data['price']
                product['Name'] = product_data['name']
                product['Upc'] = product_data['upc']
                try:
                    product['GrossWeight'] = float(product_data['grossWeight'])
                except:
                    netWeight = product_data.get("netWeight")
                    if netWeight is None:
                        netWeight = 0
                    product['GrossWeight'] = float(netWeight)

                if product_data['depth'] is None:
                    product['Depth'] = 0
                else:
                    product['Depth'] = float(product_data['depth']) * self.scale
                
                # product locational info
                deltax = float(shelf_data['x_front_right']) - float(shelf_data['x_front_left'])
                deltay = float(shelf_data['y_front_right']) - float(shelf_data['y_front_left'])
                dis = (deltax**2 + deltay**2) ** 0.5
                shelf_vec = [deltax/dis, deltay/dis]
                product['X_3D'] = float(shelf_data['x_front_left']) + shelf_vec[0] * product_data['x'] * self.scale
                product['Y_3D'] = float(shelf_data['y_front_left']) + shelf_vec[1] * product_data['x'] * self.scale
                product['Z_3D'] = float(shelf_data['height'])
                product['Adjusted_x'] = shelf_vec[0] * float(product_data['widthOnShelf']) * self.scale
                product['Adjusted_y'] = shelf_vec[1] * float(product_data['widthOnShelf']) * self.scale

                # system info
                product['smart_system_name'] = shelf_data['smart_system_name']
                product['smart_system_id'] = system_id
                product['X'] = float(product_data['x'])
                product['section'] = int(product_data['section'])
                product['shelf'] = int(product_data['shelf'])
                product['shelf_width'] = dis # NOTE: Can we use this to localize relative weight location instead of shelf info boundaries? Returned in inches instead of feet ????
                product['X_Left'] = float(product_data['x']) * self.scale
                product['X_Right'] = (float(product_data['x']) + float(product_data['widthOnShelf'])) * self.scale
                class_info[(gondola_id, shelf_id)].append(product)
        return class_info

    def getPlanogram(self, smartSystemUId:str, timestamp:str, realogram:bool=False, use_timestamp:bool=True) -> list:
        """
        Hit Fullstack API endpoint to download planogram for specified Smart System ID. Timestamp argument gives historical planogram (planogram config at specified time), while realogram bool toggles whether planogram or realogram is returned
        """
        timestamp = datetime.utcfromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        API_ENDPOINT = getenv("PLANOGRAM_API_ENDPOINT")
        method = "GET"
        uri = f"/api/SmartSystem/{smartSystemUId}/ProductExport".lower()
        if use_timestamp:
            query_string = f"?activeDate={timestamp}"
        else:
            query_string = ""

        if realogram:
            query_string += "&realogram=true"
        
        requestDate = int(time.time())
        apiKey = getenv("PLANOGRAM_API_ACCESS_KEY")
        secretKey = getenv("PLANOGRAM_API_SECRET_KEY")
        hashMessage = method.lower() + uri.lower() + query_string.lower().replace("?","").replace("=","").replace("&",",") + str(requestDate) + apiKey
        message = bytes(hashMessage, 'utf-8')
        secret = bytes(secretKey, 'utf-8')

        hash = hmac.new(secret, message, hashlib.sha256)
        signature = base64.b64encode(hash.digest()).decode('utf-8')
        authorizationHeaders = apiKey + "," + str(requestDate) + "," + signature
        r = requests.get(url = API_ENDPOINT + uri + query_string, headers={"Authorization" : authorizationHeaders})
        if r.status_code == 200:
            records = json.loads(r.text)
            data = []
            for record in records:
                data.append(record)
            return data
        else:
            print(f"Error downloading planogram/realogram with status code {r.status_code} and info {r.text}")
            return None
        
    def lookup_by_upc(self, product_upc, product_mapper, gondola):
        """
        Helper function used to locate specific product via UPC in product mapper data struct
        Not used by anything crucial
        """
        for location in product_mapper.keys():
            if location[0] == gondola:
                for item in product_mapper[location]:
                    if item["Upc"] == product_upc:
                        return item
        print(f"Item not found")
        return None
