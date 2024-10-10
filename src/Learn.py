import sklearn 
import hulearn
import math
import numpy as np

def location_prediction(self, products:list, weight_event:dict, current_cart:list, location:tuple, pmap:list, store_id:str, confidence_multiplier:float, vendor:bool=False, additional_weights:list=[]) -> list:
        """
        Make prediction on weight event using downloaded product mapper
        """
        # Catch noisy weight events where weight delta doesnt reach the minimum weight of any products on shelf
        weight = weight_event["weight_delta"]
        sorted_weights = sorted([product["GrossWeight"] for product in products] + additional_weights)
        min_weight = sorted_weights[0] if sorted_weights != 0 else sorted_weights[1]
        if abs(weight) < self.MIN_WEIGHT_THRESHOLD_MULTIPLIER * min_weight:
            self.logging.info(f"WEIGHT EVENT FILTERED OUT, weight {weight} and minimum product weight {min_weight}")
            return list()
        
        # Begin forming prediction by localizing where weight occured on a shelf
        shelf_coordinates = self.shelf_infos[store_id][str(products[0]['smart_system_id'])][str(products[0]['gondola_id'])][str(products[0]['shelf'])]   
        relative_weight_loc = max(0, min(float(weight_event['xlocation']), 1)) * sqrt(
            abs(float(shelf_coordinates['y_front_right']) - float(shelf_coordinates['y_front_left'])) ** 2
            + abs(float(shelf_coordinates['x_front_right']) - float(shelf_coordinates['x_front_left'])) ** 2)
    
        # Hand off further refinement to either putback or grab helper functions
        if weight > 0:
            if vendor:
                return self.weight_distance_prediction(products, relative_weight_loc, weight_event, confidence_multiplier, vendor)
            return self.handle_putback(current_cart, relative_weight_loc, weight, location, pmap, confidence_multiplier, vendor)
        else:
            return self.weight_distance_prediction(products, relative_weight_loc, weight_event, confidence_multiplier)
                #return self.handle_grab(weight, products, relative_weight_loc, confidence_multiplier)
        
def weight_distance_prediction(self, products: list[dict[str, Any]], relative_weight_loc: float, weight_event: dict[str, Any] , confidence_multiplier, vendor=False) -> list[Candidate]:
        """
        New algorithm for calculating prediction confidence
        Takes in a list of products, relative weight location of event, weight event details
        Returns a list of candidate products with prediction confidences
        """
        abs_weight_delta = abs(float(weight_event["weight_delta"]))
        weight_event_point = (
            relative_weight_loc, 
            abs_weight_delta
        )
        product_points = list()
        for product in products:
            data_points = []

            dist_to_left_boundary = abs(relative_weight_loc - product["X_Left"])
            dist_to_right_boundary = abs(relative_weight_loc - product["X_Right"])
            
            if dist_to_left_boundary <= dist_to_right_boundary:
                closest_boundary = product["X_Left"]
            else:
                closest_boundary = product["X_Right"]
            
            product_weight = product["GrossWeight"]
            if product_weight:
                raw_quantity = abs_weight_delta / product_weight
            else:
                raw_quantity = 1
                
            if raw_quantity >= 1:  # Don't consider qty 0 
                data_points.append((closest_boundary, product_weight * math.floor(raw_quantity)))
            data_points.append((closest_boundary, product_weight * math.ceil(raw_quantity)))

            product_points.append({"product": product, "datapoints": data_points})

        # Calculate distance between weight event point and all other points
        point_distances = self.calculate_2d_distance_new(weight_event_point, product_points)
        
        weights = np.array([d["weight_distance"] for d in point_distances])
        weight_denominator = np.sqrt(weights.sum())
        
        for point in point_distances:
            point["weight_distance"] /= weight_denominator
            point["distance"] = point["weight_distance"] + point["location_distance"]
        
        inverse_distances = np.array([1 / data["distance"] for data in point_distances])
        confidences = inverse_distances / inverse_distances.sum()
        
        # Add predictions
        vendor_qty_multiplier = -1 if vendor else 1
        prediction_candidates = [
            Candidate(
                product=distance["product"],
                probability=confidence_multiplier * confidence * self.QTY_DECAY**(distance["qty"] - 1 + distance["qty_remainder"]),
                quantity=distance["qty"] * vendor_qty_multiplier
            )
            for distance, confidence in zip(point_distances, confidences)
        ]
        
        return prediction_candidates

def calculate_2d_distance(self, weight_event_point:tuple, product_points:list):
    """
    Helper function to determine prediction candidates by calculdating the distance between weight point and product point
    """
    distances = list()
    for point_data in product_points:
        product_distances = list()
        data_points = point_data["datapoints"]
        for point in data_points:
            product_distances.append(math.dist(weight_event_point, point)) 
        min_product_weight = data_points[product_distances.index(min(product_distances))][1] # Get the weight of the product corresponding to the minimum distance
        distances.append({
            "distance" : min(product_distances),
            "weight" : min_product_weight,
            "qty": max(1, int(min_product_weight / point_data["product"]["GrossWeight"])),
            "product" : point_data["product"]
        })
    absolute_minimum = min([distance["distance"] for distance in distances]) * self.MIN_DISTANCE_SCALAR
    final_candidates, tossed_candidates = list(), list()
    for data in distances:
        if data["distance"] <= absolute_minimum:
            final_candidates.append(data)
        else:
            tossed_candidates.append(data)
    return final_candidates, tossed_candidates