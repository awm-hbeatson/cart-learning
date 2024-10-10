import numpy as np
from os import getenv 
from json import loads
import pymysql
from Backtesting import Backtester
from ProductMapper import ProductMapper
from hulearn.classification import FunctionClassifier
from awm_connector.awm_connector import AWM_Connector
from cart_tools.Toolkit import load_shelf_info

PRODUCT_LIMIT = 50
R_W_L = 0 # Relative weight location
W_D = 1 # Weight delta

TEST_SESSION = "f9a44d34-642a-43ff-a202-0e3b04c8eda2"


backtester = Backtester()
sql = pymysql.connect(
    host=getenv("SQL_ADDRESS"),
    user=getenv("SQL_USERNAME"),
    password=getenv("SQL_PASSWORD"),
    database="frictionless",
    port=4000
)


def load_all_sessions(sql):
    """
    
    """
    returned_sessions = list()
    with sql.cursor() as cursor:
        cursor.execute(f"select DISTINCT session_id from frictionless.cart_predictions where num_grabs > 1 and num_grabs < 3 and num_putbacks = 0;")
        sessions = cursor.fetchall()
        for session in sessions:
            session = session[0]
            result = backtester.make_backtest_request(session, f"{getenv('CART_BRANCH')}-v3.2.0-p")
            #print(result.headers)
            if "is_correct" in result.headers:
                if result.headers["is_correct"] == "True":
                    returned_sessions.append(session)
    return returned_sessions

def get_session(sql, session_id):
    """
    
    """
    with sql.cursor() as cursor:
      
        cursor.execute(f"select * from frictionless.cart_predictions where session_id='{session_id}'")
        result = cursor.fetchone()
        metadata = loads(result[5])
        
        inputs = {
            "metadata" : metadata["metadata"],
            "predictions" : metadata["predictions"],
            "products" : metadata["products"]
        }
    with sql.cursor() as cursor:
        cursor.execute(f"select * from frictionless.reviewed_cart where session_id='{session_id}'")
        outputs = cursor.fetchone()
        #print(outputs)
    return (inputs, outputs)
            

def test_f(inp):
    return np.array(inp)


def preprocess_input(predictions, product_mapper, shelf_infos):
    preds = list()
    for prediction in predictions:
        print(prediction)
        store_id = prediction["store_id"]
        slice_pmap = product_mapper.get_class_info(
            shelf_info=shelf_infos[store_id], 
            timestamp=slice["start"], 
            use_realogram=False,
        )
        #print(prediction)
        z = [0, 0] + [0] * 3 * PRODUCT_LIMIT
        z[W_D] = prediction["sample_value"]
        z[R_W_L] = prediction["weight_location_x"]
        preds.append(z)
        # Set product weight and boundary values

    return np.array(preds)

def postprocess_output(results):
    entries = list()
    contents = loads(results[3])
    for item in contents["cart"]:
        z = [0] * PRODUCT_LIMIT
        z[int(item["shelf_location"])-1] = int(item["quantity"])
        entries.append(z)
    return np.array(entries)


if __name__ == "__main__":
    # Load shelf info
    connector = AWM_Connector(
        storage_address = getenv("STORAGE_ADDRESS"),
        storage_key = getenv("STORAGE_KEY"),
        storage_secret = getenv("STORAGE_SECRET"),
        service_name = f"cart",
        source_bucket = getenv("SOURCE_BUCKET"),
        network = 'internal',
        no_config = True,
        use_local_config = False,
        use_local_service_config = False
    )
    store_ids = list(loads(getenv("storeinfo")).keys()) # Load multiprod list of streaming stores
    print(f"Store ID list = {store_ids}")
    shelf_infos = load_shelf_info(connector)
    product_mapper = ProductMapper()

    all_sessions = load_all_sessions(sql)
    print(f"All sessions = {all_sessions}")
    for session in all_sessions:
        inputs, outputs = get_session(sql, session)
        prepared_inputs = preprocess_input(inputs["predictions"])
        prepared_outputs = postprocess_output(outputs)
        print(prepared_outputs)

    classifier = FunctionClassifier(test_f)