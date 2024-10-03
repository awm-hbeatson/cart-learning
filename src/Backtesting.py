import requests  
import mlflow
import random
from json import dumps
from collections import defaultdict
from time import sleep, time
from os import getenv, environ, wait
from numpy import select, random, arange

class Session:
    def __init__(self):
        pass

class CartVersion:
    def __init__(self) -> None:
        pass

class Backtester:
    def __init__(self):
        if getenv("CLUSTER_ID") == "global.us.central.1":
            # Initiate ML flow
            environ["MLFLOW_TRACKING_USERNAME"] = "mlflow"
            environ["GIT_PYTHON_REFRESH"] = "quiet"
            mlflow.set_tracking_uri("https://mlflow.awmfric.com")
            mlflow.set_experiment("Cart-Backtest-Demo")
            self.cart_versions = [
                f"{getenv('CART_BRANCH')}-v3.2.0-p",
                f"{getenv('CART_BRANCH')}-v3.2.1-p",
                #f"buzz-v3.2.2-3.5weight-p"
            ]
            #self.cart_versions = ['buzz-v3.2.2-p-0.6', 'buzz-v3.2.2-p-0.625', 'buzz-v3.2.2-p-0.65', 'buzz-v3.2.2-p-0.675', 'buzz-v3.2.2-p-0.7000000000000001', 'buzz-v3.2.2-p-0.7250000000000001', 'buzz-v3.2.2-p-0.7500000000000001']
            self.session_tags = [
                "multi-product-grab", 
                "incomplete-reach", 
            ]
            self.prediction_tags = [
                "false-negative",
                "false-positive",
                "qty-error",
                "off-by-one",
                "location_match"
            ]
        else:
            print(f"Current cluster is not Global, backtesting will be functionally useless")

    def make_backtest_request(self, session, version):
        """
        Hit Cart Metrics endpoint to test for prediction correctness
        """
        metrics_address = "http://cart-metrics-dev.default.svc.cluster.local:5006/backtest"
        body = {
                "session_id":session,
                "cart_version": version
        }
        try:
            backtest_analysis = requests.get(
                metrics_address,
                json=body
            )
        except Exception as e:
            print(f"Error {e}")
            return dict()
        return backtest_analysis

    def get_prediction_tags(self, headers):
        """
        Parse the headers from Cart-Metrics API to figure out which prediction level tags apply to the carts results
        """
        prediction_tags = list()
        #print(headers)
        if "is_correct" in headers:
            num_fn = int(headers['num_fn'])
            num_reviews = int(headers['num_reviews'])
            num_fp = int(headers['num_fp'])
            if num_fn > 0:
                prediction_tags.append("false-negative")
            if num_fp > 0:
                prediction_tags.append("false-positive")
        return prediction_tags
    
    def get_session_tags(self, sql, session):
        """
        Get session-level tags (stuff like bad weight events, bad reaches, etc) from DB
        """
        with sql.cursor() as cursor:
            cursor.execute(f"select * from frictionless.upload_record_tables where session_id='{session}'")
            session_tags = cursor.fetchone()[-1] if cursor.rowcount > 0 else list()
            if session_tags is None:
                session_tags = list()
        return session_tags

    def backtest_all_sessions(self, sql, config, versions=False, run_analysis=False):
        """
        Wrapper function to aggregate all Cart predictions and run them through backtesting endpoint, score for correctness, and create MLFlow Experiment
        """
        # If needed, re-predict upon all global sessions
        if run_analysis:
            self.run_all_sessions(sql, config)

        # Get all predictions from global storage
        session_results = dict()
        with sql.cursor() as cursor:

            # Load backtested cart versions for comparison
            if versions:
               cursor.execute("SELECT DISTINCT cart_version from frictionless.cart_predictions")
               cart_versions = [session[0] for session in cursor.fetchall()]
            else:
                cart_versions = self.cart_versions
            print(f"Cart versions being tested: {cart_versions}")
            # Aggregate all session IDs
            cursor.execute("SELECT DISTINCT session_id from frictionless.cart_predictions")
            sessions = [session[0] for session in cursor.fetchall()]

            # Begin backtesting pulled down session IDs
            print(f"Backtesting {len(sessions)} sessions")
            session_ctr = 1
            for session in sessions:
                print(session_ctr)
                session_ctr += 1
                # Collect session-level tags
                session_tags = self.get_session_tags(sql, session)
                for version in self.cart_versions:
                    result = self.make_backtest_request(session, version)
                    print(result.headers, version)
                    # Incorporate session-level and prediction-level tags
                    if "is_correct" in result.headers:
                        prediction_tags = self.get_prediction_tags(result.headers) 
                        session_results.setdefault(session, {"versions": dict(), "session_tags": session_tags})
                        if version not in session_results[session]["versions"].keys():
                            session_results[session]["versions"][version] = dict()
                        session_results[session]["versions"][version]["prediction_tags"] = prediction_tags
                        session_results[session]["versions"][version]["is_correct"] = result.headers["is_correct"] == "True"
                            
        print("Scoring results...")
        # Initialize data struct for correctness with all counters set to 0
        version_results = {
            version: { 
                "correct" : 0,
                "total" : 0,
                "session_tags" : {
                    session_tag : {
                        "correct" : 0,
                        "total" :0
                    } for session_tag in self.session_tags
                },
                "prediction_tags" : {
                    prediction_tag : {
                        "total" : 0
                    } for prediction_tag in self.prediction_tags
                }
            } for version in self.cart_versions 
        }

        # Start scoring
        for results in session_results.values():
            for version, version_info in results["versions"].items():
                is_correct = version_info["is_correct"]
                # Update based on general results 
                if is_correct:
                    version_results[version]["correct"] += 1
                version_results[version]["total"] += 1
                for session_tag in version_results[version]["session_tags"].keys():
                    # Update based on each tag
                    if session_tag in results["session_tags"]:
                        if is_correct:
                            version_results[version]["session_tags"][session_tag]["correct"] += 1
                        version_results[version]["session_tags"][session_tag]["total"] += 1

                for prediction_tag in version_info["prediction_tags"]:
                    version_results[version]["prediction_tags"][prediction_tag]["total"] += 1

        # Run MLFlow experiment
        self.run_mlflow_experiment(version_results, config)
        print("Done...")
        while True:
            sleep(1)

    def run_mlflow_experiment(self, version_results, config):
        """
        Run MLFlow experiment for each version, logging config, and scoring each tag.
        """
        for version, results in version_results.items():
            with mlflow.start_run(run_name=f"backtest_{version}"):
                mlflow_params = {
                    "version" : version,
                    "session_count" : results['total'],
                    "config" : config
                }
                mlflow.log_params(mlflow_params)

                # Start doing tag-level scoring
                score = round((results['correct']/results['total']), 3)
                mlflow.log_metric(f"general_score", score)

                # Session tags
                for session_tag in results["session_tags"].keys():
                    session_tag_score = round(results["session_tags"][session_tag]["correct"]/(max(results["session_tags"][session_tag]["total"], 1)), 3)
                    mlflow.log_metric(f"{session_tag}_score", session_tag_score)

                # Prediction tags
                for prediction_tag in results["prediction_tags"].keys():
                    prediction_tag_total = results["prediction_tags"][prediction_tag]["total"]
                    mlflow.log_metric(f"{prediction_tag}_total", prediction_tag_total)
                print(f"Version {version} had {results['correct']} correct sessions of {results['total']} total, for an accuracy of {score*100}%")

    def build_param_sweep_configs(self, config, value, start, stop, step):
        """
        
        """
        configs = list()
        #values = 
        for i in arange(start, stop, step):
            config_copy = config.hyperparameters.copy()
            config_copy[value] = round(i, 3) 
            configs.append(config_copy)
        return configs

    def run_cart(self, session_id: str, config:dict, dev=True):
        """
        Analyze session if need be
        """
        #print(f"Running with config {config}")
        if dev:
            uri = "http://cart-analyzer-dev.default.svc.cluster.local:5005/analysis"
        else:
            uri = "http://cart-analyzer-3-1-9.default.svc.cluster.local:5017/analysis"
        r = requests.get(
            url=uri,
            json={
                "session_id": session_id, 
                "gid_hash": "", 
                "submit": True, 
                "config" : dumps(config)
            }
        )
        return str(r.status_code)

    def run_all_sessions(self, sql, config):
        """
        Get all sessions within global storage and re-predict upon them
        """
        with sql.cursor() as cursor:
            all_session_id_query = "SELECT session_id from frictionless.cart_predictions"
            session_ids = cursor.execute(all_session_id_query)
            sessions = list(set([session[0] for session in cursor.fetchall()]))
            print(session_ids)
            for session_id in sessions:
                print(f'Testing {session_id}...')
                self.run_cart(session_id, config, dev=True)
        print("DONE")
        # while True:
        #     sleep(1)


    