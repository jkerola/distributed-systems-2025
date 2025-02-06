from locust import task, HttpUser, between

class TestAPI(HttpUser):
    # host = 'http://control:8080'
# host = 'http://0.0.0.0:8080'
    # def on_start(self):
    #     self.client.verify = False
    #     print("Starting Test !!!")

    @task
    def do_get(self):
        self.client.verify = False
        self.client.get("/")

    wait_time = between(0.1, 2)

    # def on_stop(self):
    #     print("Stopping Test")