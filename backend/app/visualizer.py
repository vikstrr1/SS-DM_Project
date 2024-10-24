import json

class Visualizer:
    def __init__(self, data):
        self.data = data

    def create_chart_data(self):
        # Transform self.data to a format suitable for charting
        return json.dumps(self.data)  # Adjust this as needed
