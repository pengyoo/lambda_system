from serving_layer.api import app

def start_api():
    app.run(debug=True)

if __name__ == "__main__":
    start_api()