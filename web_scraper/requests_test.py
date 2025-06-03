import requests

# Zona pequeña de prueba
zona_prueba = {
    "top": -33.4896,
    "bottom": -33.5077,
    "left": -70.7973,
    "right": -70.7762,
    "tipos": "alerts,traffic,users"
}

url = (
    f"https://www.waze.com/live-map/api/georss"
    f"?top={zona_prueba['top']}&bottom={zona_prueba['bottom']}"
    f"&left={zona_prueba['left']}&right={zona_prueba['right']}"
    f"&env=row&types={zona_prueba['tipos']}"
)

print("Realizando solicitud de prueba a Waze...")
try:
    res = requests.get(url)
    if res.status_code == 200:
        print("Datos recibidos:")
        print(res.text[:2000])
    else:
        print(f"Código HTTP: {res.status_code}")
except requests.RequestException as err:
    print(f"Excepción de red: {err}")
