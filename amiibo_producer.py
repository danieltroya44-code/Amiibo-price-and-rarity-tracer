import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import requests

TOPIC_NAME = "amiibo.market.raw"
BOOTSTRAP_SERVERS = ["localhost:9092"]
LOCAL_JSON_FILE = "all_amiibos.json"

# Inicializar productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def load_amiibo_catalog(filepath=LOCAL_JSON_FILE):
    # 1. Intentar conexión a la API en vivo (Timeout de 3 segundos)
    api_url = "https://www.amiiboapi.com/api/amiibo/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    print("⏳ Intentando conectar a AmiiboAPI en vivo (Timeout: 3s)...")
    try:
        response = requests.get(api_url, headers=headers, timeout=3)
        if response.status_code == 200:
            catalog = response.json().get("amiibo", [])
            print(
                f"✅ ¡Conexión exitosa a la API! {len(catalog)} amiibos en tiempo real."
            )
            return catalog
    except Exception:
        print("⚠️ Sin respuesta de la API (bloqueo de red o timeout).")

    # 2. Modo Offline: Si falla la API, usar tu lógica local intacta
    print(f"🔄 Activando MODO OFFLINE: Leyendo '{filepath}'...")
    if not os.path.exists(filepath):
        print(f"Error: No existe el archivo {filepath}")
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    catalog = []

    # Extraer la lista o diccionario base
    if isinstance(raw_data, dict):
        if "amiibo" in raw_data:
            items = raw_data["amiibo"]
        elif "amiibos" in raw_data:
            # Caso diccionario { "0000000000000002": {...} }
            items = []
            for k, v in raw_data["amiibos"].items():
                v["id_hex"] = k.replace("0x", "")
                items.append(v)
        else:
            items = [
                {"id_hex": k.replace("0x", ""), **v}
                for k, v in raw_data.items()
                if isinstance(v, dict)
            ]
    else:
        items = raw_data

    for item in items:
        # 1. Resolver Head y Tail
        if "id_hex" in item:
            full_hex = str(item["id_hex"]).zfill(16)
            head = full_hex[:8]
            tail = full_hex[8:]
        else:
            head = str(item.get("head", "00000000")).replace("0x", "").zfill(8)
            tail = str(item.get("tail", "00000002")).replace("0x", "").zfill(8)

        # 2. Resolver URL de Imagen obligatoria
        img = (
            item.get("image")
            or item.get("image_url")
            or f"https://raw.githubusercontent.com/N3evin/AmiiboAPI/master/images/icon_{head}-{tail}.png"
        )

        # 3. Resolver Series (soporta camelCase y snake_case)
        game_s = (
            item.get("gameSeries")
            or item.get("game_series")
            or item.get("game")
            or "Nintendo"
        )
        amiibo_s = (
            item.get("amiiboSeries")
            or item.get("amiibo_series")
            or item.get("series")
            or "General"
        )

        catalog.append(
            {
                "name": item.get("name", "Unknown Amiibo"),
                "gameSeries": game_s,
                "amiiboSeries": amiibo_s,
                "head": head,
                "tail": tail,
                "image": img,
            }
        )

    print(
        f"Catálogo parseado correctamente: {len(catalog)} figuras listas con metadatos completos."
    )
    return catalog


def generate_market_payload(amiibo):
    """Genera la métrica de mercado simulada/rastreada para un amiibo individual."""
    retail_price = 15.99
    amiibo_id = f"{amiibo.get('head', '00000000')}{amiibo.get('tail', '00000000')}"
    name = amiibo.get("name", "Unknown")

    # Figuras con alto valor histórico/rareza
    grail_keywords = [
        "Qbby",
        "Mega Yarn Yoshi",
        "Solaire",
        "Navirou",
        "Gold Mario",
        "Poochy",
    ]
    mid_rare_keywords = [
        "Silver Mario",
        "Metroid",
        "Majora",
        "Skyward",
        "Player 2",
        "Callie",
        "Marie",
    ]

    if any(k.lower() in name.lower() for k in grail_keywords):
        multiplier = random.uniform(8.0, 16.0)  # Nivel Santo Grial ($120 - $250+)
        active_listings = random.randint(1, 5)
    elif any(k.lower() in name.lower() for k in mid_rare_keywords):
        multiplier = random.uniform(2.5, 6.0)  # Nivel Raro ($40 - $95)
        active_listings = random.randint(6, 18)
    else:
        multiplier = random.uniform(0.6, 2.2)  # Nivel Común / Regular ($9 - $35)
        active_listings = random.randint(20, 55)

    loose_price = round(retail_price * multiplier, 2)
    cib_price = round(loose_price * random.uniform(1.25, 1.6), 2)
    sealed_price = round(cib_price * random.uniform(1.2, 1.5), 2)

    return {
        "event_id": f"evt_{int(time.time() * 1000)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amiibo_id": amiibo_id,
        "metadata": {
            "name": name,
            "game_series": amiibo.get("gameSeries", "General"),
            "amiibo_series": amiibo.get("amiiboSeries", "General"),
            "image_url": amiibo.get("image", ""),
            "retail_price_usd": retail_price,
        },
        "market_data": {
            "loose_price_usd": loose_price,
            "cib_price_usd": cib_price,
            "sealed_price_usd": sealed_price,
            "active_listings_count": active_listings,
            "recent_sales_24h": random.randint(0, 10),
        },
    }


def main():
    catalog = load_amiibo_catalog()
    if not catalog:
        return

    print(f"Iniciando streaming continuo de {len(catalog)} figuras hacia Kafka...")

    try:
        while True:
            for item in catalog:
                payload = generate_market_payload(item)
                key = payload["amiibo_id"]

                # Enviar mensaje particionado por ID
                future = producer.send(TOPIC_NAME, key=key, value=payload)
                record_metadata = future.get(timeout=10)

                print(
                    f"[{payload['timestamp'][:19]}] Enviado: {payload['metadata']['name'][:22]:<22} | "
                    f"Loose: ${payload['market_data']['loose_price_usd']:>6.2f} | "
                    f"Listings: {payload['market_data']['active_listings_count']:>2} | "
                    f"Partición: {record_metadata.partition}"
                )
                time.sleep(0.08)
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()