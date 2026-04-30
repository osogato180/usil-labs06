# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "-r /Workspace/Users/osogato180@gmail.com/usil-labs06/requirements.txt",
# ]
# ///
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv
load_dotenv()

# COMMAND ----------

# os.getenv("MONGODB_CONNECTION_STRING")

# COMMAND ----------

def connect_to_mongodb():
    """Conectar a MongoDB Atlas usando la cadena de conexión"""
    connection_string = os.getenv("MONGODB_CONNECTION_STRING")
    if not connection_string:
        print("Error: No se encontró la variable de entorno MONGODB_CONNECTION_STRING")
        #connection_string = ""
        return None

    try:
        client = MongoClient(connection_string)
        # Verificar la conexión
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Error conectando a MongoDB Atlas: {e}")
        return None

# COMMAND ----------

def query_theaters_by_city(city_name):
    """Consultar teatros en la base de datos sample_mflix por nombre de ciudad"""
    try:
        # Conectar a MongoDB
        client = connect_to_mongodb()
        if not client:
            return None

        # Acceder a la base de datos sample_mflix
        db = client.sample_mflix

        # Acceder a la colección de teatros
        theaters_collection = db.theaters

        # Consultar teatros por ciudad
        query = {"location.address.city": city_name}
        #theaters = list(theaters_collection.find(query).limit(5))

        # Obtener conteo total
        total_count = theaters_collection.count_documents(query)

        # Cerrar conexión
        client.close()

        #return theaters, total_count
        return total_count

    except Exception as e:
        print(f"Error al consultar MongoDB: {e}")
        return None, 0

# COMMAND ----------

city_name = "Chicago"
total_count = query_theaters_by_city(city_name)
print(f"Total de teatros en {city_name}: {total_count}")

# COMMAND ----------

# Conexión a MongoDB
client = connect_to_mongodb()
db = client['sample_mflix'] 
collection = db['theaters']

def get_next_theater_id():
    """Obtiene el siguiente theaterId basado en el máximo existente"""
    result = collection.find().sort("theaterId", -1).limit(1)
    max_id = 0
    for doc in result:
        max_id = doc.get('theaterId', 0)
    return max_id + 1

def insert_theaters():
    """Inserta 5 nuevos theaters con IDs automáticos"""
    
    # Obtener el siguiente ID disponible
    next_id = get_next_theater_id()
    
    # Datos de los nuevos theaters
    new_theaters = [
        {
            "location": {
                "address": {
                    "street1": "1250 Broadway",
                    "city": "New York",
                    "state": "NY",
                    "zipcode": "10001"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-73.9857, 40.7484]
                }
            }
        },
        {
            "location": {
                "address": {
                    "street1": "456 Hollywood Blvd",
                    "city": "Los Angeles",
                    "state": "CA",
                    "zipcode": "90028"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-118.3267, 34.1019]
                }
            }
        },
        {
            "location": {
                "address": {
                    "street1": "789 State St",
                    "city": "Chicago",
                    "state": "IL",
                    "zipcode": "60605"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-87.6244, 41.8756]
                }
            }
        },
        {
            "location": {
                "address": {
                    "street1": "321 Ocean Drive",
                    "city": "Miami",
                    "state": "FL",
                    "zipcode": "33139"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-80.1300, 25.7810]
                }
            }
        },
        {
            "location": {
                "address": {
                    "street1": "555 Pine St",
                    "city": "Chicago",
                    "state": "WA",
                    "zipcode": "98101"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-122.3370, 47.6062]
                }
            }
        }
    ]
    
    # Agregar theaterId automático a cada documento
    for i, theater in enumerate(new_theaters):
        theater['theaterId'] = next_id + i
    
    try:
        # Insertar los documentos
        result = collection.insert_many(new_theaters)
        print(f"✅ Insertados {len(result.inserted_ids)} theaters exitosamente")            
    
    except Exception as e:
        print(f"❌ Error al insertar: {e}")

# COMMAND ----------

insert_theaters()
city_name = "Chicago"
total_count = query_theaters_by_city(city_name)
print(f"Total de teatros en {city_name}: {total_count}")
client.close()
