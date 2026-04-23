# Recommendation engine

### Para levantar

Inicializar la DB (solo la primera vez): 
```
docker-compose up airflow-init
```

Levantar todo: 
```
docker-compose up -d
```

Verificar Airflow: 

Verificar que se levante el servidor de Airflow en 
http://localhost:8080/

```
usuario: admin
password: admin
```

Si no permite acceso, crear el usuario `admin` ejecutando 

```
docker exec -it airflow_webserver airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@example.com
```
