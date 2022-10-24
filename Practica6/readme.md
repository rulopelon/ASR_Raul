# Entrega 2 de la práctica 6 
- # Objetivo
El objetivo de esta entrega es realizar una prueba de estrés a un cluster de kubernetes previamente desplegado. 
Para ello se va a crear un contenedor Docker con toda la funcionalidad incorporada.
El despliegue del contenedor se va a realizar por medio de un Job de Kubernetes.

- # Contenedor Docker
El contenedor Docker requerido debe de poder ejecutar comando ab, es decir que debe de tener instalada la librería apache2.
El fichero dockerfile con la descripción del contenedor es el siguiente:

```
FROM ubuntu:latest

RUN apt-get -y update; \
    apt-get -y upgrade; \
    apt-get -y install apt-utils \
RUN apt install apache2-utils
```

    - ## Pasos para la construcción del contenedor