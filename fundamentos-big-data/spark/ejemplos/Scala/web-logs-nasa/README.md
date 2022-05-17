# Análisis de Web Logs de la NASA

A partir de unos ficheros de logs en formato txt con el siguiente aspecto:

> 133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713

Leemos las líneas y extraemos los siguientes campos:

- **Host**: 133.43.96.45
- **User_identifier**: -
- **User_id**: -
- **Date**: 01/Aug/1995:00:00:23 -0400
- **Request_method**: GET
- **Resource**: /images/launch-logo.gif
- **Protocol**: HTTP/1.0
- **Http_status**: 200
- **Content_size**: 1713

Lo convertiremos a un DataFrame y lanzaremos consultas para extraer conclusiones.