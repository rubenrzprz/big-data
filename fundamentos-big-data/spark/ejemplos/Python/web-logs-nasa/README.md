# Análisis de Web Logs de la NASA

Para este ejercicio emplearemos unos logs que podemos obtener a través de:
```bash
wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
```

Los logs presentan el siguiente aspecto:

> 133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713

Leemos sus líneas y extraemos los siguientes campos mediante una expresión regular:

- **Host**: 133.43.96.45
- **User_identifier**: -
- **User_id**: -
- **Date**: 01/Aug/1995:00:00:23 -0400
- **Request_method**: GET
- **Resource**: /images/launch-logo.gif
- **Protocol**: HTTP/1.0
- **Http_status**: 200
- **Content_size**: 1713

Posteriormente, lo convertiremos a un DataFrame y finalmente analizaremos sus datos mediante consultas.