# Procesamiento de Streams

## Introducción

El procesamiento de streams (o flujo de datos) es el acto de incoporar continuamente datos nuevos para calcular un resultado. Los datos de entrada son ilimitados y no tienen un inicio o final determinado.

### Procesamiento en streaming vs procesamiento en batch

En el procesamiento por lotes (batch), el cálculo se ejecuta con un conjunto de datos de entrada fija y una única vez.

Sin embargo, en el procesamiento en streaming, la entrada de datos es ilimitada y se obtienen múltiples versiones del resultado a medida que se está ejecutando.

A pesar de sus diferencias, en la práctica suelen trabajar juntos: las aplicaciones de stream combinan datos de entrada con datos escritos periódicamente por un batch y la salida de los trabajos de streaming suelen ser ficheros o tablas que se emplean posteriormente en trabajos por lotes.
