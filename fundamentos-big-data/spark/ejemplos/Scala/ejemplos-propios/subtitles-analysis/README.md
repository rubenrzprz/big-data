# Análisis de subtítulos en japonés

A partir de ficheros ```.txt``` o ```.srt``` de subtítulos en japonés, esta aplicación lee los datos y los tokeniza (extrae las palabras y obtiene su información gramatical y lecturas).

Creando una instancia de la clase Subtitles, se pueden realizar las siguientes operaciones:

- Contar palabras.
- Buscar las líneas que contienen un caracter o conjunto de caracteres.
- Buscar las palabras que contienen un tipo de escritura determinado (hiragana, katakana o kanji).
- Buscar las palabras que contienen una lectura determinada.
- Buscar las palabras de un tipo (verbos, sustantivos, partículas...).
- Obtener *n* líneas antes y/o *m* líneas después de una línea en concreto (contexto de una oración).

