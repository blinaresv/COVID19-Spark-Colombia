# Análisis de COVID-19 en Colombia con Apache Spark

**Curso:** Herramientas y visualización de datos
**Institución:** Fundación Universitaria Los Libertadores
**Integrantes:** Brandon Felipe Linares Viasus, Adriana Lucía Carreño Medina, Sariath Eyleen Xiomara Ariza Vargas

---

## Cómo llegamos a este proyecto (trazabilidad)

Antes de escribir una sola línea de código tomamos tres decisiones, y conviene dejarlas escritas porque explican casi todo lo que vino después.

Primero, el dataset. La actividad pide un archivo de más de 1 GB con columnas de varios tipos. Revisamos las fuentes permitidas y, dentro de Datos Abiertos Colombia, los únicos conjuntos que de verdad pasan el gigabyte son los de contratación pública (SECOP) y el de casos de COVID-19. Comparamos las dos opciones y nos quedamos con COVID-19 por una razón práctica: pesa 1.4 GB, trae fechas, una variable numérica y varias categóricas, y es un tema que cualquiera entiende sin explicación previa. Eso último pesa mucho en la sustentación.

Segundo, las preguntas. En vez de hacer cinco gráficas sueltas, definimos cinco preguntas distintas y para cada una elegimos el tipo de gráfica que mejor la responde. Esa es la lógica que está detrás de la sección "Por qué cada gráfica".

Tercero, el flujo de trabajo con Spark. Como el archivo no cabe cómodo en memoria, todo el trabajo pesado (agrupar, contar, filtrar) se hace en Spark y solo el resultado ya reducido pasa a Pandas para graficar. Nunca convertimos el dataset completo.

---

## Dataset

| Campo | Detalle |
|---|---|
| Nombre | Casos positivos de COVID-19 en Colombia |
| Fuente | Instituto Nacional de Salud (INS) — Datos Abiertos Colombia |
| Tamaño | ~1.4 GB |
| Registros | 6 390 971 filas cargadas / 6 390 957 tras limpieza |
| Archivo | `Casos_positivos_COVID19_Colombia.csv` |

Cada fila es un caso confirmado. Las columnas que usamos son la fecha de diagnóstico, la fecha de notificación, la edad, el sexo, el departamento, el municipio, el tipo de contagio, el estado de gravedad y el desenlace (recuperado o fallecido).

---

## Tecnologías

Apache Spark (PySpark) para cargar y procesar el dataset, Pandas para los resultados ya reducidos, Matplotlib y Seaborn para las gráficas, y Jupyter Notebook como entorno.

---

## Limpieza de datos

Todo el proceso corre en Spark sobre el DataFrame completo. Solo el resultado de cada `groupBy`, `agg` o `limit` se convierte a Pandas con `.toPandas()`.

**Nombres de columnas.** El CSV del portal a veces trae tildes, espacios y mayúsculas distintas según cuándo se descargue. Una función `norm()` les quita las tildes, los pasa a minúscula y reemplaza los espacios por guion bajo. Después un diccionario los lleva a nombres fijos (`fecha_notificacion`, `departamento`, etc.). Sin esto, el notebook se caería con un error de columna si cambia la versión del archivo. También elimina la columna de código del departamento cuando viene junto a la del nombre, para que no quede duplicada.

**Tipos de dato.** La edad llegaba como texto y la pasamos a entero. Las fechas también venían como texto y las convertimos con `to_date`, porque sin eso no podríamos agrupar por mes ni calcular rangos. Spark intenta adivinar los tipos al leer, pero con las fechas en formato colombiano no siempre acierta.

**Texto categórico.** A `sexo`, `estado`, `recuperado`, `tipo_contagio` y `departamento` les aplicamos `trim` y `upper`. Así "Recuperado", "recuperado" y " RECUPERADO " dejan de contarse como tres categorías diferentes.

**Sexo.** Dejamos solo M y F. Cualquier otro valor pasa a nulo.

**Edades imposibles.** Quitamos los registros con edad menor que 0 o mayor que 110. Valores como -1 o 999 son errores de captura, y 0 a 110 cubre cualquier edad real.

**Duplicados.** `dropDuplicates()` sobre todo el DataFrame, porque en las descargas del portal pueden colarse filas repetidas.

**Nulos.** No los imputamos. Columnas como `fecha_muerte` están vacías a propósito en los casos que no fallecieron, así que rellenarlas no tendría sentido, y borrar toda fila con algún nulo nos dejaría casi sin datos. En su lugar, cada gráfica filtra los nulos solo de las columnas que necesita.

El resultado: de 6 390 971 filas quedaron 6 390 957 (se fueron 14 por edades fuera de rango). El DataFrame limpio queda en `df_clean` con `.cache()` para que las consultas siguientes sean rápidas.

---

## Por qué cada gráfica: tipo, datos y color

Esta es la parte que sustenta las decisiones de visualización. Para cada una explicamos tres cosas: por qué ese tipo de gráfica, qué datos la alimentan y por qué ese color.

### 1. Línea — evolución de los casos en el tiempo

Elegimos una línea porque los datos tienen un orden natural: los meses van uno tras otro. La línea conecta esos puntos y deja ver de un vistazo las subidas y bajadas, que aquí son las olas de contagio. Un gráfico de barras no comunicaría igual de bien la idea de continuidad.

Los datos salen de agrupar la fecha de diagnóstico por mes y contar los casos de cada uno. Usamos la fecha de diagnóstico porque marca cuándo se confirmó el caso. Son solo dos series: el mes y el número de casos.

El color es un azul en una sola tonalidad (paleta secuencial Blues). Como hay una sola magnitud que avanza en el tiempo y no hay categorías que separar, un único tono es lo correcto; meter varios colores daría a entender que hay grupos distintos, y no los hay.

### 2. Barras horizontales — departamentos con más casos

Las barras sirven para comparar cantidades entre categorías, que aquí son los departamentos. Las pusimos horizontales para que los nombres largos se lean sin girar la cabeza. Dejamos solo el top 10 porque con más de quince categorías una barra se vuelve ilegible.

Los datos vienen de agrupar por departamento, contar los casos y quedarnos con los diez más altos. Una columna categórica (departamento) y su conteo.

El color es otra vez un azul secuencial, el mismo tono en todas las barras. Todas miden lo mismo (número de casos), así que un solo color mantiene la lectura limpia. Usar un color por barra insinuaría que cada departamento es una categoría sin relación con las demás, cuando en realidad solo estamos ordenando por magnitud.

### 3. Histograma — distribución de la edad

La edad es una variable continua y lo que queremos saber es cómo se reparte: en qué edades se concentran los casos. Para eso el histograma es el gráfico indicado. Agrupamos las edades en rangos de cinco años para que la forma se vea clara sin tanto ruido.

Los datos salen de partir la edad en esos rangos y contar cuántos casos caen en cada uno. Una sola variable numérica. El eje Y arranca en cero, como debe ser en un histograma, para no exagerar las diferencias.

El color es un verde secuencial de un solo tono, por la misma razón que en los casos anteriores: una sola magnitud, sin categorías que distinguir.

### 4. Mapa de calor — edad frente a gravedad

Aquí cruzamos dos variables categóricas, el grupo de edad y el estado de gravedad, y queremos ver qué tan llena está cada combinación. El mapa de calor hace justo eso: cada celda es un cruce y su color indica cuántos casos hay. La matriz queda de cinco filas por cuatro columnas, tamaño de sobra para que el formato tenga sentido.

Los datos vienen de un pivote: las filas son los grupos de edad (0-17, 18-39, 40-59, 60-79, 80+) y las columnas los estados de gravedad, con el conteo en cada celda.

El color es la paleta secuencial viridis, no una divergente. Esto es a propósito: son conteos, todos positivos, sin un punto medio con significado especial. La paleta divergente solo tendría sentido si midiéramos algo con centro en cero, como una correlación, que no es el caso.

### 5. Diagrama de caja — edad según el resultado del caso

Queremos comparar la edad entre dos grupos: quienes se recuperaron y quienes fallecieron. El boxplot es ideal para eso porque muestra la mediana, el rango donde está la mayoría y los valores atípicos de cada grupo, lado a lado.

Para no traer millones de filas a Pandas, filtramos en Spark los casos recuperados y fallecidos, tomamos solo la edad y sacamos una muestra del 5 %. La muestra es suficiente para dibujar la distribución y mantiene el principio de reducir antes de convertir.

El color usa una paleta cualitativa (ColorBrewer Set2) con dos tonos distintos, uno por grupo. Aquí sí corresponden colores diferentes porque recuperado y fallecido son dos categorías sin orden entre sí, y el color ayuda a separarlas de un vistazo.

### Estilo común a las cinco

Todas llevan título descriptivo en negrita de 12 puntos, ejes etiquetados con sus unidades, los bordes superior y derecho ocultos, y un grid horizontal suave en barras e histograma. Ninguna usa los colores por defecto de Matplotlib ni combinaciones de rojo y verde, que dan problemas a quienes tienen daltonismo.

---

## Hallazgos principales

| Visualización | Hallazgo |
|---|---|
| Evolución temporal | Pico en junio de 2021 con 859 087 casos; segunda ola fuerte en enero de 2022 (Ómicron, 752 234) |
| Top departamentos | Bogotá encabeza con 1 888 134 casos (cerca del 30 % del total), seguida de Antioquia (955 268) y Valle (572 721) |
| Distribución de edad | El rango más afectado es 25-29 años (743 670 casos); el tramo de 20 a 44 reúne más del 40 % |
| Edad y gravedad | El estado leve domina en todos los grupos; los fallecidos se concentran en 60-79 y 80+ |
| Edad y desenlace | Mediana de recuperados: 37 años. Mediana de fallecidos: 70 años. Una diferencia de 33 años |

---

## Cómo ejecutar

1. Instala Java 17 o superior y configura `JAVA_HOME`. En Mac, `brew install openjdk@21`; en Windows, descarga Temurin 17 de adoptium.net.
2. Deja el archivo `Casos_positivos_COVID19_Colombia.csv` en esta misma carpeta.
3. Abre `Linares_Carreno_Ariza_HVD_Spark.ipynb` en VS Code o Jupyter.
4. Ejecuta todas las celdas en orden con Run All.
5. La sesión de Spark se cierra sola al final con `spark.stop()`.

Configuración de Spark que usamos:

```python
SparkSession.builder
    .appName('COVID19_Colombia_Spark')
    .config('spark.driver.memory', '4g')
    .config('spark.sql.shuffle.partitions', '64')
```

Si el equipo tiene menos de 8 GB de RAM, baja `spark.driver.memory` a `'2g'` y filtra más antes de cada agregación.

---

## Archivo a entregar

`Linares_Carreno_Ariza_HVD_Spark.ipynb`, con todas las celdas ejecutadas y los resultados a la vista. El dataset no va en el repositorio porque pesa 1.4 GB; se descarga del enlace del INS en Datos Abiertos Colombia.
