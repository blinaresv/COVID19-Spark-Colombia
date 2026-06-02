# Análisis de COVID-19 en Colombia con Apache Spark

**Curso:** Herramientas y visualización de datos  
**Institución:** Fundación Universitaria Los Libertadores  
**Integrantes:**
- Brandon Felipe Linares Viasus
- Adriana Lucía Carreño Medina
- Sariath Eyleen Xiomara Ariza Vargas

---

## Dataset

| Campo | Detalle |
|---|---|
| Nombre | Casos positivos de COVID-19 en Colombia |
| Fuente | Instituto Nacional de Salud (INS) — Datos Abiertos Colombia |
| Tamaño | ~1.4 GB |
| Registros | 6 390 971 filas cargadas / 6 390 957 tras limpieza |
| Archivo | `Casos_positivos_COVID19_Colombia.csv` |

Cada fila representa un caso confirmado e incluye: fecha de diagnóstico, fecha de notificación, edad, sexo, departamento, municipio, tipo de contagio, estado de gravedad y desenlace (recuperado o fallecido).

---

## Tecnologías usadas

- **Apache Spark (PySpark)** — carga y procesamiento distribuido del dataset
- **Pandas** — manejo de los resultados reducidos para graficar
- **Matplotlib / Seaborn** — visualizaciones
- **Jupyter Notebook** — entorno de desarrollo y presentación

---

## Proceso de limpieza de datos

Todas las transformaciones se hicieron en Spark sobre el DataFrame completo. Solo el resultado ya reducido (resultado de `.groupBy`, `.agg`, `.limit`, etc.) se pasó a Pandas con `.toPandas()`.

### 1. Normalización de nombres de columnas

El CSV del portal de Datos Abiertos puede traer encabezados con tildes, espacios y mayúsculas inconsistentes dependiendo de cuándo se descargó. Se aplicó la función `norm()` que:

- Elimina tildes y caracteres especiales (normalización Unicode NFKD)
- Convierte todo a minúsculas
- Reemplaza espacios y caracteres no alfanuméricos por guión bajo `_`

Luego se aplicó un diccionario de equivalencias para asignar nombres canónicos fijos (`fecha_notificacion`, `departamento`, `municipio`, `tipo_contagio`, etc.), garantizando que el notebook funcione sin importar la versión del CSV descargado.

**Por qué:** sin esto el notebook fallaría con `AnalysisException` si los encabezados cambian entre versiones del archivo.

---

### 2. Conversión de tipos

| Columna | Tipo original | Tipo final | Motivo |
|---|---|---|---|
| `edad` | String | Integer | Permitir filtros numéricos y rangos |
| `fecha_notificacion` | String | Date | Agrupar por mes/año en la serie temporal |
| `fecha_diagnostico` | String | Date | Ídem, columna preferida para la serie |
| `fecha_muerte` | String | Date | Consistencia, aunque tiene muchos nulos |

**Por qué:** Spark infiere tipos al leer el CSV pero no siempre acierta con las fechas en formato colombiano. `to_date` convierte con el formato correcto.

---

### 3. Estandarización de texto en columnas categóricas

A las columnas `sexo`, `estado`, `recuperado`, `tipo_contagio` y `departamento` se les aplicó:
- `F.trim()` — elimina espacios al inicio y al final
- `F.upper()` — convierte todo a mayúsculas

**Por qué:** un mismo valor como `"Recuperado"`, `"recuperado"` y `" RECUPERADO "` se tratarían como tres categorías distintas al agrupar.

---

### 4. Unificación de valores de sexo

La columna `sexo` solo admite dos valores válidos: `M` (masculino) y `F` (femenino). Cualquier otro valor (por ejemplo `"NO REPORTADO"`, `"INDETERMINADO"` o nulo) se reemplazó por `null`.

**Por qué:** evitar que categorías residuales contaminen análisis de género si se añade en el futuro.

---

### 5. Filtrado de edades fuera de rango

Se eliminaron todos los registros con `edad < 0` o `edad > 110`.

**Por qué:** valores como `-1`, `999` o `9999` son errores de captura evidentes. El rango 0–110 cubre cualquier edad biológicamente posible.

---

### 6. Eliminación de duplicados

Se llamó a `dropDuplicates()` sobre el DataFrame completo para eliminar filas idénticas.

**Por qué:** en datasets descargados del portal público pueden aparecer registros repetidos por carga incremental o errores del sistema fuente.

---

### 7. Tratamiento de nulos — decisión explícita de NO imputar

Las columnas `fecha_muerte` y las relacionadas con datos de viaje tienen una alta proporción de nulos **por diseño**: `fecha_muerte` solo aplica a casos fallecidos y los datos de viaje solo a casos importados. Se decidió **no imputar** estos nulos.

En cada visualización se filtran los nulos únicamente de las columnas que esa gráfica necesita, con `filter(col(...).isNotNull())`.

**Por qué:** imputar una fecha de muerte para un caso recuperado no tiene sentido. Eliminar todas las filas con algún nulo habría descartado la gran mayoría del dataset.

---

### Resultado de la limpieza

```
Registros cargados:              6 390 971
Registros después de limpieza:   6 390 957  (14 eliminados por edades fuera de rango)
```

El DataFrame limpio se almacena en `df_clean` y se guarda en caché con `.cache()` para acelerar las consultas sucesivas.

---

## Visualizaciones

| # | Tipo | Variable analizada | Pregunta que responde |
|---|---|---|---|
| 1 | Líneas | Casos por mes | ¿Cómo evolucionaron los contagios en el tiempo? |
| 2 | Barras horizontales | Top 10 departamentos | ¿Qué departamentos concentraron más casos? |
| 3 | Histograma | Distribución de edad | ¿Qué edades fueron más afectadas? |
| 4 | Mapa de calor | Grupo de edad × estado de gravedad | ¿Cómo se relacionan la edad y la gravedad? |
| 5 | Boxplot | Edad por desenlace (recuperado vs. fallecido) | ¿Difiere la edad según el desenlace? |

### Criterios de estilo aplicados a todas las gráficas

- Título descriptivo en **negrita 12pt**
- Ejes etiquetados con unidades
- Sin bordes superior ni derecho (`spines` ocultos)
- Eje Y desde cero
- Paleta **secuencial** (Blues, Greens, Viridis) para conteos y series ordenadas
- Paleta **cualitativa** (ColorBrewer Set2) para el boxplot con dos categorías
- Sin colores por defecto de Matplotlib

---

## Hallazgos principales

| Visualización | Hallazgo clave |
|---|---|
| Evolución temporal | Pico en **junio 2021** con 859 087 casos; segunda ola en **enero 2022** (Ómicron, 752 234) |
| Top departamentos | **Bogotá** lidera con 1 888 134 casos (~30 % del total); le siguen Antioquia (955 268) y Valle (572 721) |
| Distribución de edad | Rango más afectado: **25–29 años** (743 670 casos); el tramo 20–44 años concentra más del 40 % |
| Edad vs. gravedad | Estado Leve domina en todos los grupos; Fallecido se concentra en **60–79** y **80+** |
| Desenlace por edad | Mediana recuperados: **37 años** · Mediana fallecidos: **70 años** · Brecha: **33 años** |

---

## Cómo ejecutar

1. Asegúrate de tener Java 17 o superior instalado y la variable `JAVA_HOME` configurada.
   - Mac: `brew install openjdk@21` (versión usada: OpenJDK 21)
   - Windows: descargar Temurin 17 de adoptium.net
2. Coloca el archivo `Casos_positivos_COVID19_Colombia.csv` en esta misma carpeta.
3. Abre `Linares_Carreno_Ariza_HVD_Spark.ipynb` en VS Code o Jupyter.
4. Ejecuta todas las celdas en orden (**Run All**).
5. La sesión de Spark se cierra automáticamente al final con `spark.stop()`.

### Configuración de Spark utilizada

```python
SparkSession.builder
    .appName('COVID19_Colombia_Spark')
    .config('spark.driver.memory', '4g')
    .config('spark.sql.shuffle.partitions', '64')
```

Si el equipo tiene menos de 8 GB de RAM disponible, reducir `spark.driver.memory` a `'2g'` y aumentar los filtros previos a las agregaciones.

---

## Archivo a entregar

`Linares_Carreno_Ariza_HVD_Spark.ipynb` — con todas las celdas ejecutadas y los resultados visibles.
