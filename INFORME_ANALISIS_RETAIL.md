# Informe de Análisis de Datos - Sistema Retail

---

## Resumen

### Datos Procesados
- **Total de Transacciones:** 1,108,987
- **Periodo:** 6 meses (Enero - Junio 2013)
- **Tiendas Analizadas:** 4 (ID: 102, 103, 107, 110)
- **Clientes Únicos:** 131,186
- **Productos Únicos:** 449
- **Total Productos Vendidos:** 10,591,793 unidades

### Hallazgos Clave
- **Alta calidad de datos:** 0% valores nulos
- **Crecimiento continuo:** Incremento del 4.5% en transacciones de enero a junio
- **26.31% clientes nuevos:** Realizaron solo 1 compra en el periodo
- **Tienda 103 líder:** 36.71% del total de transacciones
- **Promedio compra:** 9.55 productos por transacción

---

## Análisis de Productos y Categorías

### Catálogo de Productos

#### Estructura General
- **Categorías Totales:** 50
- **Productos en Catálogo:** 112,010 registros
- **Productos Únicos:** 69,891
- **Productos Duplicados:** 42,119 (37.6%)

#### Distribución por Categorías

##### Top 10 Categorías con Más Productos

| Posición | Categoría | Código | Productos | Porcentaje |
|----------|-----------|--------|-----------|------------|
| 1 | CONCESIONARIOS | 2 | 23,258 | 20.76% |
| 2 | PASABOCAS | 9 | 5,279 | 4.71% |
| 3 | CUIDADO DE LA COCINA | 11 | 4,869 | 4.35% |
| 4 | GALLETAS | 13 | 4,541 | 4.05% |
| 5 | QUESO | 8 | 4,260 | 3.80% |
| 6 | ENLATADOS | 14 | 3,784 | 3.38% |
| 7 | MASCOTAS | 40 | 3,693 | 3.30% |
| 8 | DESODORANTES | 48 | 3,434 | 3.07% |
| 9 | SALSAS | 22 | 3,422 | 3.06% |
| 10 | PANES-TOSTADAS | 5 | 3,322 | 2.97% |

##### Categorías con Menos Productos

| Categoría | Código | Productos | Porcentaje |
|-----------|--------|-----------|------------|
| BANANO EXPORTACION | 46 | 3 | 0.00% |
| MANGO TOMY | 50 | 12 | 0.01% |
| PAPAYA COMUN | 45 | 21 | 0.02% |
| LULO NACIONAL | 41 | 29 | 0.03% |
| LEGUMBRES VERDES | 31 | 97 | 0.09% |

### Relaciones Producto-Categoría

#### Productos en Múltiples Categorías
- **Total:** 23,646 productos (33.83% del catálogo)
- **Promedio categorías por producto:** 1.34
- **Promedio productos por categoría:** 1,870.74

> **Insight:** Un tercio de los productos están clasificados en múltiples categorías

---

## Análisis de Transacciones

### Estadísticas de Compra

#### Productos por Transacción

| Métrica | Valor |
|---------|-------|
| **Media** | 9.55 productos |
| **Mediana** | 6 productos |
| **Moda** | 1 producto |
| **Desviación Estándar** | 10.0 |
| **Mínimo** | 1 producto |
| **Máximo** | 128 productos |

#### Distribución por Cuartiles

| Percentil | Productos |
|-----------|-----------|
| P25 | 3 productos |
| P50 (Mediana) | 6 productos |
| P75 | 12 productos |
| **IQR** | 9 productos |


## Patrones de Comportamiento de Clientes

### Segmentación por Frecuencia de Compra

#### Top 20 Frecuencias de Compra

| Compras | Clientes | Porcentaje | Segmento |
|---------|----------|------------|----------|
| 1 | 34,513 | 26.31% | 🆕 **Nuevos/Ocasionales** |
| 2 | 16,182 | 12.34% | 🔵 **Esporádicos** |
| 3 | 10,613 | 8.09% | 🔵 **Esporádicos** |
| 4-6 | 20,444 | 15.58% | 🟢 **Regulares** |
| 7-10 | 15,512 | 11.82% | 🟡 **Frecuentes** |
| 11-20 | 18,972 | 14.46% | 🟠 **Muy Frecuentes** |
| >20 | 14,950 | 11.40% | 🔴 **Leales/VIP** |

### Análisis por Segmento

#### Clientes Nuevos/Ocasionales (26.31%)
- **Características:** Solo 1 compra en 6 meses
- **Oportunidad:** Programas de fidelización y remarketing
- **Acción recomendada:** Campañas de reactivación

#### Clientes Esporádicos (20.43%)
- **Características:** 2-3 compras en 6 meses
- **Promedio:** 0.5 compras/mes
- **Acción recomendada:** Incentivos para aumentar frecuencia

#### Clientes Regulares (15.58%)
- **Características:** 4-6 compras en 6 meses
- **Promedio:** ~1 compra/mes
- **Acción recomendada:** Mantener engagement

#### Clientes Frecuentes (11.82%)
- **Características:** 7-10 compras en 6 meses
- **Promedio:** 1.5 compras/mes
- **Acción recomendada:** Programas de recompensas

#### Clientes Leales/VIP (25.86%)
- **Características:** >10 compras en 6 meses
- **Promedio:** >2 compras/mes
- **Valor:** Alto lifetime value
- **Acción recomendada:** Tratamiento VIP y beneficios exclusivos

### Cliente Más Frecuente
- **ID:** 336296
- **Moda:** Cliente con mayor número de transacciones
- **Categoría:** Cliente VIP/Mayorista

---

## Análisis Temporal

### Distribución Mensual

| Mes | Transacciones | Porcentaje | Tendencia |
|-----|---------------|------------|-----------|
| Enero 2013 | 184,916 | 16.67% | ⚪ Base |
| Febrero 2013 | 170,771 | 15.40% | 🔴 -7.6% |
| Marzo 2013 | 189,313 | 17.07% | 🟢 +10.9% |
| Abril 2013 | 182,632 | 16.47% | 🟡 -3.5% |
| Mayo 2013 | 188,199 | 16.97% | 🟢 +3.0% |
| Junio 2013 | 193,156 | 17.42% | 🟢 +2.6% |

#### 📈 Insights Mensuales
- **Febrero:** Mes más bajo (menor cantidad de días)
- **Marzo:** Pico de ventas (+17.07%)
- **Junio:** Cierre fuerte del semestre
- **Tendencia general:** Crecimiento del 4.5% (enero vs junio)

### Distribución Semanal

| Día | Transacciones | Porcentaje | Ranking |
|-----|---------------|------------|---------|
| Domingo | 191,406 | 17.26% | 🥇 1° |
| Sábado | 189,015 | 17.04% | 🥈 2° |
| Jueves | 158,766 | 14.32% | 🥉 3° |
| Martes | 150,739 | 13.59% | 4° |
| Lunes | 142,445 | 12.84% | 5° |
| Viernes | 139,371 | 12.57% | 6° |
| Miércoles | 137,245 | 12.38% | 7° |

####  Insights Semanales
- **Fin de semana dominante:** 34.30% de ventas (sábado + domingo)
- **Días fuertes:** Jueves es el mejor día entre semana
- **Días débiles:** Miércoles y viernes tienen menos tráfico
- **Recomendación:** Promociones mid-week para equilibrar

---

##  Análisis por Tienda

### Comparativa de Desempeño

| Tienda | Transacciones | Porcentaje | Posición | Categoría |
|--------|---------------|------------|----------|-----------|
| **103** | 407,130 | 36.71% | 🥇 | Líder |
| **102** | 314,286 | 28.34% | 🥈 | Alto desempeño |
| **107** | 254,633 | 22.96% | 🥉 | Medio-alto |
| **110** | 132,938 | 11.99% | 4° | Oportunidad |

### Análisis Individual

#### 🥇 Tienda 103 - Líder del Mercado
- **Transacciones:** 407,130
- **Participación:** 36.71%
- **Status:** Tienda estrella
- **Características:**
  - Volumen 30% superior a la segunda tienda
  - Probablemente ubicación estratégica
  - Mayor tráfico de clientes

#### 🥈 Tienda 102 - Alto Desempeño
- **Transacciones:** 314,286
- **Participación:** 28.34%
- **Status:** Tienda consolidada
- **Características:**
  - Segundo mayor volumen
  - Desempeño consistente

#### 🥉 Tienda 107 - Medio-Alto
- **Transacciones:** 254,633
- **Participación:** 22.96%
- **Status:** Tienda en desarrollo
- **Oportunidad:** Potencial de crecimiento del 23% para alcanzar a 102

#### Tienda 110 - Oportunidad de Mejora
- **Transacciones:** 132,938
- **Participación:** 11.99%
- **Status:** Requiere atención
- **Gap:** 67% menos transacciones que la líder
- **Recomendaciones:**
  - Análisis de ubicación y competencia
  - Revisar estrategias de marketing local
  - Evaluar mix de productos
  - Capacitación de personal

---

## Productos Más Vendidos

### Top 30 Productos Globales

| Rank | Product ID | Ventas | % Total | Acumulado |
|------|------------|--------|---------|-----------|
| 1 | 5 | 300,526 | 2.84% | 2.84% |
| 2 | 10 | 290,313 | 2.74% | 5.58% |
| 3 | 3 | 269,855 | 2.55% | 8.13% |
| 4 | 4 | 260,418 | 2.46% | 10.59% |
| 5 | 6 | 254,644 | 2.40% | 12.99% |
| 6 | 8 | 253,899 | 2.40% | 15.39% |
| 7 | 7 | 225,877 | 2.13% | 17.52% |
| 8 | 16 | 224,159 | 2.12% | 19.64% |
| 9 | 11 | 221,968 | 2.10% | 21.74% |
| 10 | 9 | 212,480 | 2.01% | 23.75% |
| 11 | 12 | 210,081 | 1.98% | 25.73% |
| 12 | 21 | 202,214 | 1.91% | 27.64% |
| 13 | 13 | 185,167 | 1.75% | 29.39% |
| 14 | 19 | 183,467 | 1.73% | 31.12% |
| 15 | 14 | 179,634 | 1.70% | 32.82% |
| 16 | 15 | 173,154 | 1.63% | 34.45% |
| 17 | 17 | 166,405 | 1.57% | 36.02% |
| 18 | 18 | 166,233 | 1.57% | 37.59% |
| 19 | 26 | 151,794 | 1.43% | 39.02% |
| 20 | 23 | 145,956 | 1.38% | 40.40% |
| 21 | 20 | 141,429 | 1.34% | 41.74% |
| 22 | 22 | 136,176 | 1.29% | 43.03% |
| 23 | 30 | 133,878 | 1.26% | 44.29% |
| 24 | 32 | 130,965 | 1.24% | 45.53% |
| 25 | 28 | 126,722 | 1.20% | 46.73% |
| 26 | 24 | 122,922 | 1.16% | 47.89% |
| 27 | 25 | 121,363 | 1.15% | 49.04% |
| 28 | 27 | 113,850 | 1.07% | 50.11% |
| 29 | 29 | 113,668 | 1.07% | 51.18% |
| 30 | 2 | 113,246 | 1.07% | 52.25% |

### Análisis de Concentración

- **Top 5 productos:** 12.99% de las ventas
- **Top 10 productos:** 23.75% de las ventas
- **Top 30 productos:** 52.25% de las ventas

> El 30% de los productos más vendidos (30 de 449) representan más de la mitad de todas las ventas. Esto sugiere una alta concentración en productos estrella.

---

## ✅ Calidad de Datos


#### Métricas de Calidad

| Métrica | Valor |
|---------|-------|
| **Valores Nulos** | 0 |
| **Completitud** | 100% |
| **Duplicados Exactos** | 1 (0.0001%) |
| **Duplicados Cliente-Fecha** | 3,687 (0.33%) |
| **Consistencia Temporal** | 100% |
| **Integridad Referencial** | 100% |