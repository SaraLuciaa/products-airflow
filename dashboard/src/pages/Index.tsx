import { MetricCard } from "@/components/MetricCard";
import { TopProductsGrid } from "@/components/TopProductsGrid";
import { ForecastChart } from "@/components/ForecastChart";
import { CohortHeatmap } from "@/components/CohortHeatmap";
import { WeeklyDistributionChart } from "@/components/WeeklyDistributionChart";
import { CategoryDistributionChart } from "@/components/CategoryDistributionChart";
import { ProductQualityMetrics } from "@/components/ProductQualityMetrics";
import { TopCustomersGrid } from "@/components/TopCustomersGrid";
import { MonthlyDistributionChart } from "@/components/MonthlyDistributionChart";
import { StoreDistributionChart } from "@/components/StoreDistributionChart";
import { TopCategoriesChart } from "@/components/TopCategoriesChart";
import { CustomerSegmentationChart } from "@/components/CustomerSegmentationChart";
import { DailyTimeSeriesChart } from "@/components/DailyTimeSeriesChart";
import { DistributionBoxplot } from "@/components/DistributionBoxplot";
import { CorrelationHeatmap } from "@/components/CorrelationHeatmap";
import { BusinessRecommendations } from "@/components/BusinessRecommendations";
import { ShoppingCart, Users, Package, Store, TrendingUp, Clock } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
// Importar directamente los JSONs generados por los DAGs
import edaData from "@/data/transacciones_eda.json";
import modelosAvanzadosData from "@/data/transacciones_modelos_avanzados.json";
import tiendaRevisionData from "@/data/tienda_revision_inicial.json";
import tiendaStatsData from "@/data/tienda_estadisticas_product_category.json";

const Index = () => {
  // Usar datos directamente de transacciones_eda.json (generado por DAG)
  const edaStats = edaData.estadisticas;
  const edaRevision = edaData.revision_inicial;
  
  // Estadísticas numéricas y categóricas
  const stats = edaStats;
  const topProducts = stats?.estadisticas_categoricas?.top_10_productos_mas_vendidos?.top_10 || [];
  const topCustomers = stats?.estadisticas_categoricas?.top_10_clientes_mas_compras?.top_10 || [];
  const topCategories = stats?.top_categorias_por_ventas?.top || [];
  const totalUnidades = stats?.top_categorias_por_ventas?.total_unidades;
  
  // Productos únicos y total vendidos
  const productosUnicos = stats?.estadisticas_categoricas?.top_10_productos_mas_vendidos?.productos_unicos;
  const totalProductosVendidos = stats?.estadisticas_categoricas?.top_10_productos_mas_vendidos?.total_productos_vendidos;
  
  // Datos de productos
  const categoryDistribution = tiendaStatsData?.categorias?.distribucion_categorias || [];
  
  // Datos de modelos avanzados (generados por DAG)
  const segmentation = modelosAvanzadosData?.segmentacion_clientes;
  const cohortesRetention = modelosAvanzadosData?.cohortes_retencion?.retention || [];
  const forecastMensual = modelosAvanzadosData?.forecast_mensual;

  return (
    <div className="min-h-screen bg-background">
      {/* Header with Colombian flag accent */}
      <header className="border-b border-border bg-card shadow-sm">
        <div className="container mx-auto px-4 py-6">
          <div className="flex items-center gap-3">
            <div className="w-2 h-16 rounded-full gradient-flag"></div>
            <div>
              <h1 className="text-4xl font-bold bg-gradient-to-r from-primary via-accent to-secondary bg-clip-text text-transparent">
                Dashboard Supermercado
              </h1>
              <p className="text-muted-foreground mt-1">
                Análisis de datos • Enero - Junio 2013
              </p>
            </div>
          </div>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8 space-y-8">
        {/* Key Metrics */}
        <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard
            title="Total Transacciones"
            value={edaRevision?.estructura?.num_transacciones?.toLocaleString() || '0'}
            subtitle="Durante el período"
            icon={ShoppingCart}
            variant="primary"
          />
          <MetricCard
            title="Clientes Únicos"
            value={stats?.estadisticas_categoricas?.customer_id?.clientes_unicos?.toLocaleString() || '0'}
            subtitle="Base de clientes"
            icon={Users}
            variant="accent"
          />
          <MetricCard
            title="Productos Únicos"
            value={productosUnicos || '0'}
            subtitle="En catálogo"
            icon={Package}
            variant="secondary"
          />
          <MetricCard
            title="Tiendas Activas"
            value={edaRevision?.tiendas_unicas || '0'}
            subtitle="En operación"
            icon={Store}
            variant="default"
          />
        </section>

        {/* Products per Transaction Stats */}
        <section className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <MetricCard
            title="Promedio Productos/Transacción"
            value={stats?.estadisticas_numericas?.num_productos_por_transaccion?.media || '0'}
            subtitle={`Mediana: ${stats?.estadisticas_numericas?.num_productos_por_transaccion?.mediana || '0'}`}
            icon={TrendingUp}
          />
          <MetricCard
            title="Máximo en una Compra"
            value={stats?.estadisticas_numericas?.num_productos_por_transaccion?.maximo || '0'}
            subtitle={`Mínimo: ${stats?.estadisticas_numericas?.num_productos_por_transaccion?.minimo || '0'}`}
            icon={Package}
          />
          <MetricCard
            title="Total Productos Vendidos"
            value={totalProductosVendidos?.toLocaleString() || '0'}
            subtitle="Unidades totales"
            icon={ShoppingCart}
          />
          <MetricCard
            title="Tiempo Entre Compras"
            value={`${stats?.estadisticas_categoricas?.tiempo_entre_compras?.promedio_global_dias?.toFixed(1) || 'N/A'}`}
            subtitle="Promedio en días"
            icon={Clock}
            variant="accent"
          />
        </section>

        {/* Top Products */}
        <TopProductsGrid products={topProducts} />

        {/* Top Customers */}
        {topCustomers.length > 0 && (
          <TopCustomersGrid customers={topCustomers} />
        )}

        {/* Daily Time Series */}
        {stats?.estadisticas_categoricas?.distribucion_temporal_diaria && (
          <DailyTimeSeriesChart data={stats.estadisticas_categoricas.distribucion_temporal_diaria} />
        )}

        {/* Weekly Distribution */}
        {stats?.estadisticas_categoricas?.distribucion_dia_semana && (
          <WeeklyDistributionChart data={stats.estadisticas_categoricas.distribucion_dia_semana} />
        )}

        {/* Monthly Distribution */}
        {stats?.estadisticas_categoricas?.distribucion_temporal_mensual && (
          <MonthlyDistributionChart data={stats.estadisticas_categoricas.distribucion_temporal_mensual} />
        )}

        {/* Distribution Boxplot */}
        <DistributionBoxplot 
          customers={topCustomers}
          categories={topCategories}
        />

        {/* Store Distribution */}
        {stats?.estadisticas_categoricas?.distribucion_por_tienda && (
          <StoreDistributionChart data={stats.estadisticas_categoricas.distribucion_por_tienda} />
        )}

        {/* Forecast Chart */}
        {forecastMensual?.global && (
          <ForecastChart data={forecastMensual.global} title="Pronóstico Global de Ventas" />
        )}

        {/* Store Forecasts */}
        {forecastMensual?.tiendas && forecastMensual.tiendas.length > 0 && (
          <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <ForecastChart data={forecastMensual.tiendas} title="Pronóstico Tienda 102" storeId={102} />
            <ForecastChart data={forecastMensual.tiendas} title="Pronóstico Tienda 103" storeId={103} />
          </section>
        )}

        {/* Cohort Analysis */}
        {cohortesRetention.length > 0 && (
          <CohortHeatmap data={cohortesRetention} />
        )}

        {/* Customer Segmentation */}
        {segmentation && (
          <CustomerSegmentationChart 
            data={segmentation.clusters_resumen} 
            totalClientes={segmentation.num_clientes}
          />
        )}

        {/* Correlation Heatmap */}
        {segmentation?.clusters_resumen && segmentation.clusters_resumen.length > 0 && (
          <CorrelationHeatmap 
            clusters={segmentation.clusters_resumen}
            featureNames={segmentation.feature_names?.map((name: string) => {
              const names: Record<string, string> = {
                'frecuencia': 'Frecuencia',
                'volumen_total': 'Volumen Total',
                'diversidad_productos': 'Diversidad Productos',
                'diversidad_categorias': 'Diversidad Categorías'
              };
              return names[name] || name;
            }) || ['Frecuencia', 'Volumen Total', 'Diversidad Productos', 'Diversidad Categorías']}
          />
        )}

        {/* Business Recommendations */}
        {segmentation?.clusters_resumen && segmentation.clusters_resumen.length > 0 && (
          <BusinessRecommendations clusters={segmentation.clusters_resumen} />
        )}

        {/* Top Categories by Sales */}
        {topCategories.length > 0 && (
          <TopCategoriesChart 
            data={topCategories} 
            totalUnidades={totalUnidades}
          />
        )}

        {/* Product Quality Metrics */}
        <ProductQualityMetrics data={tiendaRevisionData} />

        {/* Category Distribution */}
        {categoryDistribution.length > 0 && (
          <CategoryDistributionChart data={categoryDistribution} />
        )}

        {/* Footer */}
        <footer className="text-center text-sm text-muted-foreground py-8 border-t border-border mt-8">
          <p>Dashboard de Análisis • Datos período 2013 • Sistema de Gestión Comercial</p>
        </footer>
      </main>
    </div>
  );
};

export default Index;
